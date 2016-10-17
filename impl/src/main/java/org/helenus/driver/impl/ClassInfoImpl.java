/*
 * Copyright (C) 2015-2016 The Helenus Driver Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.helenus.driver.impl;

import java.lang.annotation.Annotation;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.commons.lang3.tuple.Pair;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.UDTValue;

import org.helenus.commons.lang3.reflect.ReflectionUtils;
import org.helenus.driver.ColumnPersistenceException;
import org.helenus.driver.ExcludedKeyspaceKeyException;
import org.helenus.driver.ObjectConversionException;
import org.helenus.driver.ObjectNotFoundException;
import org.helenus.driver.StatementManager;
import org.helenus.driver.info.ClassInfo;
import org.helenus.driver.info.FieldInfo;
import org.helenus.driver.info.TableInfo;
import org.helenus.driver.persistence.CQLDataType;
import org.helenus.driver.persistence.Column;
import org.helenus.driver.persistence.Entity;
import org.helenus.driver.persistence.InitialObjects;
import org.helenus.driver.persistence.Keyspace;
import org.helenus.driver.persistence.KeyspaceKey;
import org.helenus.driver.persistence.Table;
import org.helenus.driver.persistence.UDTEntity;
import org.helenus.driver.persistence.UDTRootEntity;

/**
 * The <code>ClassInfo</code> class provides information about a particular
 * POJO class.
 *
 * @copyright 2015-2016 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 19, 2015 - paouelle, vasu - Creation
 *
 * @param <T> The type of POJO represented by this class
 *
 * @since 1.0
 */
public class ClassInfoImpl<T> implements ClassInfo<T> {
  /**
   * The <code>Context</code> class provides a specific context for the POJO
   * as referenced while building a statement.
   *
   * @copyright 2015-2016 The Helenus Driver Project Authors
   *
   * @author The Helenus Driver Project Authors
   * @version 1 - Jan 19, 2015 - paouelle, vasu - Creation
   *
   * @since 1.0
   */
  public class Context implements StatementManager.Context<T> {
    /**
     * Holds the registered keyspace keys.
     *
     * @author paouelle
     */
    protected final Map<String, Object> keyspaceKeys = new LinkedHashMap<>(8);

    /**
     * Instantiates a new <code>Context</code> object.
     *
     * @author paouelle
     */
    Context() {}

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.StatementManager.Context#getObjectClass()
     */
    @Override
    public Class<T> getObjectClass() {
      return ClassInfoImpl.this.getObjectClass();
    }

    /**
     * Gets the class info for the POJO.
     *
     * @author paouelle
     *
     * @return the non-<code>null</code> class info for the POJO
     */
    public ClassInfoImpl<T> getClassInfo() {
      return ClassInfoImpl.this;
    }

    /**
     * Gets the keyspace name associated with this context.
     *
     * @author paouelle
     *
     * @return the non-<code>null</code> keyspace name associated with this
     *         context
     * @throws ObjectNotFoundException if unable to compute the keyspace name
     *         based on provided keyspace keys
     * @throws ExcludedKeyspaceKeyException if the context defines a keyspace key value
     *         which is marked as excluded for a given keyspace key
     */
    @SuppressWarnings("synthetic-access")
    public String getKeyspace() {
      final String[] types = keyspace.keys();
      String name = keyspace.name();

      if (!ArrayUtils.isEmpty(types)) {
        final String[] svalues = new String[types.length];

        // let's make sure we can resolve all keyspace keys
        for (int i = 0; i < types.length; i++) {
          final FieldInfoImpl<T> finfo = (FieldInfoImpl<T>)getKeyspaceKeyByType(types[i]);
          final KeyspaceKey skey = finfo.getKeyspaceKey();
          final String key = skey.name();
          final Object value = keyspaceKeys.get(key);

          if (value == null) {
            throw new ObjectNotFoundException(
              getObjectClass(), "missing keyspace key '" + key + "'"
            );
          }
          if (ArrayUtils.contains(skey.exclude(), value)) {
            throw new ExcludedKeyspaceKeyException(
              "excluded keyspace key '"
              + key
              + "' value '"
              + value
              + "' for object class: "
              + clazz.getName()
            );
          }
          // use the natural toString() to convert the value into a string
          svalues[i] = String.valueOf(value);
        }
        final String svalue = StringUtils.join(svalues, '_');

        if (name.isEmpty()) {
          name = svalue;
        } else {
          name += '_' + svalue;
        }
      }
      if (name.isEmpty()) {
        throw new ObjectNotFoundException(
          getObjectClass(), "invalid empty keyspace name"
        );
      }
      // replaces all non-alphanumeric and non underscores with underscores
      // to comply with Cassandra
      return name.replaceAll("[^a-zA-Z0-9_]", "_").toLowerCase();
    }

    /**
     * Adds a keyspace key to the context for use in keyspace name creation.
     *
     * @author paouelle
     *
     * @param  name the keyspace key name
     * @param  value the keyspace key value
     * @throws NullPointerException if <code>name</code> or <code>value</code>
     *         is <code>null</code>
     * @throws IllegalArgumentException if the POJO doesn't require the specified
     *         keyspace key or if the value doesn't match the POJO's definition for the
     *         specified keyspace key
     * @throws ExcludedKeyspaceKeyException if the specified keyspace key value is
     *         marked as excluded the specified keyspace key
     */
    public void addKeyspaceKey(String name, Object value) {
      validateKeyspaceKey(name, value);
      keyspaceKeys.put(name, value);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.StatementManager.Context#getObject(com.datastax.driver.core.Row)
     */
    @Override
    public T getObject(Row row) {
      return ClassInfoImpl.this.getObject(row, keyspaceKeys);
    }

    /**
     * Converts the specified UDT value into a POJO object defined by this
     * class information.
     *
     * @author paouelle
     *
     * @param  uval the UDT value to convert into a POJO
     * @return the POJO object corresponding to the given UDT value
     * @throws ObjectConversionException if unable to convert to a POJO
     */
    public T getObject(UDTValue uval) {
      return ClassInfoImpl.this.getObject(uval);
    }

    /**
     * Gets the initial objects to insert in a newly created table for this
     * entity based on this context.
     *
     * @author paouelle
     *
     * @return a non-<code>null</code> collection of the initial objects to insert
     *         in the table
     */
    @SuppressWarnings("synthetic-access")
    public Collection<T> getInitialObjects() {
      if (getKeyspaceKeys().isEmpty()) {
        return ClassInfoImpl.this.getInitialObjects(null);
      }
      final Map<String, String> svalues = new LinkedHashMap<>(getKeyspaceKeys().size());

      for (final Map.Entry<String, FieldInfoImpl<T>> e: getKeyspaceKeyTypes().entrySet()) {
        final String type = e.getKey();
        final FieldInfoImpl<T> finfo = e.getValue();
        final String name = finfo.getKeyspaceKeyName();
        final Object value = keyspaceKeys.get(name);

        org.apache.commons.lang3.Validate.isTrue(
          value != null,
          "missing keyspace key '%s'",
          name
        );
        // use the natural toString() to convert the value into a string
        svalues.put(type, String.valueOf(value));
      }
      return ClassInfoImpl.this.getInitialObjects(svalues);
    }
  }

  /**
   * The <code>POJOContext</code> class provides a specific context for the POJO
   * as referenced while building an insert or update statement.
   *
   * @copyright 2015-2015 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 19, 2015 - paouelle - Creation
   *
   * @since 1.0
   */
  public class POJOContext extends Context {
    /**
     * Holds the POJO object
     *
     * @author paouelle
     */
    protected final T object;

    /**
     * Instantiates a new <code>POJOContext</code> object.
     *
     * @author paouelle
     *
     * @param  object the POJO object
     * @throws NullPointerException if <code>object</code> is <code>null</code>
     * @throws IllegalArgumentException if <code>object</code> is not of the
     *         appropriate class
     */
    @SuppressWarnings("synthetic-access")
    public POJOContext(T object) {
      org.apache.commons.lang3.Validate.notNull(object, "invalid null object");
      org.apache.commons.lang3.Validate.isTrue(
        clazz.isInstance(object),
        "invalid POJO class '%s'; expecting '%s'",
        object.getClass().getName(), clazz.getName()
      );
      this.object = object;
      populateKeyspaceKeys(ClassInfoImpl.this.keyspaceKeysByName);
    }

    /**
     * Populates the keyspace keys defined in the given map from the POJO.
     *
     * @author paouelle
     *
     * @param fields the map of keyspace key fields from the POJO where to extract
     *        the keyspace key values
     */
    protected void populateKeyspaceKeys(Map<String, FieldInfoImpl<T>> fields) {
      keyspaceKeys.clear();
      for (final Map.Entry<String, FieldInfoImpl<T>> e: fields.entrySet()) {
        final String name = e.getKey();
        final FieldInfoImpl<T> field = e.getValue();
        final Object val = field.getValue(object);

        keyspaceKeys.put(name, val);
      }
    }

    /**
     * Gets the POJO object associated with this context.
     *
     * @author paouelle
     *
     * @return the POJO object associated with this context
     */
    public T getObject() {
      return object;
    }

    /**
     * Retrieves all columns and their values from the POJO and the specified
     * table.
     *
     * @author paouelle
     *
     * @param  tname the name of the table from which to retrieve columns
     * @return a non-<code>null</code> map of all column/value pairs for the POJO
     * @throws IllegalArgumentException if a mandatory column is missing from the POJO
     * @throws ColumnPersistenceException if unable to persist a column's value
     */
    public Map<String, Pair<Object, CQLDataType>> getColumnValues(String tname) {
      final TableInfoImpl<T> table = (TableInfoImpl<T>)getTable(tname);

      if (table == null) { // table not defined so nothing to return
        return Collections.emptyMap();
      }
      return table.getColumnValues(object);
    }

    /**
     * Retrieves all partition key columns and their values from the POJO and the
     * specified table.
     *
     * @author paouelle
     *
     * @param  tname the name of the table from which to retrieve columns
     * @return a non-<code>null</code> map of all partition key column/value pairs
     *         for the POJO
     * @throws IllegalArgumentException if a column is missing from the POJO
     * @throws ColumnPersistenceException if unable to persist a column's value
     */
    public Map<String, Pair<Object, CQLDataType>> getPartitionKeyColumnValues(String tname) {
      final TableInfoImpl<T> table = (TableInfoImpl<T>)getTable(tname);

      if (table == null) { // table not defined so nothing to return
        return Collections.emptyMap();
      }
      return table.getPartitionKeyColumnValues(object);
    }

    /**
     * Retrieves all keyspace and partition key columns and their values from the
     * POJO and the specified table.
     *
     * @author paouelle
     *
     * @param  tname the name of the table from which to retrieve columns
     * @return a non-<code>null</code> map of all keyspace key and partition key
     *         column/value pairs for the POJO
     * @throws IllegalArgumentException if a column or a keyspace key is missing from
     *         the POJO
     * @throws ColumnPersistenceException if unable to persist a column's value
     */
    public Map<String, Pair<Object, CQLDataType>> getKeyspaceAndPartitionKeyColumnValues(
      String tname
    ) {
      final TableInfoImpl<T> table = (TableInfoImpl<T>)getTable(tname);

      if (table == null) { // table not defined so return keyspace keys only
        return getKeyspaceKeyValues();
      }
      return table.getKeyspaceAndPartitionKeyColumnValues(object);
    }

    /**
     * Retrieves all primary key columns and their values from the POJO and the
     * specified table.
     *
     * @author paouelle
     *
     * @param  tname the name of the table from which to retrieve columns
     * @return a non-<code>null</code> map of all primary key column/value pairs
     *         for the POJO
     * @throws IllegalArgumentException if a column is missing from the POJO
     * @throws ColumnPersistenceException if unable to persist a column's value
     */
    public Map<String, Pair<Object, CQLDataType>> getPrimaryKeyColumnValues(String tname) {
      final TableInfoImpl<T> table = (TableInfoImpl<T>)getTable(tname);

      if (table == null) { // table not defined so nothing to return
        return Collections.emptyMap();
      }
      return table.getPrimaryKeyColumnValues(object);
    }

    /**
     * Retrieves all primary key columns and their values from the POJO and the
     * specified table while giving priority to values provided by the specified
     * override map.
     *
     * @author paouelle
     *
     * @param  tname the name of the table from which to retrieve columns
     * @param  pkeys_override a non-<code>null</code> map of primary key values
     *         to use instead of those provided by the object
     * @return a non-<code>null</code> map of all primary key column/value pairs
     *         for the POJO
     * @throws IllegalArgumentException if a column is missing from the POJO
     * @throws ColumnPersistenceException if unable to persist a column's value
     */
    public Map<String, Pair<Object, CQLDataType>> getPrimaryKeyColumnValues(
      String tname, Map<String, Object> pkeys_override
    ) {
      final TableInfoImpl<T> table = (TableInfoImpl<T>)getTable(tname);

      if (table == null) { // table not defined so nothing to return
        return Collections.emptyMap();
      }
      return table.getPrimaryKeyColumnValues(object, pkeys_override);
    }

    /**
     * Retrieves all keyspace keys and their values from the POJO.
     *
     * @author paouelle
     *
     * @return a non-<code>null</code> map of all keyspace key name/value pairs
     *         for the POJO
     * @throws IllegalArgumentException if a keyspace key is missing from the POJO
     * @throws ColumnPersistenceException if unable to persist a keyspace key's value
     */
    public Map<String, Pair<Object, CQLDataType>> getKeyspaceKeyValues() {
      final Map<String, Pair<Object, CQLDataType>> values = new LinkedHashMap<>(keyspaceKeys.size());

      for (final Map.Entry<String, FieldInfoImpl<T>> e: getKeyspaceKeys().entrySet()) {
        final String name = e.getKey();
        final FieldInfoImpl<T> field = e.getValue();
        final Object value = field.getValue(object);

        org.apache.commons.lang3.Validate.isTrue(
          value != null, "missing keyspace key '%s'", name
        );
        values.put(name, Pair.of(value, field.getDataType()));
      }
      return values;
    }

    /**
     * Retrieves all keyspace and primary key columns and their values from the
     * POJO and the specified table.
     *
     * @author paouelle
     *
     * @param  tname the name of the table from which to retrieve columns
     * @return a non-<code>null</code> map of all keyspace key and primary key
     *         column/value pairs for the POJO
     * @throws IllegalArgumentException if a column or a keyspace key is missing from
     *         the POJO
     * @throws ColumnPersistenceException if unable to persist a column's value
     */
    public Map<String, Pair<Object, CQLDataType>> getKeyspaceAndPrimaryKeyColumnValues(
      String tname
    ) {
      final TableInfoImpl<T> table = (TableInfoImpl<T>)getTable(tname);

      if (table == null) { // table not defined so return keyspace keys only
        return getKeyspaceKeyValues();
      }
      return table.getKeyspaceAndPrimaryKeyColumnValues(object);
    }

    /**
     * Retrieves all mandatory and primary key columns and their values from
     * the POJO and the specified table.
     *
     * @author paouelle
     *
     * @param  tname the name of the table from which to retrieve columns
     * @return a non-<code>null</code> map of all mandatory and primary key
     *         column/value pairs for the POJO
     * @throws IllegalArgumentException if a column is missing from the POJO
     * @throws ColumnPersistenceException if unable to persist a column's value
     */
    public Map<String, Pair<Object, CQLDataType>> getMandatoryAndPrimaryKeyColumnValues(String tname) {
      final TableInfoImpl<T> table = (TableInfoImpl<T>)getTable(tname);

      if (table == null) { // table not defined so nothing to return
        return Collections.emptyMap();
      }
      return table.getMandatoryAndPrimaryKeyColumnValues(object);
    }

    /**
     * Retrieves all non primary key columns and their non-encoded values from
     * the POJO from the specified table.
     * <p>
     * <i>Note:</i> The returned values should not be encoded.
     *
     * @author paouelle
     *
     * @param  tname the name of the table from which to retrieve columns
     * @return a non-<code>null</code> map of all non primary key column/value
     *         (non-encoded) pairs for the POJO
     * @throws IllegalArgumentException if a mandatory column is missing from
     *         the POJO
     */
    public Map<String, Pair<Object, CQLDataType>> getNonPrimaryKeyColumnNonEncodedValues(
      String tname
    ) {
      final TableInfoImpl<T> table = (TableInfoImpl<T>)getTable(tname);

      if (table == null) { // table not defined so nothing to return
        return Collections.emptyMap();
      }
      return table.getNonPrimaryKeyColumnNonEncodedValues(object);
    }

    /**
     * Retrieves the specified column non-encoded value from the POJO and the
     * specified table.
     *
     * @author paouelle
     *
     * @param  tname the name of the table from which to retrieve the column
     * @param  name the name of the column to retrieve
     * @return the column non-encoded value for the POJO
     * @throws IllegalArgumentException if the column name is not defined by the
     *         POJO or is mandatory and missing from the POJO
     * @throws ColumnPersistenceException if unable to persist a column's value
     */
    public Pair<Object, CQLDataType> getColumnNonEncodedValue(String tname, CharSequence name) {
      final TableInfoImpl<T> table = (TableInfoImpl<T>)getTable(tname);

      if (table == null) { // table not defined so nothing to return
        return Pair.of(null, null);
      }
      return table.getColumnNonEncodedValue(object, name);
    }

   /**
     * Retrieves the specified column value from the POJO and the specified
     * table.
     *
     * @author paouelle
     *
     * @param  tname the name of the table from which to retrieve the column
     * @param  name the name of the column to retrieve
     * @return the column value for the POJO
     * @throws IllegalArgumentException if the column name is not defined by the
     *         POJO or is mandatory and missing from the POJO
     * @throws ColumnPersistenceException if unable to persist a column's value
     */
    public Pair<Object, CQLDataType> getColumnValue(String tname, CharSequence name) {
      final TableInfoImpl<T> table = (TableInfoImpl<T>)getTable(tname);

      if (table == null) { // table not defined so nothing to return
        return Pair.of(null, null);
      }
      return table.getColumnValue(object, name);
    }

    /**
     * Retrieves the specified columns and their values from the POJO and the
     * specified table.
     *
     * @author paouelle
     *
     * @param  tname the name of the table from which to retrieve the column
     * @param  names the names of the columns to retrieve
     * @return a non-<code>null</code> map of all requested column/value pairs
     *         for the POJO
     * @throws IllegalArgumentException if any of the column names are not defined
     *         by the POJO or is mandatory and missing from the POJO
     * @throws ColumnPersistenceException if unable to persist a column's value
     */
    public Map<String, Pair<Object, CQLDataType>> getColumnValues(
      String tname, Iterable<CharSequence> names
    ) {
      final TableInfoImpl<T> table = (TableInfoImpl<T>)getTable(tname);

      if (table == null) { // table not defined so nothing to return
        return Collections.emptyMap();
      }
      return table.getColumnValues(object, names);
    }

    /**
     * Retrieves the specified columns and their values from the POJO and the
     * specified table.
     *
     * @author paouelle
     *
     * @param  tname the name of the table from which to retrieve the column
     * @param  names the names of the columns to retrieve
     * @return a non-<code>null</code> map of all requested column/value pairs
     *         for the POJO
     * @throws IllegalArgumentException if any of the column names are not defined
     *         by the POJO or is mandatory and missing from the POJO
     * @throws ColumnPersistenceException if unable to persist a column's value
     */
    public Map<String, Pair<Object, CQLDataType>> getColumnValues(
      String tname, CharSequence... names
    ) {
      final TableInfoImpl<T> table = (TableInfoImpl<T>)getTable(tname);

      if (table == null) { // table not defined so nothing to return
        return Collections.emptyMap();
      }
      return table.getColumnValues(object, names);
    }

    /**
     * Adds a keyspace key to the context for use in keyspace name creation by
     * retrieving its value from the associated POJO.
     *
     * @author paouelle
     *
     * @param  name the keyspace name
     * @throws NullPointerException if <code>name</code> is <code>null</code>
     * @throws IllegalArgumentException if the POJO doesn't require the specified
     *         keyspace key or if the value doesn't match the POJO's definition for the
     *         specified keyspace key
     * @throws ExcludedKeyspaceKeyException if the specified keyspace key value is
     *         marked as excluded the specified keyspace key
     */
    public void addKeyspaceKey(String name) {
      org.apache.commons.lang3.Validate.notNull(name, "invalid null name");
      final FieldInfoImpl<T> field = keyspaceKeysByName.get(name);

      org.apache.commons.lang3.Validate.isTrue(
        field != null,
        "%s doesn't define keyspace key: %s",
        clazz.getSimpleName(),
        name
      );
      final Object val = field.getValue(object);

      validateKeyspaceKey(name, val);
      keyspaceKeys.put(name, val);
    }
  }

  /**
   * Holds the entity annotation class used to annotated the POJO class.
   *
   * @author paouelle
   */
  protected final Class<? extends Annotation> entityAnnotationClass;

  /**
   * Holds the class for the POJO.
   *
   * @author vasu
   */
  protected final Class<T> clazz;

  /**
   * Holds the default serialization constructor to use to instantiate a blank
   * POJO.
   *
   * @author paouelle
   */
  protected final Constructor<T> constructor;

  /**
   * Holds the map of all final fields with their default values.
   *
   * @author paouelle
   */
  protected final Map<Field, Object> finalFields;

  /**
   * Holds the optional initial objects factory methods.
   *
   * @author paouelle
   */
  private final Set<Method> initials;

  /**
   * Holds the keyspace annotation.
   *
   * @author vasu
   */
  private final Keyspace keyspace;

  /**
   * Holds the table information defined for the POJO.
   *
   * @author paouelle
   */
  private final Map<String, TableInfoImpl<T>> tables;

  /**
   * Holds the primary table for the POJO.
   *
   * @author paouelle
   */
  private final TableInfoImpl<T> primary;

  /**
   * Holds the set of column names defined by the POJO.
   *
   * @author paouelle
   */
  private final Set<String> columns;

  /**
   * Holds a map of all fields annotated as keyspace keys keyed by the
   * keyspace key name.
   *
   * @author paouelle
   */
  protected final Map<String, FieldInfoImpl<T>> keyspaceKeysByName;

  /**
   * Holds a map of all fields annotated as keyspace keys keyed by the
   * keyspace key type.
   *
   * @author paouelle
   */
  protected final Map<String, FieldInfoImpl<T>> keyspaceKeysByType;

  /**
   * Instantiates a new <code>ClassInfo</code> object.
   *
   * @author paouelle
   *
   * @param  mgr the non-<code>null</code> statement manager
   * @param  clazz the class of POJO for which to get a class info object for
   * @param  entityAnnotationClass the non-<code>null</code> entity annotation
   *         class to compute from
   * @throws NullPointerException if <code>clazz</code> is <code>null</code>
   * @throws IllegalArgumentException if <code>clazz</code> doesn't represent
   *         a valid POJO class
   */
  ClassInfoImpl(
    StatementManagerImpl mgr,
    Class<T> clazz,
    Class<? extends Annotation> entityAnnotationClass
  ) {
    org.apache.commons.lang3.Validate.notNull(clazz, "invalid null POJO class");
    this.columns = new LinkedHashSet<>(25);
    this.tables = new LinkedHashMap<>(12);
    this.keyspaceKeysByName = new LinkedHashMap<>(8);
    this.keyspaceKeysByType = new LinkedHashMap<>(8);
    this.entityAnnotationClass = entityAnnotationClass;
    this.clazz = clazz;
    this.constructor = findDefaultCtor(entityAnnotationClass);
    this.finalFields = findFinalFields();
    this.keyspace = findKeyspace();
    this.primary = findTables(mgr);
    findColumns();
    findKeyspaceKeys();
    this.initials = findInitials();
  }

  /**
   * Instantiates a new <code>ClassInfo</code> object.
   *
   * @author paouelle
   *
   * @param  mgr the non-<code>null</code> statement manager
   * @param  clazz the class of POJO for which to get a class info object for
   * @throws NullPointerException if <code>clazz</code> is <code>null</code>
   * @throws IllegalArgumentException if <code>clazz</code> doesn't represent
   *         a valid POJO class
   */
  ClassInfoImpl(StatementManagerImpl mgr, Class<T> clazz) {
    this(mgr, clazz, Entity.class);
    org.apache.commons.lang3.Validate.isTrue(
      !Modifier.isAbstract(clazz.getModifiers()),
      "entity class '%s', cannot be abstract", clazz.getSimpleName()
    );
  }

  /**
   * Instantiates a new <code>ClassInfo</code> object.
   *
   * @author paouelle
   *
   * @param  cinfo the non-<code>null</code> class info to link to
   * @param  clazz the class of POJO for which to get a class info object for
   * @throws NullPointerException if <code>clazz</code> is <code>null</code>
   * @throws IllegalArgumentException if <code>clazz</code> doesn't represent
   *         a valid POJO class
   */
  ClassInfoImpl(ClassInfoImpl<T> cinfo, Class<T> clazz) {
    this.entityAnnotationClass = cinfo.entityAnnotationClass;
    this.clazz = clazz;
    this.constructor = cinfo.constructor;
    this.finalFields = cinfo.finalFields;
    this.keyspace = cinfo.keyspace;
    this.primary = cinfo.primary;
    this.columns = cinfo.columns;
    this.initials = cinfo.initials;
    this.tables = cinfo.tables;
    this.keyspaceKeysByName = cinfo.keyspaceKeysByName;
    this.keyspaceKeysByType = cinfo.keyspaceKeysByType;
  }

  /**
   * Finds a default constructor for the POJO.
   *
   * @author paouelle
   *
   * @param  entityAnnotationClass the non-<code>null</code> entity annotation
   *         class to compute from
   * @return the non-<code>null</code> special serialization default
   *         constructor
   * @throws IllegalArgumentException if unable to access the default
   *         constructor for the POJO class
   */
  private Constructor<T> findDefaultCtor(
    Class<? extends Annotation> entityAnnotationClass
  ) {
    // use the serialization constructor instead of the default one such
    // that all fields be initialized by Java to their default values
    // (i.e. null, 0, 0L, false, ...) and not by any defined ctors
    try {
      return ReflectionUtils.getSerializationConstructorFromAnnotation(
        clazz, entityAnnotationClass
      );
    } catch (Exception e) {
      throw new IllegalArgumentException(
        "unable to access serialization constructor: "
        + clazz.getName()
        + "()",
        e
      );
    }
  }

  /**
   * Finds all final fields in this class and all super classes up to and
   * excluding the first class that is not annotated with the corresponding
   * entity annotation.
   *
   * @author paouelle
   *
   * @return a non-<code>null</code> map of all final fields and their default
   *         values
   */
  private Map<Field, Object> findFinalFields() {
    final Map<Field, Object> ffields = new HashMap<>(8);

    // if this class is abstract then we know we won't be instantiating it
    // anyway, so we can just skip this as any derived class would hit its
    // default ctor and thus; properly initialize its final fields if any
    if (Modifier.isAbstract(clazz.getModifiers())) {
      return ffields;
    }
    MutableObject<Object> obj = null; // lazy evaluated

    // go up the hierarchy until we hit the class for which we have a default
    // serialization constructor as that class will have its final fields
    // properly initialized by the ctor whereas all others will have their final
    // fields initialized with 0, false, null, ...
    for (Class<? super T> clazz = this.clazz;
         clazz != constructor.getDeclaringClass();
         clazz = clazz.getSuperclass()) {
      for (final Field field: clazz.getDeclaredFields()) {
        final int mods = field.getModifiers();

        if (Modifier.isFinal(mods) && !Modifier.isStatic(mods)) {
          field.setAccessible(true); // so we can access its value directly
          if (obj == null) {
            // instantiates a dummy version and access its value
            try {
              // find default ctor even if private
              final Constructor<T> ctor = this.clazz.getDeclaredConstructor();

              ctor.setAccessible(true); // in case it was private
              final T t = ctor.newInstance();

              obj = new MutableObject<>(t);
            } catch (NoSuchMethodException|IllegalAccessException|InstantiationException e) {
              throw new IllegalArgumentException(
                "unable to instantiate object: " + this.clazz.getName(), e
              );
            } catch (InvocationTargetException e) {
              final Throwable t = e.getTargetException();

              if (t instanceof Error) {
                throw (Error)t;
              } else if (t instanceof RuntimeException) {
                throw (RuntimeException)t;
              } else { // we don't expect any of those
                throw new IllegalArgumentException(
                  "unable to instantiate object: " + this.clazz.getName(), t
                );
              }
            }
          }
          try {
            ffields.put(field, field.get(obj.getValue()));
          } catch (IllegalAccessException e) {
            throw new IllegalArgumentException(
              "unable to access final value for field: "
              + field.getDeclaringClass().getName()
              + "."
              + field.getName(), e
            );
          }
        }
      }
    }
    return ffields;
  }

  /**
   * Finds initial objects factory methods for the POJO if configured.
   *
   * @author paouelle
   *
   * @return the non-<code>null</code> initial objects factory methods
   * @throws IllegalArgumentException if the initial objects methods are not
   *         properly defined
   */
  private Set<Method> findInitials() {
    final Set<Method> initials = ReflectionUtils.getAllAnnotationsForMethodsAnnotatedWith(
      clazz, InitialObjects.class, true
    ).keySet();

    initials.forEach(m -> {
      // validate the method is static
      if (!Modifier.isStatic(m.getModifiers())) {
        throw new IllegalArgumentException(
          "initial objects method '"
          + m.getName()
          + "' is not static in class: "
          + clazz.getSimpleName()
        );
      }
      // validate the return type is compatible with this class
      final Class<?> type = m.getReturnType();

      if (type.isArray()) {
        final Class<?> ctype = type.getComponentType();

        if (!ctype.isAssignableFrom(clazz)) {
          throw new IllegalArgumentException(
            "incompatible returned array of class '"
            + ctype.getName()
            + "' for initial objects method '"
            + m.getName()
            + "' in class: "
            + clazz.getSimpleName()
          );
        }
      } else if (!clazz.isAssignableFrom(type)) {
        // must be a collection, stream, iterator, enumeration, iterable
        if (!Collection.class.isAssignableFrom(type)
            && !Stream.class.isAssignableFrom(type)
            && !Iterator.class.isAssignableFrom(type)
            && !Enumeration.class.isAssignableFrom(type)
            && !Iterable.class.isAssignableFrom(type)) {
          throw new IllegalArgumentException(
            "incompatible returned class '"
            + type.getName()
            + "' for initial objects method '"
            + m.getName()
            + "' in class: "
            + clazz.getSimpleName()
          );
        }
        // now check its argument type
        final Type rtype = m.getGenericReturnType();

        if (rtype instanceof ParameterizedType) {
          final ParameterizedType ptype = (ParameterizedType)rtype;

          // the expected types will always have only 1 argument
          if (ptype.getActualTypeArguments().length != 1) {
            throw new IllegalArgumentException(
              "incompatible returned type '"
              + ptype.getTypeName()
              + "' for initial objects method '"
              + m.getName()
              + "' in class: "
              + clazz.getSimpleName()
            );
          }
          final Class<?> aclazz = ReflectionUtils.getRawClass(ptype.getActualTypeArguments()[0]);

          if (!clazz.isAssignableFrom(aclazz)) {
            throw new IllegalArgumentException(
              "incompatible returned type argument '"
              + aclazz.getName()
              + "' for initial objects method '"
              + m.getName()
              + "' in class: "
              + clazz.getSimpleName()
            );
          }
        } else {
          throw new IllegalArgumentException(
            "incompatible returned type '"
            + rtype.getTypeName()
            + "' for initial objects method '"
            + m.getName()
            + "' in class: "
            + clazz.getSimpleName()
          );
        }
      }
      // validate that if keyspace keys are defined, the method expects a Map<String, String>
      // to provide the values for the keyspace keys when initializing objects
      final Class<?>[] cparms = m.getParameterTypes();

      if (keyspaceKeysByType.isEmpty()) {
        // should always be 0 as we used no classes in getMethod()
        if (cparms.length != 0) {
          throw new IllegalArgumentException(
            "expecting no parameters for initial objects method '"
            + m.getName()
            + "' in class: "
            + clazz.getSimpleName()
          );
        }
      } else {
        // should always be 1 as we used only 1 class in getMethod()
        if (cparms.length != 1) {
          throw new IllegalArgumentException(
            "expecting one Map<String, String> parameter for initial objects method '"
            + m.getName()
            + "' in class: "
            + clazz.getSimpleName()
          );
        }
        // should always be a map as we used a Map to find the method
        if (!Map.class.isAssignableFrom(cparms[0])) {
          throw new IllegalArgumentException(
            "expecting parameter for initial objects method '"
            + m.getName()
            + "' to be of type Map<String, String> in class: "
            + clazz.getSimpleName()
          );
        }
        final Type[] tparms = m.getGenericParameterTypes();

        // should always be 1 as we used only 1 class in getMethod()
        if (tparms.length != 1) { // should always be 1 as it was already tested above
          throw new IllegalArgumentException(
            "expecting one Map<String, String> parameter for initial objects method '"
            + m.getName()
            + "' in class: "
            + clazz.getSimpleName()
          );
        }
        if (tparms[0] instanceof ParameterizedType) {
          final ParameterizedType ptype = (ParameterizedType)tparms[0];

          // maps will always have 2 arguments
          for (final Type atype: ptype.getActualTypeArguments()) {
            final Class<?> aclazz = ReflectionUtils.getRawClass(atype);

            if (String.class != aclazz) {
              throw new IllegalArgumentException(
                "expecting a Map<String, String> parameter for initial objects method '"
                + m.getName()
                + "' in class: "
                + clazz.getSimpleName()
              );
            }
          }
        } else {
          throw new IllegalArgumentException(
            "expecting a Map<String, String> parameter for initial objects method '"
            + m.getName()
            + "' in class: "
            + clazz.getSimpleName()
          );
        }
      }
    });
    return initials;
  }

  /**
   * Finds and return the keyspace annotation.
   *
   * @author paouelle
   *
   * @return the non-<code>null</code> keyspace annotation
   * @throws IllegalArgumentException if the POJO class is improperly annotated
   */
  private Keyspace findKeyspace() {
    final Keyspace keyspace = clazz.getAnnotation(Keyspace.class);

    org.apache.commons.lang3.Validate.isTrue(
      keyspace != null,
      "%s is not annotated with @Keyspace",
      clazz.getSimpleName()
    );
    // make sure that a keyspace name or key exist
    org.apache.commons.lang3.Validate.isTrue(
      !keyspace.name().isEmpty() || (keyspace.keys().length != 0),
      "@Keyspace annotation for %s must defined at least one of 'name' or 'keys'",
      clazz.getSimpleName()
    );
    return keyspace;
  }

  /**
   * Finds and records all tables annotated on the POJO class.
   *
   * @author paouelle
   *
   * @param  mgr the non-<code>null</code> statement manager
   * @return the primary table for this POJO or <code>null</code> if none defined
   * @throws IllegalArgumentException if the POJO class is improperly annotated
   */
  private TableInfoImpl<T> findTables(StatementManagerImpl mgr) {
    final Map<String, Table> tables = ReflectionUtils.getAnnotationsByType(
      String.class, Table.class, clazz
    );
    TableInfoImpl<T> primary = null;

    if (this instanceof UDTClassInfoImpl) {
      org.apache.commons.lang3.Validate.isTrue(
        tables.isEmpty(),
        "%s is annotated with @Table",
        clazz.getSimpleName()
      );
      // create a dummy table info
      final UDTEntity ue = clazz.getAnnotation(UDTEntity.class);

      if (ue == null) {
        org.apache.commons.lang3.Validate.isTrue(
          ReflectionUtils.findFirstClassAnnotatedWith(clazz, UDTRootEntity.class) != null,
          "class '%s' is not annotated with @UDTEntity or @UDTRootEntity",
          clazz.getSimpleName()
        );
      }
      // create fake table with "udt" as the name
      this.tables.put("udt", new TableInfoImpl<>(mgr, (UDTClassInfoImpl<T>)this, "udt"));
    } else {
      org.apache.commons.lang3.Validate.isTrue(
        !tables.isEmpty(),
        "%s is not annotated with @Table",
        clazz.getSimpleName()
      );
      for (final Table table: tables.values()) {
        final TableInfoImpl<T> tinfo = new TableInfoImpl<>(mgr, this, table);

        this.tables.put(tinfo.getName(), tinfo);
        if (tinfo.getTable().primary()) {
          if (primary != null) {
            throw new IllegalArgumentException(
              "class '"
              + clazz.getSimpleName()
              + "' annotates 2 primary tables: '"
              + primary.getName()
              + "' and '"
              + tinfo.getName()
              + "'"
            );
          }
          primary = tinfo;
        }
      }
    }
    return primary;
  }

  /**
   * Finds and records all columns annotated in the POJO class.
   *
   * @author paouelle
   */
  private void findColumns() {
    // make sure to walk up the class hierarchy
    for (final Map.Entry<Field, Column[]> e: ReflectionUtils.getAllAnnotationsForFieldsAnnotatedWith(
      clazz, Column.class, true
    ).entrySet()) {
      for (final Column column: e.getValue()) {
        columns.add(column.name());
      }
    }
  }

  /**
   * Finds and records all fields annotated as keyspace keys.
   *
   * @author paouelle
   *
   * @throws IllegalArgumentException if unable to find a getter or setter
   *         method for the field of if improperly annotated
   */
  private void findKeyspaceKeys() {
    final Set<String> kkeys = new HashSet<>(keyspace.keys().length * 3 / 2);

    for (final String s: keyspace.keys()) {
      kkeys.add(s);
    }
    // check keyspace keys defined on the class itself
    final KeyspaceKey[] keys = clazz.getAnnotationsByType(KeyspaceKey.class);

    if (this instanceof UDTClassInfoImpl) {
      for (final KeyspaceKey key: keys) {
        org.apache.commons.lang3.Validate.isTrue(
          !keyspaceKeysByType.containsKey(key.type()),
          "multiple @KeyspaceKey annotations found with type '%s' for class: %s",
          key.type(),
          clazz.getSimpleName()
        );
        org.apache.commons.lang3.Validate.isTrue(
          !keyspaceKeysByName.containsKey(key.name()),
          "multiple @KeyspaceKey annotations found with name '%s' for class: %s",
          key.name(),
          clazz.getSimpleName()
        );
        org.apache.commons.lang3.Validate.isTrue(
          kkeys.remove(key.type()),
          "@Keyspace annotation does not define keyspace key type '%s' for class: %s",
          key.type(),
          clazz.getSimpleName()
        );
        final FieldInfoImpl<T> field = new FieldInfoImpl<>(this, key);

        keyspaceKeysByName.put(key.name(), field);
        keyspaceKeysByType.put(key.type(), field);
      }
    } else {
      org.apache.commons.lang3.Validate.isTrue(
        keys.length == 0,
        "%s POJOs do not support @KeyspaceKey annotations on the type; define a field instead",
        getEntityAnnotationClass().getSimpleName()
      );
      // make sure to walk up the class hierarchy
      for (final Field f: ReflectionUtils.getAllFieldsAnnotatedWith(
        clazz, KeyspaceKey.class, true
      )) {
        final FieldInfoImpl<T> field = new FieldInfoImpl<>(this, f);
        final KeyspaceKey key = field.getKeyspaceKey();

        org.apache.commons.lang3.Validate.isTrue(
          !keyspaceKeysByType.containsKey(key.type()),
          "multipe @KeyspaceKey annotations found with type '%s' for class: %s",
          key.type(),
          clazz.getSimpleName()
        );
        org.apache.commons.lang3.Validate.isTrue(
          !keyspaceKeysByName.containsKey(key.name()),
          "multipe @KeyspaceKey annotations found with name '%s' for class: %s",
          key.name(),
          clazz.getSimpleName()
        );
        org.apache.commons.lang3.Validate.isTrue(
          kkeys.remove(key.type()),
          "@Keyspace annotation does not define keyspace key type '%s' for class: %s",
          key.type(),
          clazz.getSimpleName()
        );
        keyspaceKeysByName.put(key.name(), field);
        keyspaceKeysByType.put(key.type(), field);
      }
    }
    org.apache.commons.lang3.Validate.isTrue(
      kkeys.isEmpty(),
      "missing @KeyspaceKey annotations for @Keyspace defined keyspace keys: %s for class: %s",
      StringUtils.join(kkeys, ", "),
      clazz.getSimpleName()
    );
  }

  /**
   * Sets the specified keyspace key fields in the POJO object.
   *
   * @author paouelle
   *
   * @param  object the non-<code>null</code> POJO object
   * @param  row the non-<code>null</code> row being decoded to the POJO
   * @param  map a non-<code>null</code> map of keyspace keys to fields to use
   * @param  values the keyspace key values defined by the statement used when issuing
   *         the query
   * @throws ObjectConversionException if unable to set keyspace keys in the POJO
   */
  private void setKeyspaceKeyFields(
    T object,
    Row row,
    Map<String, FieldInfoImpl<T>> map,
    Map<String, Object> values
  ) {
    for (final Map.Entry<String, Object> e: values.entrySet()) {
      final String name = e.getKey();
      final Object value = e.getValue();
      final FieldInfoImpl<T> field = map.get(name);

      if (field != null) {
        try {
          field.setValue(object, value);
        } catch (IllegalArgumentException iae) {
          throw new ObjectConversionException(
            clazz,
            row,
            "unable to set '" + name + "' keyspace key in object",
            iae
          );
        }
      }
    }
  }

  /**
   * Decodes the column fields from a row and sets the decoded value in the POJO
   * object.
   *
   * @author paouelle
   *
   * @param  object the non-<code>null</code> POJO object
   * @param  row the non-<code>null</code> row being decoded to the POJO
   * @throws ObjectConversionException if unable to decode the row in the POJO
   */
  private void decodeAndSetColumnFields(T object, Row row) {
    for (final ColumnDefinitions.Definition coldef: row.getColumnDefinitions()) {
      // find the table for this column
      final TableInfoImpl<T> table = (TableInfoImpl<T>)getTable(coldef.getTable());

      if (table != null) {
        // find the field in the table for this column
        final FieldInfoImpl<T> field = table.getColumnImpl(coldef.getName());

        if (field != null) {
          // now let's set the value for this column
          field.decodeAndSetValue(object, row);
        }
      }
    }
  }

  /**
   * Validates if a column is defined by the POJO.
   *
   * @author paouelle
   *
   * @param  name the column name to validate
   * @param  ignoreNonColumnNames <code>true</code> to not generate an exception if
   *         the specified name cannot be interpreted as a column name;
   *         <code>false</code> to generate an exception
   * @throws NullPointerException if <code>name</code> is <code>null</code>
   * @throws IllegalArgumentException if any of the specified columns are not defined
   *         by the POJO
   */
  private void validateColumn(Object name, boolean ignoreNonColumnNames) {
    org.apache.commons.lang3.Validate.notNull(name, "invalid null column name");
    if (name instanceof Utils.CNameSequence) {
      for (final String n: ((Utils.CNameSequence)name).getNames()) {
        validateColumn(n); // recurse to validate
      }
      return;
    }
    final String n;

    if (name instanceof Utils.FCall) {
      for (final Object parm: ((Utils.FCall)name)) {
        validateColumn(parm, true);
      }
      return;
    }
    if (name instanceof Utils.CName) {
      n = ((Utils.CName)name).getColumnName();
    } else if (name instanceof CharSequence) {
      n = name.toString();
    } else {
      if (ignoreNonColumnNames) { // ignore it
        return;
      }
      throw new IllegalArgumentException(
        "unexpected column name '" + name + "'"
      );
    }
    org.apache.commons.lang3.Validate.isTrue(
      columns.contains(n),
      "%s doesn't define column '%s'",
      clazz.getSimpleName(),
      n
    );
  }

  /**
   * Gets the tables info defined by the POJO.
   *
   * @author paouelle
   *
   * @return a stream of all defined tables info
   */
  protected Stream<TableInfoImpl<T>> tablesImpl() {
    return tables.values().stream();
  }

  /**
   * Gets the tables info defined by the POJO.
   *
   * @author paouelle
   *
   * @return a collection of all defined tables info
   */
  protected Collection<TableInfoImpl<T>> getTablesImpl() {
    return tables.values();
  }

  /**
   * Gets the entity annotation class used to annotated the POJO class.
   *
   * @author paouelle
   *
   * @return the entity annotation class used to annotated the POJO class
   */
  public Class<? extends Annotation> getEntityAnnotationClass() {
    return entityAnnotationClass;
  }

  /**
   * Gets the class of POJO represented by this class info object.
   *
   * @author paouelle
   *
   * @return the non-<code>null</code> type of POJO represented by this class
   *         info
   */
  @Override
  public Class<T> getObjectClass() {
    return clazz;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.info.ClassInfo#supportsTablesAndIndexes()
   */
  @Override
  public boolean supportsTablesAndIndexes() {
    return true;
  }

  /**
   * Gets all classes (including type classes if this is a root class) for
   * the POJO represented by this class info object.
   *
   * @author paouelle
   *
   * @return a stream of classes of POJOs (including type classes if this is a
   *         root class) represented by this class info
   */
  public Stream<Class<? extends T>> objectClasses() {
    return Stream.of(clazz);
  }

  /**
   * Gets all class infos (including type classes if this is a root class)
   * for the POJO.
   *
   * @author paouelle
   *
   * @return a stream of all class infos (including type classes if this is a
   *         root class) for the POJO
   */
  public Stream<ClassInfoImpl<? extends T>> classInfos() {
    return Stream.of(this);
  }

  /**
   * Gets all user-defined types the pojo class represented by this class is
   * dependent on.
   *
   * @author paouelle
   *
   * @return a stream of all class infos for the user-defined types the pojo
   *         class depends on
   */
  public Stream<UDTClassInfoImpl<?>> udts() {
    return tables.values().stream().flatMap(t -> t.udts()).distinct();
  }

  /**
   * Gets the default value for the specified final field.
   *
   * @author paouelle
   *
   * @param  field the non-<code>null</code> final field for which to gets its
   *         default value
   * @return the corresponding default value
   * @throws IllegalArgumentException if the field is not a defined final field
   *         for the associated class or one of its super class up to and
   *         excluding the first one that is not annotated with the entity
   *         annotation
   */
  public Object getDefaultValue(Field field) {
    final Object dflt = finalFields.getOrDefault(field, this);

    org.apache.commons.lang3.Validate.isTrue(
      dflt != this,
      "field '%s.%s' is not a valid final field for class '%s'",
      field.getDeclaringClass().getName(), field.getName(), clazz
    );
    return dflt;
  }

  /**
   * Gets the keyspace annotation for this POJO.
   *
   * @author paouelle
   *
   * @return the non-<code>null</code> keyspace annotation for this POJO
   */
  @Override
  public Keyspace getKeyspace() {
    return keyspace;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.info.ClassInfo#getNumKeyspaceKeys()
   */
  @Override
  public int getNumKeyspaceKeys() {
    return keyspaceKeysByType.size();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.info.ClassInfo#getKeyspaceKey(java.lang.String)
   */
  @Override
  public FieldInfo<T> getKeyspaceKey(String name) {
    return keyspaceKeysByName.get(name);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.info.ClassInfo#getKeyspaceKeyByType(java.lang.String)
   */
  @Override
  public FieldInfo<T> getKeyspaceKeyByType(String type) {
    return keyspaceKeysByType.get(type);
  }

  /**
   * Checks if the specified name is defined as a keyspace key.
   *
   * @author paouelle
   *
   * @param  name the name of the keyspace key
   * @return <code>true</code> if that name is defined as a keyspace key;
   *         <code>false</code> otherwise
   */
  public boolean isKeyspaceKey(String name) {
    final FieldInfoImpl<T> field = keyspaceKeysByName.get(name);

    return (field != null) ? field.isKeyspaceKey() : false;
  }

  /**
   * Gets the field info for all keyspace keys defined by this POJO.
   *
   * @author paouelle
   *
   * @return the non-<code>null</code> map of all keyspace key fields keyed by
   *         their names
   */
  public Map<String, FieldInfoImpl<T>> getKeyspaceKeys() {
    return keyspaceKeysByName;
  }

  /**
   * Gets the field info for all keyspace key types defined by this POJO.
   *
   * @author paouelle
   *
   * @return the non-<code>null</code> map of all keyspace key type fields keyed by
   *         their types
   */
  public Map<String, FieldInfoImpl<T>> getKeyspaceKeyTypes() {
    return keyspaceKeysByType;
  }

  /**
   * Gets the table info corresponding to the given table name.
   *
   * @author paouelle
   *
   * @param  name the name of the table to get
   * @return the corresponding table info
   * @throws NullPointerException if <code>name</code> is <code>null</code>
   * @throws IllegalArgumentException if the specified table is not defined by
   *         the POJO
   */
  public TableInfoImpl<T> getTableImpl(String name) {
    org.apache.commons.lang3.Validate.notNull(name, "invalid null table name");
    // start with provided name
    TableInfoImpl<T> tinfo = tables.get(name);

    if (tinfo == null) {
      // try again with a cleaned up version
      tinfo = tables.get(TableInfoImpl.cleanName(name));
    }
    org.apache.commons.lang3.Validate.isTrue(
      tinfo != null,
      "%s doesn't define table '%s'",
      clazz.getName(),
      name
    );
    return tinfo;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.info.ClassInfo#getTable(java.lang.String)
   */
  @Override
  public TableInfo<T> getTable(String name) {
    return getTableImpl(name);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.info.ClassInfo#getPrimaryTable()
   */
  @Override
  public Optional<TableInfo<T>> getPrimaryTable() {
    return Optional.ofNullable(primary);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.info.ClassInfo#getAuditTable()
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  public Optional<TableInfo<T>> getAuditTable() {
    return ((Stream<TableInfo<T>>)(Stream)tables.values().stream())
      .filter(t -> (Table.Type.AUDIT == t.getTable().type()))
      .findAny();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.info.ClassInfo#getNumTables()
   */
  @Override
  public int getNumTables() {
    return tables.size();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.info.ClassInfo#getTables()
   */
  @Override
  public Collection<TableInfo<T>> getTables() {
    return Collections.unmodifiableCollection(tables.values());
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.info.ClassInfo#tables()
   */
  @SuppressWarnings({"rawtypes", "cast", "unchecked"})
  @Override
  public Stream<TableInfo<T>> tables() {
    return (Stream<TableInfo<T>>)(Stream)tables.values().stream();
  }


  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.info.ClassInfo#iterator()
   */
  @Override
  public Iterator<TableInfo<T>> iterator() {
    return getTables().iterator();
  }

  /**
   * Retrieves all columns from the POJO no matter which table they are
   * defined in.
   *
   * @author paouelle
   *
   * @return a non-<code>null</code> set of all columns for the POJO
   */
  public Collection<String> getColumns() {
    return columns;
  }

  /**
   * Creates a new context for this class info.
   *
   * @author paouelle
   *
   * @return a non-<code>null</code> newly created context for this class info
   */
  public Context newContext() {
    return new Context();
  }

  /**
   * Creates a new context for this class info with the given POJO object.
   *
   * @author paouelle
   *
   * @param  object the POJO object
   * @return a non-<code>null</code> newly created context for this class info
   * @throws NullPointerException if <code>object</code> is <code>null</code>
   * @throws IllegalArgumentException if <code>object</code> is not of the
   *         appropriate class
   */
  public POJOContext newContext(T object) {
    return new POJOContext(object);
  }

  /**
   * Checks if the specified name is defined as a column for any tables.
   *
   * @author paouelle
   *
   * @param  name the name of the column
   * @return <code>true</code> if that name is defined as a column for any tables;
   *         <code>false</code> otherwise
   */
  public boolean isColumn(String name) {
    return columns.contains(name);
  }

  /**
   * Validates if a column is defined by the POJO in any tables.
   *
   * @author paouelle
   *
   * @param  name the column name to validate
   * @throws NullPointerException if <code>name</code> is <code>null</code>
   * @throws IllegalArgumentException if any of the specified columns are not defined
   *         by the POJO
   */
  public void validateColumn(Object name) {
    validateColumn(name, false);
  }

  /**
   * Validates if a column is defined by the POJO in any tables or if it is
   * defined as a keyspace key.
   *
   * @author paouelle
   *
   * @param  name the column name or keyspace key to validate
   * @throws NullPointerException if <code>name</code> is <code>null</code>
   * @throws IllegalArgumentException if the specified column or keyspace key is
   *         not defined by the POJO
   */
  public void validateColumnOrKeyspaceKey(String name) {
    org.apache.commons.lang3.Validate.notNull(name, "invalid null column name or keyspace key");
    org.apache.commons.lang3.Validate.isTrue(
      columns.contains(name) || keyspaceKeysByName.containsKey(name),
      "%s doesn't define column or keyspace key '%s'",
      clazz.getSimpleName(),
      name
    );
  }

  /**
   * Validates if specified columns are defined by the POJO in any tables.
   *
   * @author paouelle
   *
   * @param  names the names of the column to validate
   * @throws NullPointerException if any of the column names are <code>null</code>
   * @throws IllegalArgumentException if any of the column names are not
   *         defined by the POJO
   */
  public void validateColumns(Iterable<Object> names) {
    for (final Object name: names) {
      validateColumn(name);
    }
  }

  /**
   * Validates if specified columns are defined by the POJO in any table.
   *
   * @author paouelle
   *
   * @param  names the names of the column to validate
   * @throws NullPointerException if any of the column names are <code>null</code>
   * @throws IllegalArgumentException if any of the column names are not
   *         defined by the POJO
   */
  public void validateColumns(String... names) {
    for (final String name: names) {
      validateColumn(name);
    }
  }

  /**
   * Validates if a column is defined by the POJO and its potential value in
   * any table.
   *
   * @author paouelle
   *
   * @param  name the column name to validate
   * @param  value the value to validate for the column
   * @throws NullPointerException if <code>name</code> is <code>null</code>
   * @throws IllegalArgumentException if the specified column is not defined
   *         by the POJO or if the specified value is not of the
   *         right type or is <code>null</code> when the column is mandatory
   */
  public void validateColumnAndValue(String name, Object value) {
    org.apache.commons.lang3.Validate.notNull(name, "invalid null column name");
    final FieldInfoImpl<T> field = tables.values().stream()
      .map(t -> t.getColumnImpl(name))
      .filter(f -> f != null)
      .findAny()
      .orElse(null);

    org.apache.commons.lang3.Validate.isTrue(
      field != null,
      "%s doesn't define column '%s'",
      clazz.getSimpleName(),
      name
    );
    field.validateValue(value);
  }

  /**
   * Validates the specified keyspace key and its value as being a valid keyspace
   * key for the POJO.
   *
   * @author paouelle
   *
   * @param  name the keyspace key to validate
   * @param  value the value for the keyspace key
   * @throws IllegalArgumentException if the keyspace key is not defined by this POJO
   *         if the specified value is not of the right type or is
   *         <code>null</code> when the field is mandatory
   * @throws ExcludedKeyspaceKeyException if the specified keyspace key value is
   *         marked as excluded the specified keyspace key
   */
  public void validateKeyspaceKey(String name, Object value) {
    org.apache.commons.lang3.Validate.notNull(name, "invalid null name");
    org.apache.commons.lang3.Validate.notNull(value, "invalid null value");
    final FieldInfoImpl<T> field = keyspaceKeysByName.get(name);

    org.apache.commons.lang3.Validate.isTrue(
      field != null,
      "%s doesn't define keyspace key: %s",
      clazz.getSimpleName(),
      name
    );
    field.validateValue(value);
  }

  /**
   * Converts the specified result row into a POJO object defined by this
   * class information and keyspace key map.
   *
   * @author paouelle
   *
   * @param  row the result row to convert into a POJO
   * @param  keyspaceKeys a map of keyspace key values to report back into the created
   *         POJO
   * @return the POJO object corresponding to the given result row
   * @throws NullPointerException if <code>keyspaceKeys</code> is <code>null</code>
   * @throws ObjectConversionException if unable to convert to a POJO
   */
  public T getObject(Row row, Map<String, Object> keyspaceKeys) {
    if (row == null) {
      return null;
    }
    org.apache.commons.lang3.Validate.notNull(keyspaceKeys, "invalid null keyspace keys");
    try {
      // create an empty shell for the pojo
      final T object = constructor.newInstance();

      // start by setting back all final fields' values
      finalFields.forEach(
        (field, value) -> {
          try {
            // set it in field directly
            field.set(object, value);
          } catch (IllegalAccessException e) { // should not happen
            throw new IllegalStateException(e);
          }
        }
      );
      // now set keyspace keys back into pojo
      setKeyspaceKeyFields(object, row, this.keyspaceKeysByName, keyspaceKeys);
      // now take care of the columns
      decodeAndSetColumnFields(object, row);
      return object;
    } catch (IllegalAccessException|InstantiationException e) {
      throw new IllegalStateException(clazz.getName(), e);
    } catch (InvocationTargetException e) {
      final Throwable t = e.getTargetException();

      if (t instanceof Error) {
        throw (Error)t;
      } else if (t instanceof RuntimeException) {
        throw (RuntimeException)t;
      } else {
        throw new ObjectConversionException(
          clazz,
          row,
          "failed to instantiate blank POJO",
          t
        );
      }
    }
  }

  /**
   * Converts the specified UDT value into a POJO object defined by this
   * class information.
   *
   * @author paouelle
   *
   * @param  uval the UDT value to convert into a POJO
   * @return the POJO object corresponding to the given UDT value
   * @throws ObjectConversionException if unable to convert to a POJO
   */
  public T getObject(UDTValue uval) {
    throw new ObjectConversionException(
      clazz,
      uval,
      getEntityAnnotationClass().getSimpleName()
      + " POJOs cannot be retrieved from UDT values"
    );
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.info.ClassInfo#getInitialObjects(java.util.Map)
   */
  @Override
  @SuppressWarnings("unchecked")
  public Collection<T> getInitialObjects(Map<String, String> keyspaceKeys) {
    if (initials.isEmpty()) {
      return Collections.emptyList();
    }
    try {
      final List<T> objs = new ArrayList<>(16);

      for (final Method initial: initials) {
        final Object ret;

        if (keyspaceKeysByType.isEmpty()) {
          ret = initial.invoke(null);
        } else {
          ret = initial.invoke(null, keyspaceKeys);
        }
        if (ret == null) {
          return Collections.emptyList();
        }
        final Class<?> type = initial.getReturnType();

        if (type.isArray()) {
          final int l = Array.getLength(ret);

          for (int i = 0; i < l; i++) {
            objs.add(clazz.cast(Array.get(ret, i)));
          }
        } else if (ret instanceof Collection) {
          ((Collection<?>)ret).forEach(o -> objs.add(clazz.cast(o)));
        } else if (ret instanceof Stream) {
          ((Stream<?>)ret).forEach(o -> objs.add(clazz.cast(o)));
        } else if (ret instanceof Iterator) {
          for (final Iterator<?> i = (Iterator<?>)ret; i.hasNext(); ) {
            objs.add(clazz.cast(i.next()));
          }
        } else if (ret instanceof Enumeration<?>) {
          for (final Enumeration<?> e = (Enumeration<?>)ret; e.hasMoreElements(); ) {
            objs.add(clazz.cast(e.nextElement()));
          }
        } else if (ret instanceof Iterable) {
          for (final Iterator<?> i = ((Iterable<?>)ret).iterator(); i.hasNext(); ) {
            objs.add(clazz.cast(i.next()));
          }
        } else {
          objs.add(clazz.cast(ret));
        }
      }
      return objs;
    } catch (IllegalAccessException e) { // should not happen
      throw new IllegalStateException(e);
    } catch (InvocationTargetException e) {
      final Throwable t = e.getTargetException();

      if (t instanceof Error) {
        throw (Error)t;
      } else if (t instanceof RuntimeException) {
        throw (RuntimeException)t;
      } else { // we don't expect any of those
        throw new IllegalStateException(t);
      }
    }
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    return clazz.hashCode();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj instanceof ClassInfoImpl) {
      final ClassInfoImpl<?> c = (ClassInfoImpl<?>)obj;

      return clazz.equals(c.clazz);
    }
    return false;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return (
      getClass().getSimpleName()
      + "[clazz=" + clazz
      + ",keyspace=" + keyspace
      + ",columns=" + columns
      + "]"
    );
  }
}

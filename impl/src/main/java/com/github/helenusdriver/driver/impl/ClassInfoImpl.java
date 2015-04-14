/*
 * Copyright (C) 2015-2015 The Helenus Driver Project Authors.
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
package com.github.helenusdriver.driver.impl;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableObject;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.UDTValue;
import com.github.helenusdriver.commons.lang3.reflect.ReflectionUtils;
import com.github.helenusdriver.driver.ColumnPersistenceException;
import com.github.helenusdriver.driver.ObjectConversionException;
import com.github.helenusdriver.driver.StatementManager;
import com.github.helenusdriver.driver.info.ClassInfo;
import com.github.helenusdriver.driver.info.FieldInfo;
import com.github.helenusdriver.driver.info.TableInfo;
import com.github.helenusdriver.persistence.Column;
import com.github.helenusdriver.persistence.Entity;
import com.github.helenusdriver.persistence.InitialObjects;
import com.github.helenusdriver.persistence.Keyspace;
import com.github.helenusdriver.persistence.SuffixKey;
import com.github.helenusdriver.persistence.Table;
import com.github.helenusdriver.persistence.UDTEntity;

/**
 * The <code>ClassInfo</code> class provides information about a particular
 * POJO class.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 19, 2015 - paouelle, vasu - Creation
 *
 * @param <T> The type of POJO represented by this class
 *
 * @since 1.0
 */
@lombok.ToString(of={"clazz", "keyspace", "columns"})
@lombok.EqualsAndHashCode(of="clazz")
public class ClassInfoImpl<T> implements ClassInfo<T> {
  /**
   * The <code>Context</code> class provides a specific context for the POJO
   * as referenced while building a statement.
   *
   * @copyright 2015-2015 The Helenus Driver Project Authors
   *
   * @author The Helenus Driver Project Authors
   * @version 1 - Jan 19, 2015 - paouelle, vasu - Creation
   *
   * @since 1.0
   */
  public class Context implements StatementManager.Context<T> {
    /**
     * Holds the registered suffixes.
     *
     * @author paouelle
     */
    protected final Map<String, Object> suffixes = new LinkedHashMap<>(8);

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
     * @see com.github.helenusdriver.driver.StatementManager.Context#getObjectClass()
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
     * @throws IllegalArgumentException if unable to compute the keyspace name
     *         based on provided suffixes
     * @throws ExcludedSuffixKeyException if the context defines a suffix value
     *         which is marked as excluded for a given suffix key
     */
    @SuppressWarnings("synthetic-access")
    public String getKeyspace() {
      final String[] types = keyspace.suffixes();
      String name = keyspace.name();

      if (!ArrayUtils.isEmpty(types)) {
        final String[] svalues = new String[types.length];

        // let's make sure we can resolve all suffix keys
        for (int i = 0; i < types.length; i++) {
          final FieldInfoImpl<T> finfo = (FieldInfoImpl<T>)getSuffixKeyByType(types[i]);
          final SuffixKey skey = finfo.getSuffixKey();
          final String key = skey.name();
          final Object value = suffixes.get(key);

          org.apache.commons.lang3.Validate.isTrue(
            value != null, "missing suffix key '%s'", key
          );
          if (ArrayUtils.contains(skey.exclude(), value)) {
            throw new ExcludedSuffixKeyException(
              "excluded suffix key '" + key + "' value '" + value + "' for object class: "
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
      org.apache.commons.lang3.Validate.isTrue(
        !name.isEmpty(),
        "invalid empty keyspace name"
      );
      // replaces all non-alphanumeric and non underscores with underscores
      // to comply with Cassandra
      return name.replaceAll("[^a-zA-Z0-9_]", "_").toLowerCase();
    }

    /**
     * Adds a suffix to the context for use in keyspace name creation.
     *
     * @author paouelle
     *
     * @param  suffix the suffix name
     * @param  value the suffix value
     * @throws NullPointerException if <code>suffix</code> or <code>value</code>
     *         is <code>null</code>
     * @throws IllegalArgumentException if the POJO doesn't require the specified
     *         suffix or if the value doesn't match the POJO's definition for the
     *         specified suffix
     */
    public void addSuffix(String suffix, Object value) {
      validateSuffix(suffix, value);
      suffixes.put(suffix, value);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.github.helenusdriver.driver.StatementManager.Context#getObject(com.datastax.driver.core.Row)
     */
    @Override
    public T getObject(Row row) {
      return ClassInfoImpl.this.getObject(row, suffixes);
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
     * @return the initial objects to insert in the table or <code>null</code>
     *         if none needs to be inserted
     */
    @SuppressWarnings("synthetic-access")
    public T[] getInitialObjects() {
      if (getSuffixKeys().isEmpty()) {
        return ClassInfoImpl.this.getInitialObjects(null);
      }
      final Map<String, String> svalues = new LinkedHashMap<>(getSuffixKeys().size());

      for (final Map.Entry<String, FieldInfoImpl<T>> e: getSuffixTypes().entrySet()) {
        final String type = e.getKey();
        final FieldInfoImpl<T> finfo = e.getValue();
        final String key = finfo.getSuffixKeyName();
        final Object value = suffixes.get(key);

        org.apache.commons.lang3.Validate.isTrue(
          value != null,
          "missing suffix key '%s'",
          key
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
      populateSuffixes(ClassInfoImpl.this.suffixesByName);
    }

    /**
     * Populates the suffixes defined in the given map from the POJO.
     *
     * @author paouelle
     *
     * @param suffixFields the map of suffix fields from the POJO where to extract
     *        the suffix values
     */
    protected void populateSuffixes(Map<String, FieldInfoImpl<T>> suffixFields) {
      for (final Map.Entry<String, FieldInfoImpl<T>> e: suffixFields.entrySet()) {
        final String suffix = e.getKey();
        final FieldInfoImpl<T> field = e.getValue();
        final Object val = field.getValue(object);

        suffixes.put(suffix, val);
      }
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
    public Map<String, Object> getColumnValues(String tname) {
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
    public Map<String, Object> getPartitionKeyColumnValues(String tname) {
      final TableInfoImpl<T> table = (TableInfoImpl<T>)getTable(tname);

      if (table == null) { // table not defined so nothing to return
        return Collections.emptyMap();
      }
      return table.getPartitionKeyColumnValues(object);
    }

    /**
     * Retrieves all suffix and partition key columns and their values from the
     * POJO and the specified table.
     *
     * @author paouelle
     *
     * @param  tname the name of the table from which to retrieve columns
     * @return a non-<code>null</code> map of all suffix key and partition key
     *         column/value pairs for the POJO
     * @throws IllegalArgumentException if a column or a suffix is missing from
     *         the POJO
     * @throws ColumnPersistenceException if unable to persist a column's value
     */
    public Map<String, Object> getSuffixAndPartitionKeyColumnValues(String tname) {
      final TableInfoImpl<T> table = (TableInfoImpl<T>)getTable(tname);

      if (table == null) { // table not defined so return suffixes only
        return getSuffixKeyValues();
      }
      return table.getSuffixAndPartitionKeyColumnValues(object);
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
    public Map<String, Object> getPrimaryKeyColumnValues(String tname) {
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
    public Map<String, Object> getPrimaryKeyColumnValues(
      String tname, Map<String, Object> pkeys_override
    ) {
      final TableInfoImpl<T> table = (TableInfoImpl<T>)getTable(tname);

      if (table == null) { // table not defined so nothing to return
        return Collections.emptyMap();
      }
      return table.getPrimaryKeyColumnValues(object, pkeys_override);
    }

    /**
     * Retrieves all suffix keys and their values from the POJO.
     *
     * @author paouelle
     *
     * @return a non-<code>null</code> map of all suffix key name/value pairs
     *         for the POJO
     * @throws IllegalArgumentException if a suffix is missing from the POJO
     * @throws ColumnPersistenceException if unable to persist a suffix key's value
     */
    public Map<String, Object> getSuffixKeyValues() {
      final Map<String, Object> values = new LinkedHashMap<>(suffixes.size());

      for (final Map.Entry<String, FieldInfoImpl<T>> e: getSuffixKeys().entrySet()) {
        final String name = e.getKey();
        final FieldInfoImpl<T> field = e.getValue();
        final Object value = field.getValue(object);

        org.apache.commons.lang3.Validate.isTrue(
          value != null,
          "missing suffix key '%s'",
          name
        );
        values.put(name, value);
      }
      return values;
    }

    /**
     * Retrieves all suffix and primary key columns and their values from the
     * POJO and the specified table.
     *
     * @author paouelle
     *
     * @param  tname the name of the table from which to retrieve columns
     * @return a non-<code>null</code> map of all suffix key and primary key
     *         column/value pairs for the POJO
     * @throws IllegalArgumentException if a column or a suffix is missing from
     *         the POJO
     * @throws ColumnPersistenceException if unable to persist a column's value
     */
    public Map<String, Object> getSuffixAndPrimaryKeyColumnValues(String tname) {
      final TableInfoImpl<T> table = (TableInfoImpl<T>)getTable(tname);

      if (table == null) { // table not defined so return suffixes only
        return getSuffixKeyValues();
      }
      return table.getSuffixAndPrimaryKeyColumnValues(object);
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
    public Map<String, Object> getMandatoryAndPrimaryKeyColumnValues(String tname) {
      final TableInfoImpl<T> table = (TableInfoImpl<T>)getTable(tname);

      if (table == null) { // table not defined so nothing to return
        return Collections.emptyMap();
      }
      return table.getMandatoryAndPrimaryKeyColumnValues(object);
    }

    /**
     * Retrieves all non primary key columns and their values from the POJO
     * from the specified table.
     * <p>
     * <i>Note:</i> The returned values should not be encoded.
     *
     * @author paouelle
     *
     * @param  tname the name of the table from which to retrieve columns
     * @return a non-<code>null</code> map of all non primary key column/value
     *         pairs for the POJO
     * @throws IllegalArgumentException if a mandatory column is missing from
     *         the POJO
     */
    public Map<String, Object> getNonPrimaryKeyColumnValues(String tname) {
      final TableInfoImpl<T> table = (TableInfoImpl<T>)getTable(tname);

      if (table == null) { // table not defined so nothing to return
        return Collections.emptyMap();
      }
      return table.getNonPrimaryKeyColumnValues(object);
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
    public Object getColumnValue(String tname, CharSequence name) {
      final TableInfoImpl<T> table = (TableInfoImpl<T>)getTable(tname);

      if (table == null) { // table not defined so nothing to return
        return Collections.emptyMap();
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
    public Map<String, Object> getColumnValues(
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
    public Map<String, Object> getColumnValues(
      String tname, CharSequence... names
    ) {
      final TableInfoImpl<T> table = (TableInfoImpl<T>)getTable(tname);

      if (table == null) { // table not defined so nothing to return
        return Collections.emptyMap();
      }
      return table.getColumnValues(object, names);
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
   * Holds the optional initial objects factory method.
   *
   * @author paouelle
   */
  private final Method initial;

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
  private final Map<String, TableInfoImpl<T>> tables = new LinkedHashMap<>(12);

  /**
   * Holds the set of column names defined by the POJO.
   *
   * @author paouelle
   */
  private final Set<String> columns = new LinkedHashSet<>(25);

  /**
   * Holds a map of all fields annotated as suffixes keyed by the
   * suffix name.
   *
   * @author paouelle
   */
  protected final Map<String, FieldInfoImpl<T>> suffixesByName = new LinkedHashMap<>(8);

  /**
   * Holds a map of all fields annotated as suffixes keyed by the
   * suffix type.
   *
   * @author paouelle
   */
  protected final Map<String, FieldInfoImpl<T>> suffixesByType = new LinkedHashMap<>(8);

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
    this.entityAnnotationClass = entityAnnotationClass;
    this.clazz = clazz;
    this.constructor = findDefaultCtor(entityAnnotationClass);
    this.finalFields = findFinalFields();
    this.keyspace = findKeyspace();
    findTables(mgr);
    findColumns();
    findSuffixKeys();
    this.initial = findInitial();
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
   * Finds an initial objects factory method for the POJO if configured.
   *
   * @author paouelle
   *
   * @return the initial objects factory method or <code>null</code> if none
   *         configured
   * @throws IllegalArgumentException if the initial objects method is not
   *         properly defined
   */
  private Method findInitial() {
    final InitialObjects io = clazz.getAnnotation(InitialObjects.class);

    if (io != null) {
      final String mname = io.staticMethod();

      try {
        final Method m = (
          suffixesByType.isEmpty()
            ? clazz.getMethod(mname)
            : clazz.getMethod(mname, Map.class)
          );

        // validate the method is static
        if (!Modifier.isStatic(m.getModifiers())) {
          throw new IllegalArgumentException(
            "initial objects method '"
            + mname
            + "' is not static in class: "
            + clazz.getSimpleName()
          );
        }
        // validate the return type is compatible with this class and is an array
        final Class<?> type = m.getReturnType();

        if (!type.isArray()) {
          throw new IllegalArgumentException(
            "initial objects method '"
            + mname
            + "' doesn't return an array in class: "
            + clazz.getSimpleName()
          );
        }
        final Class<?> ctype = type.getComponentType();

        if (!ctype.isAssignableFrom(clazz)) {
          throw new IllegalArgumentException(
            "incompatible returned class '"
            + ctype.getName()
            + "' for initial objects method '"
            + mname
            + "' in class: "
            + clazz.getSimpleName()
          );
        }
        // validate that if suffixes are defined, the method expects a Map<String, String>
        // to provide the values for the suffixes when initializing objects
        final Class<?>[] cparms = m.getParameterTypes();

        if (suffixesByType.isEmpty()) {
          // should always be 0 as we used no classes in getMethod()
          if (cparms.length != 0) {
            throw new IllegalArgumentException(
              "expecting no parameters for initial objects method '"
              + mname
              + "' in class: "
              + clazz.getSimpleName()
            );
          }
        } else {
          // should always be 1 as we used only 1 class in getMethod()
          if (cparms.length != 1) {
            throw new IllegalArgumentException(
              "expecting one Map<String, String> parameter for initial objects method '"
              + mname
              + "' in class: "
              + clazz.getSimpleName()
            );
          }
          // should always be a map as we used a Map to find the method
          if (!Map.class.isAssignableFrom(cparms[0])) {
            throw new IllegalArgumentException(
              "expecting parameter for initial objects method '"
              + mname
              + "' to be of type Map<String, String> in class: "
              + clazz.getSimpleName()
            );
          }
          final Type[] tparms = m.getGenericParameterTypes();

          // should always be 1 as we used only 1 class in getMethod()
          if (tparms.length != 1) { // should always be 1 as it was already tested above
            throw new IllegalArgumentException(
              "expecting one Map<String, String> parameter for initial objects method '"
              + mname
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
                  + mname
                  + "' in class: "
                  + clazz.getSimpleName()
                );
              }
            }
          } else {
            throw new IllegalArgumentException(
              "expecting a Map<String, String> parameter for initial objects method '"
              + mname
              + "' in class: "
              + clazz.getSimpleName()
            );
          }
        }
        return m;
      } catch (NoSuchMethodException e) {
        throw new IllegalArgumentException(
          "missing initial objects method '"
          + mname
          + "' in class: "
          + clazz.getSimpleName(),
          e
        );
      }
    }
    return null;
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
    // make sure that a keyspace name or suffix exist
    org.apache.commons.lang3.Validate.isTrue(
      !keyspace.name().isEmpty() || (keyspace.suffixes().length != 0),
      "@Keyspace annotation for %s must defined at least one of 'name' or 'suffixes'",
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
   * @throws IllegalArgumentException if the POJO class is improperly annotated
   */
  private void findTables(StatementManagerImpl mgr) {
    final Map<String, Table> tables = ReflectionUtils.getAnnotationsByType(
      String.class, Table.class, clazz
    );

    if (this instanceof UDTClassInfoImpl) {
      org.apache.commons.lang3.Validate.isTrue(
        tables.isEmpty(),
        "%s is annotated with @Table",
        clazz.getSimpleName()
      );
      // create a dummy table info
      final UDTEntity ue = clazz.getAnnotation(UDTEntity.class);

      org.apache.commons.lang3.Validate.isTrue(
        ue != null,
        "class '%s' is not annotated with @UDTEntity", clazz.getSimpleName()
      );
      this.tables.put(ue.name(), new TableInfoImpl<>(mgr, this, ue.name()));
    } else {
      org.apache.commons.lang3.Validate.isTrue(
        !tables.isEmpty(),
        "%s is not annotated with @Table",
        clazz.getSimpleName()
      );
      for (final Table table: tables.values()) {
        final TableInfoImpl<T> tinfo = new TableInfoImpl<>(mgr, this, table);

        this.tables.put(tinfo.getName(), tinfo);
      }
    }
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
   * Finds and records all fields annotated as keyspace suffixes.
   *
   * @author paouelle
   *
   * @throws IllegalArgumentException if unable to find a getter or setter
   *         method for the field of if improperly annotated
   */
  private void findSuffixKeys() {
    final Set<String> ksuffixes = new HashSet<>(keyspace.suffixes().length * 3 / 2);

    for (final String s: keyspace.suffixes()) {
      ksuffixes.add(s);
    }
    // check suffix keys defined on the class itself
    final SuffixKey[] suffixes = clazz.getAnnotationsByType(SuffixKey.class);

    if (this instanceof UDTClassInfoImpl) {
      for (final SuffixKey suffix: suffixes) {
        org.apache.commons.lang3.Validate.isTrue(
          !suffixesByType.containsKey(suffix.type()),
          "multipe @SuffixKey annotations found with type '%s' for class: %s",
          suffix.type(),
          clazz.getSimpleName()
        );
        org.apache.commons.lang3.Validate.isTrue(
          !suffixesByName.containsKey(suffix.name()),
          "multipe @SuffixKey annotations found with name '%s' for class: %s",
          suffix.name(),
          clazz.getSimpleName()
        );
        org.apache.commons.lang3.Validate.isTrue(
          ksuffixes.remove(suffix.type()),
          "@Keyspace annotation does not define suffix type '%s' for class: %s",
          suffix.type(),
          clazz.getSimpleName()
        );
        final FieldInfoImpl<T> field = new FieldInfoImpl<>(this, suffix);

        suffixesByName.put(suffix.name(), field);
        suffixesByType.put(suffix.type(), field);
      }
    } else {
      org.apache.commons.lang3.Validate.isTrue(
        suffixes.length == 0,
        "%s POJOs do not support @SuffixKey annotations on the type; define a field instead",
        getEntityAnnotationClass().getSimpleName()
      );
      // make sure to walk up the class hierarchy
      for (final Field f: ReflectionUtils.getAllFieldsAnnotatedWith(
        clazz, SuffixKey.class, true
      )) {
        final FieldInfoImpl<T> field = new FieldInfoImpl<>(this, f);
        final SuffixKey suffix = field.getSuffixKey();

        org.apache.commons.lang3.Validate.isTrue(
          !suffixesByType.containsKey(suffix.type()),
          "multipe @SuffixKey annotations found with type '%s' for class: %s",
          suffix.type(),
          clazz.getSimpleName()
        );
        org.apache.commons.lang3.Validate.isTrue(
          !suffixesByName.containsKey(suffix.name()),
          "multipe @SuffixKey annotations found with name '%s' for class: %s",
          suffix.name(),
          clazz.getSimpleName()
        );
        org.apache.commons.lang3.Validate.isTrue(
          ksuffixes.remove(suffix.type()),
          "@Keyspace annotation does not define suffix type '%s' for class: %s",
          suffix.type(),
          clazz.getSimpleName()
        );
        suffixesByName.put(suffix.name(), field);
        suffixesByType.put(suffix.type(), field);
      }
    }
    org.apache.commons.lang3.Validate.isTrue(
      ksuffixes.isEmpty(),
      "missing @SuffixKey annotations for @Keyspace defined suffixes: %s for class: %s",
      StringUtils.join(ksuffixes, ", "),
      clazz.getSimpleName()
    );
  }

  /**
   * Sets the specified suffix fields in the POJO object.
   *
   * @author paouelle
   *
   * @param  object the non-<code>null</code> POJO object
   * @param  row the non-<code>null</code> row being decoded to the POJO
   * @param  map a non-<code>null</code> map of suffixes to fields to use
   * @param  values the suffix values defined by the statement used when issuing
   *         the query
   * @throws ObjectConversionException if unable to set suffixes in the POJO
   */
  private void setSuffixFields(
    T object,
    Row row,
    Map<String, FieldInfoImpl<T>> map,
    Map<String, Object> values
  ) {
    for (final Map.Entry<String, Object> e: values.entrySet()) {
      final String suffix = e.getKey();
      final Object value = e.getValue();
      final FieldInfoImpl<T> field = map.get(suffix);

      if (field != null) {
        try {
          field.setValue(object, value);
        } catch (IllegalArgumentException iae) {
          throw new ObjectConversionException(
            clazz,
            row,
            "unable to set '" + suffix + "' suffix in object",
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
        final FieldInfoImpl<T> field = table.getColumn(coldef.getName());

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
   * @see com.github.helenusdriver.driver.info.ClassInfo#supportsTablesAndIndexes()
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
   * @return the non-<code>null</code> keysapce annotation for this POJO
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
   * @see com.github.helenusdriver.driver.info.ClassInfo#getSuffixKey(java.lang.String)
   */
  @Override
  public FieldInfo<T> getSuffixKey(String name) {
    return suffixesByName.get(name);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.info.ClassInfo#getSuffixKeyByType(java.lang.String)
   */
  @Override
  public FieldInfo<T> getSuffixKeyByType(String type) {
    return suffixesByType.get(type);
  }

  /**
   * Checks if the specified name is defined as a suffix key.
   *
   * @author paouelle
   *
   * @param  name the name of the suffix key
   * @return <code>true</code> if that name is defined as a suffix key;
   *         <code>false</code> otherwise
   */
  public boolean isSuffixKey(String name) {
    final FieldInfoImpl<T> field = suffixesByName.get(name);

    return (field != null) ? field.isSuffixKey() : false;
  }

  /**
   * Gets the field info for all suffix keys defined by this POJO.
   *
   * @author paouelle
   *
   * @return the non-<code>null</code> map of all suffix key fields keyed by
   *         their names
   */
  public Map<String, FieldInfoImpl<T>> getSuffixKeys() {
    return suffixesByName;
  }

  /**
   * Gets the field info for all suffix types defined by this POJO.
   *
   * @author paouelle
   *
   * @return the non-<code>null</code> map of all suffix type fields keyed by
   *         their types
   */
  public Map<String, FieldInfoImpl<T>> getSuffixTypes() {
    return suffixesByType;
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
   * @see com.github.helenusdriver.driver.info.ClassInfo#getTable(java.lang.String)
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
   * @see com.github.helenusdriver.driver.info.ClassInfo#getNumTables()
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
   * @see com.github.helenusdriver.driver.info.ClassInfo#getTables()
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
   * @see com.github.helenusdriver.driver.info.ClassInfo#tables()
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
   * @see com.github.helenusdriver.driver.info.ClassInfo#iterator()
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
   * defined as a suffix key.
   *
   * @author paouelle
   *
   * @param  name the column name or suffix key to validate
   * @throws NullPointerException if <code>name</code> is <code>null</code>
   * @throws IllegalArgumentException if the specified column or suffix key is
   *         not defined by the POJO
   */
  public void validateColumnOrSuffix(String name) {
    org.apache.commons.lang3.Validate.notNull(name, "invalid null column name or suffix key");
    org.apache.commons.lang3.Validate.isTrue(
      columns.contains(name) || suffixesByName.containsKey(name),
      "%s doesn't define column or suffix key '%s'",
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
   * Validates the specified suffix and its value as being a valid suffix for
   * the POJO.
   *
   * @author paouelle
   *
   * @param  suffix the suffix to validate
   * @param  value the value for the suffix
   * @throws IllegalArgumentException if the suffix is not defined by this POJO
   *         if the specified value is not of the right type or is
   *         <code>null</code> when the field is mandatory
   */
  public void validateSuffix(String suffix, Object value) {
    org.apache.commons.lang3.Validate.notNull(suffix, "invalid null suffix");
    org.apache.commons.lang3.Validate.notNull(value, "invalid null value");
    final FieldInfoImpl<T> field = suffixesByName.get(suffix);

    org.apache.commons.lang3.Validate.isTrue(
      field != null,
      "%s doesn't define keyspace suffix: %s",
      clazz.getSimpleName(),
      suffix
    );
    field.validateValue(value);
  }

  /**
   * Converts the specified result row into a POJO object defined by this
   * class information and suffix map.
   *
   * @author paouelle
   *
   * @param  row the result row to convert into a POJO
   * @param  suffixes a map of suffix values to report back into the created
   *         POJO
   * @return the POJO object corresponding to the given result row
   * @throws NullPointerException if <code>suffixes</code> is <code>null</code>
   * @throws ObjectConversionException if unable to convert to a POJO
   */
  public T getObject(Row row, Map<String, Object> suffixes) {
    if (row == null) {
      return null;
    }
    org.apache.commons.lang3.Validate.notNull(suffixes, "invalid null suffixes");
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
      // now set suffixes back into pojo
      setSuffixFields(object, row, this.suffixesByName, suffixes);
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
   * @see com.github.helenusdriver.driver.info.ClassInfo#getInitialObjects(java.util.Map)
   */
  @Override
  @SuppressWarnings("unchecked")
  public T[] getInitialObjects(Map<String, String> suffixes) {
    if (initial == null) {
      return null;
    }
    try {
      if (suffixesByType.isEmpty()) {
        return (T[])initial.invoke(null);
      }
      return (T[])initial.invoke(null, suffixes);
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
}

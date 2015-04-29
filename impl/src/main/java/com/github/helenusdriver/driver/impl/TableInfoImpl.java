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

import java.lang.reflect.Field;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.commons.lang3.tuple.Pair;

import com.github.helenusdriver.commons.lang3.reflect.ReflectionUtils;
import com.github.helenusdriver.driver.ColumnPersistenceException;
import com.github.helenusdriver.driver.info.ClassInfo;
import com.github.helenusdriver.driver.info.FieldInfo;
import com.github.helenusdriver.driver.info.TableInfo;
import com.github.helenusdriver.persistence.Column;
import com.github.helenusdriver.persistence.DataType;
import com.github.helenusdriver.persistence.Table;

/**
 * The <code>TableInfo</code> class caches all the table and its field
 * information needed by the class {@link ClassInfoImpl}.
 * <p>
 * <i>Note:</i> A fake instance of this class will be created with no table
 * annotations for user-defined type entities. By design, the
 * {@link FieldInfoImpl} class will not allow any type of keys but only columns.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 19, 2015 - paouelle - Creation
 *
 * @param <T> The type of POJO represented by this class
 *
 * @since 1.0
 */
@lombok.ToString(exclude={"cinfo"})
public class TableInfoImpl<T> implements TableInfo<T> {
  /**
   * Cleanups the specified table name according to Cassandra's guidelines.
   *
   * @author paouelle
   *
   * @param  name the table name to clean up
   * @return the corresponding cleaned up table name
   */
  static final String cleanName(String name) {
    // replaces all non-alphanumeric or underscores with underscores
    // to comply with Cassandra
    return name.replaceAll("[^a-zA-Z0-9_]", "_").toLowerCase();
  }

  /**
   * Holds the class for the POJO.
   *
   * @author vasu
   */
  private final Class<T> clazz;

  /**
   * Holds the class info for the POJO.
   *
   * @author paouelle
   */
  private final ClassInfoImpl<T> cinfo;

  /**
   * Holds the table annotation.
   *
   * @author vasu
   */
  private final Table table;

  /**
   * Holds the cleaned up table name.
   *
   * @author paouelle
   */
  private final String name;

  /**
   * Holds a map of all fields annotated as columns keyed by the field name and
   * its declaring class.
   *
   * @author vasu
   */
  private final Map<Pair<String, Class<?>>, FieldInfoImpl<T>> fields
    = new LinkedHashMap<>(25);

  /**
   * Holds a map of all fields annotated as columns keyed by the column name.
   *
   * @author vasu
   */
  private final Map<String, FieldInfoImpl<T>> columns = new LinkedHashMap<>(25);

  /**
   * Holds a map of all primary key column fields keyed by the column name.
   *
   * @author paouelle
   */
  private final Map<String, FieldInfoImpl<T>> primaryKeyColumns
    = new LinkedHashMap<>(12);

  /**
   * Holds a map of all index column fields keyed by the column name.
   *
   * @author paouelle
   */
  private final Map<String, FieldInfoImpl<T>> indexColumns
    = new LinkedHashMap<>(12);

  /**
   * Holds a map of all partition key column fields keyed by the column name.
   *
   * @author paouelle
   */
  private final Map<String, FieldInfoImpl<T>> partitionKeyColumns
    = new LinkedHashMap<>(12);

  /**
   * Holds a map of all clustering key column fields keyed by the column name.
   *
   * @author paouelle
   */
  private final Map<String, FieldInfoImpl<T>> clusteringKeyColumns
    = new LinkedHashMap<>(12);

  /**
   * Holds the key column field.
   *
   * @author paouelle
   */
  private final MutableObject<FieldInfoImpl<T>> typeKeyColumn
    = new MutableObject<>();

  /**
   * Holds a map of all final primary key values keyed by the column
   * name.
   *
   * @author paouelle
   */
  private final Map<String, Object> finalPrimaryKeyValues
    = new LinkedHashMap<>(12);

  /**
   * Holds a map of all mandatory, type key, and primary key column fields keyed
   * by the column name.
   *
   * @author paouelle
   */
  private final Map<String, FieldInfoImpl<T>> mandatoryAndPrimaryKeyColumns
    = new LinkedHashMap<>(12);

  /**
   * Holds a map of all non primary key column fields keyed by the
   * column name.
   *
   * @author paouelle
   */
  private final Map<String, FieldInfoImpl<T>> nonPrimaryKeyColumns
    = new LinkedHashMap<>(12);

  /**
   * Holds the fields which are defined as multi-keys for the table (may be
   * empty if none defined).
   *
   * @author paouelle
   */
  private final Map<String, FieldInfoImpl<T>> multiKeyColumns
    = new LinkedHashMap<>(2);

  /**
   * Holds the fields which are defined as multi-keys for the table (may be
   * empty if none defined).
   *
   * @author paouelle
   */
  private final List<FieldInfoImpl<T>> multiKeyColumnsList
    = new ArrayList<>(2);

  /**
   * Flag indicating if at least one column is defined as a collection.
   *
   * @author paouelle
   */
  private boolean hasCollectionColumns = false;

  /**
   * Instantiates a new <code>TableInfo</code> object.
   *
   * @author paouelle
   *
   * @param  mgr the non-<code>null</code> statement manager
   * @param  cinfo the non-<code>null</code> class info for the POJO
   * @param  table the table annotation or <code>null</code> if there is no table
   * @throws IllegalArgumentException if unable to find getter or setter
   *         methods for fields of if improperly annotated
   */
  public TableInfoImpl(
    StatementManagerImpl mgr, ClassInfoImpl<T> cinfo, Table table
  ) {
    this.clazz = cinfo.getObjectClass();
    this.cinfo = cinfo;
    this.table = table;
    this.name = (table != null) ? TableInfoImpl.cleanName(table.name()) : null;
    findColumnFields(mgr);
  }

  /**
   * Instantiates a new <code>TableInfo</code> object.
   *
   * @author paouelle
   *
   * @param  mgr the non-<code>null</code> statement manager
   * @param  cinfo the non-<code>null</code> class info for the POJO
   * @param  name the non-<code>null</code> table name
   * @throws IllegalArgumentException if unable to find getter or setter
   *         methods for fields of if improperly annotated
   */
  public TableInfoImpl(
    StatementManagerImpl mgr, ClassInfoImpl<T> cinfo, String name
  ) {
    this.clazz = cinfo.getObjectClass();
    this.cinfo = cinfo;
    this.table = null;
    this.name = name;
    findColumnFields(mgr);
  }

  /**
   * Finds and record all fields annotated as columns for this table.
   *
   * @author paouelle
   *
   * @param  mgr the non-<code>null</code> statement manager
   * @throws IllegalArgumentException if unable to find a getter or setter
   *         method for the field of if improperly annotated
   */
  private void findColumnFields(StatementManagerImpl mgr) {
    // make sure to walk up the class hierarchy
    FieldInfoImpl<T> lastPartitionKey = null;
    FieldInfoImpl<T> lastClusteringKey = null;

    for (final Field f: ReflectionUtils.getAllFieldsAnnotatedWith(
      clazz, Column.class, true
    )) {
      final Pair<String, Class<?>> pf = Pair.of(f.getName(), f.getDeclaringClass());
      FieldInfoImpl<T> field = fields.get(pf);

      if (field == null) {
        field = new FieldInfoImpl<>(this, f);
        if (!field.isColumn()) {
          // not annotated as a column for this table so skip it
          continue;
        }
        fields.put(pf, field);
      }
      final FieldInfoImpl<T> oldc = columns.put(field.getColumnName(), field);

      if (oldc != null) {
        throw new IllegalArgumentException(
          clazz.getSimpleName()
          + " cannot annotate more than one field with the same column name '"
          + field.getColumnName()
          + ((table != null) ? "' for table '" + table.name() : "")
          + "': found '"
          + oldc.getDeclaringClass().getSimpleName()
          + "."
          + oldc.getName()
          + "' and '"
          + field.getDeclaringClass().getSimpleName()
          + "."
          + field.getName()
          + "'"
        );
      }
      if (field.getDataType().isCollection()) {
        this.hasCollectionColumns = true;
      }
      if (field.isTypeKey()) { // by design will be false if no table is defined
        final FieldInfoImpl<T> oldk = typeKeyColumn.getValue();

        if (oldk != null) {
          throw new IllegalArgumentException(
            clazz.getSimpleName()
            + " cannot annotate more than one field as a type key for table '"
            + table.name()
            + "': found '"
            + oldk.getDeclaringClass().getSimpleName()
            + "."
            + oldk.getName()
            + "' and '"
            + field.getDeclaringClass().getSimpleName()
            + "."
            + field.getName()
            + "'"
          );
        }
        typeKeyColumn.setValue(field);
        mandatoryAndPrimaryKeyColumns.put(field.getColumnName(), field);
        if (field.isIndex()) {
          indexColumns.put(field.getColumnName(), field);
        }
      }
      if (field.isPartitionKey()) {
        lastPartitionKey = field;
        mandatoryAndPrimaryKeyColumns.put(field.getColumnName(), field);
        primaryKeyColumns.put(field.getColumnName(), field);
        partitionKeyColumns.put(field.getColumnName(), field);
        if (field.isFinal()) {
          finalPrimaryKeyValues.put(field.getColumnName(), field.getFinalValue());
        }
        if (field.isMultiKey()) {
          multiKeyColumns.put(field.getColumnName(), field);
          multiKeyColumnsList.add(field);
        }
      } else if (field.isClusteringKey()) {
        lastClusteringKey = field;
        mandatoryAndPrimaryKeyColumns.put(field.getColumnName(), field);
        primaryKeyColumns.put(field.getColumnName(), field);
        clusteringKeyColumns.put(field.getColumnName(), field);
        if (field.isFinal()) {
          finalPrimaryKeyValues.put(field.getColumnName(), field.getFinalValue());
        }
        if (field.isMultiKey()) {
          multiKeyColumns.put(field.getColumnName(), field);
          multiKeyColumnsList.add(field);
        }
      } else if (!field.isTypeKey()) {
        if (field.isIndex()) {
          indexColumns.put(field.getColumnName(), field);
        }
        if (field.isMandatory()) {
          mandatoryAndPrimaryKeyColumns.put(field.getColumnName(), field);
          nonPrimaryKeyColumns.put(field.getColumnName(), field);
        } else {
          nonPrimaryKeyColumns.put(field.getColumnName(), field);
        }
      }
    }
    if (table != null) {
      org.apache.commons.lang3.Validate.isTrue(
        !partitionKeyColumns.isEmpty(),
        "pojo '%s' must annotate one field as a partition primary key for table '%s'",
        clazz.getSimpleName(),
        table.name()
      );
    }
    // filters out columns if need be
    mgr.filter(this);
    // finalize table keys
    reorderPrimaryKeys();
    if (lastPartitionKey != null) {
      lastPartitionKey.setLast();
    }
    if (lastClusteringKey != null) {
      lastClusteringKey.setLast();
    }
  }

  /**
   * Re-order primary keys based on @Table annotation specifications.
   *
   * @author paouelle
   */
  private void reorderPrimaryKeys() {
    if ((table == null)
        || ((table.partition().length == 0) && (table.clustering().length == 0))) {
      return; // nothing to do so keep original order
    }
    // clone keys map so we can modify the original ones
    final Map<String, FieldInfoImpl<T>> partition = new LinkedHashMap<>(partitionKeyColumns);
    final Map<String, FieldInfoImpl<T>> clustering = new LinkedHashMap<>(clusteringKeyColumns);

    primaryKeyColumns.clear();
    partitionKeyColumns.clear();
    clusteringKeyColumns.clear();
    // start with partition keys specified in @Table
    for (final String columnName: table.partition()) {
      final FieldInfoImpl<T> field = partition.remove(columnName);

      org.apache.commons.lang3.Validate.isTrue(
        field != null,
        "@Table partition key '%s' not found in pojo '%s' for table '%s'",
        columnName,
        clazz.getSimpleName(),
        table.name()
      );
      primaryKeyColumns.put(columnName, field);
      partitionKeyColumns.put(columnName, field);
    }
    // add remaining partition keys
    primaryKeyColumns.putAll(partition);
    partitionKeyColumns.putAll(partition);
    // now deal with clustering keys specified in @Table
    for (final String columnName: table.clustering()) {
      final FieldInfoImpl<T> field = clustering.remove(columnName);

      org.apache.commons.lang3.Validate.isTrue(
        field != null,
        "@Table clustering key '%s' not found in pojo '%s' for table '%s'",
        columnName,
        clazz.getSimpleName(),
        table.name()
      );
      primaryKeyColumns.put(columnName, field);
      clusteringKeyColumns.put(columnName, field);
    }
    // add remaining clustering keys
    primaryKeyColumns.putAll(clustering);
    clusteringKeyColumns.putAll(clustering);
  }

  /**
   * Validates if a column is defined by the POJO in this table.
   *
   * @author paouelle
   *
   * @param  name the column name to validate
   * @param  ignoreNonColumnNames <code>true</code> to not generate an exception if
   *         the specified name cannot be interpreted as a column name;
   *         <code>false</code> to generate an exception
   * @throws NullPointerException if <code>name</code> is <code>null</code>
   * @throws IllegalArgumentException if the specified column is not defined
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
      if (table == null) {
        throw new IllegalArgumentException(
          "unexpected column name '"
          + name
          + "' for pojo '"
          + clazz.getSimpleName()
          + "'"
        );
      }
      throw new IllegalArgumentException(
        "unexpected column name '"
        + name
        + "' in table '"
        + table.name()
        + "' for udt '"
        + clazz.getSimpleName()
        + "'"
      );
    }
    if (table != null) {
      org.apache.commons.lang3.Validate.isTrue(
        columns.containsKey(n),
        "pojo '%s' doesn't define column '%s' in table '%s'",
        clazz.getSimpleName(),
        n,
        table.name()
      );
    } else {
      org.apache.commons.lang3.Validate.isTrue(
        columns.containsKey(n),
        "udt '%s' doesn't define column '%s'",
        clazz.getSimpleName(),
        n,
        table.name()
      );
    }
  }

  /**
   * Adds the specified non-primary column field to this table.
   * <p>
   * This method will be called by the root entity when it is adding non-primary
   * column fields defined only in its type POJO classes to complement its
   * schema definition.
   * <p>
   * <i>Note:</i> The provided column will not be added to this table if a
   * compatible column with the same name already exist in this table.
   *
   * @author paouelle
   *
   * @param  col the non-<code>null</code> non-primary column field to add to
   *         this table
   * @throws IllegalArgumentException if the column being added is not compatible
   *         to one already defined with the same column name in this table
   */
  void addNonPrimaryColumn(FieldInfoImpl<? extends T> col) {
    org.apache.commons.lang3.Validate.isTrue(
      cinfo instanceof RootClassInfoImpl,
      "should not have been called for class '%s'", cinfo.getClass().getName()
    );
    final FieldInfoImpl<T> old = columns.get(col.getColumnName());

    if (old != null) {
      // already defined so skip it but not before making sure it is compatible
      if (old.getDeclaringClass().equals(col.getDeclaringClass())) {
        // same exact field so bail!!!
        return;
      }
      // check data type
      org.apache.commons.lang3.Validate.isTrue(
        old.getDataType().getType() == col.getDataType().getType(),
        "incompatible type columns '%s.%s' of type '%s' and '%s.%s' of type '%s' in table '%s' in pojo '%s'",
        old.getDeclaringClass().getSimpleName(),
        old.getName(),
        old.getDataType().getType(),
        col.getDeclaringClass().getSimpleName(),
        col.getName(),
        col.getDataType().getType(),
        name,
        clazz.getSimpleName()
      );
      return;
    }
    final FieldInfoImpl<T> rcol
      = new FieldInfoImpl<>((RootClassInfoImpl<T>)cinfo, this, col);

    fields.put(Pair.of(rcol.getName(), rcol.getDeclaringClass()), rcol);
    columns.put(rcol.getColumnName(), rcol);
    if (rcol.isIndex()) {
      indexColumns.put(rcol.getColumnName(), rcol);
    }
    if (rcol.isMandatory()) {
      mandatoryAndPrimaryKeyColumns.put(rcol.getColumnName(), rcol);
      nonPrimaryKeyColumns.put(rcol.getColumnName(), rcol);
    } else {
      nonPrimaryKeyColumns.put(rcol.getColumnName(), rcol);
    }
  }

  /**
   * Retrieves all columns and their values from the given POJO .
   *
   * @author paouelle
   *
   * @param  object the non-<code>null</code> POJO object
   * @return a non-<code>null</code> map of all column/value pairs for the POJO
   * @throws IllegalArgumentException if a mandatory column is missing from the POJO
   * @throws ColumnPersistenceException if unable to persist a column's value
   */
  Map<String, Object> getColumnValues(T object) {
    final Map<String, Object> values = new LinkedHashMap<>(columns.size());

    for (final Map.Entry<String, FieldInfoImpl<T>> e: columns.entrySet()) {
      final String name = e.getKey();
      final FieldInfoImpl<T> field = e.getValue();
      final Object value = field.getValue(object);

      if (value == null) {
        if (table != null) {
          org.apache.commons.lang3.Validate.isTrue(
            !field.isMandatory(),
            "missing mandatory column '%s' from table '%s' in pojo '%s'",
            name, table.name(), clazz.getSimpleName()
          );
          if (field.isPartitionKey() || field.isClusteringKey()) {
            if (field.isOptional()) {
              throw new EmptyOptionalPrimaryKeyException(
                "missing primary key column '"
                + name
                + "' in table '"
                + table.name()
                + "' in pojo '"
                + clazz.getSimpleName()
                + "'"
              );
            }
            throw new IllegalArgumentException(
              "missing primary key column '"
              + name
              + "' in table '"
              + table.name()
              + "' in pojo '"
              + clazz.getSimpleName()
              + "'"
            );
          }
          org.apache.commons.lang3.Validate.isTrue(
            !field.isTypeKey(),
            "missing type key column '%s' from table '%s' in pojo '%s'",
            name, table.name(), clazz.getSimpleName()
          );
        } else {
          org.apache.commons.lang3.Validate.isTrue(
            !field.isMandatory(),
            "missing mandatory column '%s' in udt '%s'",
            name, clazz.getSimpleName()
          );
        }
      }
      values.put(name, value);
    }
    return values;
  }

  /**
   * Retrieves all partition key columns and their values from the POJO.
   *
   * @author paouelle
   *
   * @param  object the non-<code>null</code> POJO object
   * @return a non-<code>null</code> map of all partition key column/value pairs
   *         for the POJO
   * @throws IllegalArgumentException if a column is missing from the POJO
   * @throws ColumnPersistenceException if unable to persist a column's value
   */
  Map<String, Object> getPartitionKeyColumnValues(T object) {
    if (table == null) {
      return Collections.emptyMap();
    }
    final Map<String, Object> values = new LinkedHashMap<>(primaryKeyColumns.size());

    for (final Map.Entry<String, FieldInfoImpl<T>> e: partitionKeyColumns.entrySet()) {
      final String name = e.getKey();
      final FieldInfoImpl<T> field = e.getValue();
      final Object value = field.getValue(object);

      org.apache.commons.lang3.Validate.isTrue(
        value != null,
        "missing partition key column '%s' from table '%s' for pojop '%s'",
        name,
        table.name(),
        clazz.getSimpleName()
      );
      values.put(name, value);
    }
    return values;
  }

  /**
   * Retrieves all final primary keys and their values.
   *
   * @author paouelle
   *
   * @return a non-<code>null</code> map of all final primary key column/value
   *         pairs
   */
  Map<String, Object> getFinalPrimaryKeyValues() {
    return finalPrimaryKeyValues;
  }

  /**
   * Retrieves all suffix and partition key columns and their values from the POJO.
   *
   * @author paouelle
   *
   * @param  object the non-<code>null</code> POJO object
   * @return a non-<code>null</code> map of all suffix key and partition key
   *         column/value pairs for the POJO
   * @throws IllegalArgumentException if a column or a suffix key is missing
   *         from the POJO
   * @throws ColumnPersistenceException if unable to persist a column's value
   */
  Map<String, Object> getSuffixAndPartitionKeyColumnValues(T object) {
    if (table == null) {
      return Collections.emptyMap();
    }
    final Map<String, FieldInfoImpl<T>> skeys = cinfo.getSuffixKeys();
    final Map<String, FieldInfoImpl<T>> keys = new LinkedHashMap<>(
      primaryKeyColumns.size() + skeys.size()
    );

    // start with suffix keys
    keys.putAll(skeys);
    // now add partition keys (overriding suffixes if names clashes!!!)
    keys.putAll(partitionKeyColumns);
    final Map<String, Object> values = new LinkedHashMap<>(keys.size());

    for (final Map.Entry<String, FieldInfoImpl<T>> e: keys.entrySet()) {
      final String name = e.getKey();
      final FieldInfoImpl<T> field = e.getValue();
      final Object value = field.getValue(object);

      org.apache.commons.lang3.Validate.isTrue(
        value != null,
        "missing suffix or partition key column '%s' from table '%s' for pojo '%s'",
        name,
        table.name(),
        clazz.getSimpleName()
      );
      values.put(name, value);
    }
    return values;
  }

  /**
   * Retrieves all primary key columns and their values from the POJO.
   *
   * @author paouelle
   *
   * @param  object the non-<code>null</code> POJO object
   * @return a non-<code>null</code> map of all primary key column/value pairs
   *         for the POJO
   * @throws IllegalArgumentException if a column is missing from the POJO
   * @throws ColumnPersistenceException if unable to persist a column's value
   */
  Map<String, Object> getPrimaryKeyColumnValues(T object) {
    if (table == null) {
      return Collections.emptyMap();
    }
    final Map<String, Object> values = new LinkedHashMap<>(primaryKeyColumns.size());

    for (final Map.Entry<String, FieldInfoImpl<T>> e: primaryKeyColumns.entrySet()) {
      final String name = e.getKey();
      final FieldInfoImpl<T> field = e.getValue();
      final Object value = field.getValue(object);

      if (value == null) {
        if (field.isOptional()) {
          throw new EmptyOptionalPrimaryKeyException(
            "missing primary key column '"
            + name
            + "' in table '"
            + table.name()
            + "' for pojo '"
            + clazz.getSimpleName()
            + "'"
          );
        }
        throw new IllegalArgumentException(
          "missing primary key column '"
          + name
          + "' in table '"
          + table.name()
          + "' for pojo '"
          + clazz.getSimpleName()
          + "'"
        );
      }
      values.put(name, value);
    }
    return values;
  }

  /**
   * Retrieves all primary key columns and their values from the POJO while
   * giving priority to values provided by the specified override map.
   *
   * @author paouelle
   *
   * @param  object the non-<code>null</code> POJO object
   * @param  pkeys_override a non-<code>null</code> map of primary key values
   *         to use instead of those provided by the object
   * @return a non-<code>null</code> map of all primary key column/value pairs
   *         for the POJO
   * @throws IllegalArgumentException if a column is missing from the POJO
   * @throws ColumnPersistenceException if unable to persist a column's value
   */
  Map<String, Object> getPrimaryKeyColumnValues(
    T object, Map<String, Object> pkeys_override
  ) {
    if (table == null) {
      return Collections.emptyMap();
    }
    final Map<String, Object> values = new LinkedHashMap<>(primaryKeyColumns.size());

    for (final Map.Entry<String, FieldInfoImpl<T>> e: primaryKeyColumns.entrySet()) {
      final String name = e.getKey();
      final FieldInfoImpl<T> field = e.getValue();
      Object value = pkeys_override.getOrDefault(name, this);

      if (value == this) { // special case to detect that no override was provided
        value = field.getValue(object); // so get it from the object
      }
      if (value == null) {
        if (field.isOptional()) {
          throw new EmptyOptionalPrimaryKeyException(
            "missing primary key column '"
            + name
            + "' in table '"
            + table.name()
            + "' for pojo '"
            + clazz.getSimpleName()
            + "'"
          );
        }
        throw new IllegalArgumentException(
          "missing primary key column '"
          + name
          + "' in table '"
          + table.name()
          + "' for pojo '"
          + clazz.getSimpleName()
          + "'"
        );
      }
      values.put(name, value);
    }
    return values;
  }

  /**
   * Retrieves all suffix and primary key columns and their values from the POJO.
   *
   * @author paouelle
   *
   * @param  object the non-<code>null</code> POJO object
   * @return a non-<code>null</code> map of all suffix key and primary key
   *         column/value pairs for the POJO
   * @throws IllegalArgumentException if a column or a suffix key is missing
   *         from the POJO
   * @throws ColumnPersistenceException if unable to persist a column's value
   */
  Map<String, Object> getSuffixAndPrimaryKeyColumnValues(T object) {
    if (table == null) {
      return Collections.emptyMap();
    }
    final Map<String, FieldInfoImpl<T>> skeys = cinfo.getSuffixKeys();
    final Map<String, FieldInfoImpl<T>> keys = new LinkedHashMap<>(
      primaryKeyColumns.size() + skeys.size()
    );

    // start with suffix keys
    keys.putAll(skeys);
    // now add primary keys (overriding suffixes if names clashes!!!)
    keys.putAll(primaryKeyColumns);
    final Map<String, Object> values = new LinkedHashMap<>(keys.size());

    for (final Map.Entry<String, FieldInfoImpl<T>> e: keys.entrySet()) {
      final String name = e.getKey();
      final FieldInfoImpl<T> field = e.getValue();
      final Object value = field.getValue(object);

      org.apache.commons.lang3.Validate.isTrue(
        value != null,
        "missing suffix or primary key column '%s' from table '%s' for pojo '%s'; null value",
        name,
        table.name(),
        clazz.getSimpleName()
      );
      values.put(name, value);
    }
    return values;
  }

  /**
   * Retrieves all mandatory and primary key columns and their
   * values from the POJO.
   *
   * @author paouelle
   *
   * @param  object the non-<code>null</code> POJO object
   * @return a non-<code>null</code> map of all mandatory and primary key
   *         column/value pairs for the POJO
   * @throws IllegalArgumentException if a column is missing from the POJO
   * @throws ColumnPersistenceException if unable to persist a column's value
   */
  Map<String, Object> getMandatoryAndPrimaryKeyColumnValues(T object) {
    final Map<String, Object> values
      = new LinkedHashMap<>(mandatoryAndPrimaryKeyColumns.size());

    for (final Map.Entry<String, FieldInfoImpl<T>> e: mandatoryAndPrimaryKeyColumns.entrySet()) {
      final String name = e.getKey();
      final FieldInfoImpl<T> field = e.getValue();
      final Object value = field.getValue(object);

      if (value == null) {
        if (table != null) {
          org.apache.commons.lang3.Validate.isTrue(
            !field.isMandatory(),
            "missing mandatory column '%s' from table '%s' for pojo '%s'",
            name, table.name(), clazz.getSimpleName()
          );
          if (field.isPartitionKey() || field.isClusteringKey()) {
            if (field.isOptional()) {
              throw new EmptyOptionalPrimaryKeyException(
                "missing primary key column '"
                + name
                + "' in table '"
                + table.name()
                + "' for pojo '"
                + clazz.getSimpleName()
                + "'"
              );
            }
            throw new IllegalArgumentException(
              "missing primary key column '"
              + name
              + "' in table '"
              + table.name()
              + "' for pojo '"
              + clazz.getSimpleName()
              + "'"
            );
          }
          org.apache.commons.lang3.Validate.isTrue(
            !field.isTypeKey(),
            "missing type key column '%s' from table '%s' for pojo '%s'",
            name, table.name(), clazz.getSimpleName()
          );
        } else {
          org.apache.commons.lang3.Validate.isTrue(
            !field.isMandatory(),
            "missing mandatory column '%s' for udt '%s'",
            name, clazz.getSimpleName()
          );
        }
      }
      values.put(name, value);
    }
    return values;
  }

  /**
   * Retrieves all non primary key columns and their values from the POJO.
   * <p>
   * <i>Note:</i> The returned values should not be encoded.
   *
   * @author paouelle
   *
   * @param  object the non-<code>null</code> POJO object
   * @return a non-<code>null</code> map of all non primary key column/value
   *         pairs for the POJO
   * @throws IllegalArgumentException if a mandatory column is missing from
   *         the POJO
   */
  Map<String, Object> getNonPrimaryKeyColumnValues(T object) {
    final Map<String, Object> values = new LinkedHashMap<>(nonPrimaryKeyColumns.size());

    for (final Map.Entry<String, FieldInfoImpl<T>> e: nonPrimaryKeyColumns.entrySet()) {
      final String name = e.getKey();
      final FieldInfoImpl<T> field = e.getValue();
      final Object value = field.getNonEncodedValue(object);

      if (table != null) {
        org.apache.commons.lang3.Validate.isTrue(
          !(field.isMandatory() && (value == null)),
          "missing mandatory column '%s' from table '%s' for pojo '%s'",
          name,
          table.name(),
          clazz.getSimpleName()
        );
      } else {
        org.apache.commons.lang3.Validate.isTrue(
          !(field.isMandatory() && (value == null)),
          "missing mandatory column '%s' for udt '%s'",
          name, clazz.getSimpleName()
        );
      }
      values.put(name, value);
    }
    return values;
  }

  /**
   * Retrieves the specified column value from the POJO.
   *
   * @author paouelle
   *
   * @param  object the non-<code>null</code> POJO object
   * @param  name the name of the column to retrieve
   * @return the column value for the POJO
   * @throws IllegalArgumentException if the column name is not defined by the
   *         POJO or is mandatory and missing from the POJO
   * @throws ColumnPersistenceException if unable to persist a column's value
   */
  Object getColumnValue(T object, CharSequence name) {
    final String n;

    if (name instanceof Utils.CNameSequence) {
      n = ((Utils.CNameSequence)name).getName();
    } else {
      n = name.toString();
    }
    final FieldInfoImpl<T> field = columns.get(n);

    if (table != null) {
      org.apache.commons.lang3.Validate.isTrue(
        field != null,
        "pojo '%s' doesn't define column '%s' in table '%s'",
        clazz.getSimpleName(),
        n,
        table.name()
      );
    } else {
      org.apache.commons.lang3.Validate.isTrue(
        field != null,
        "udt '%s' doesn't define column '%s'",
        clazz.getSimpleName(),
        n
      );
    }
    final Object value = field.getValue(object);

    if (value == null) {
      if (table != null) {
        org.apache.commons.lang3.Validate.isTrue(
          !field.isMandatory(),
          "missing mandatory column '%s' in table '%s' for pojo '%s'",
          n, table.name(), clazz.getSimpleName()
        );
        if (field.isPartitionKey() || field.isClusteringKey()) {
          if (field.isOptional()) {
            throw new EmptyOptionalPrimaryKeyException(
              "missing primary key column '"
              + n
              + "' in table '"
              + table.name()
              + "' for pojo '"
              + clazz.getSimpleName()
              + "'"
            );
          }
          throw new IllegalArgumentException(
            "missing primary key column '"
            + n
            + "' in table '"
            + table.name()
            + "' for pojo '"
            + clazz.getSimpleName()
            + "'"
          );
        }
        org.apache.commons.lang3.Validate.isTrue(
          !field.isTypeKey(),
          "missing type key column '%s' in table '%s' for pojo '%s'",
          n, table.name(), clazz.getSimpleName()
        );
      } else {
        org.apache.commons.lang3.Validate.isTrue(
          !field.isMandatory(),
          "missing mandatory column '%s' for udt '%s'",
          n, clazz.getSimpleName()
        );
      }
    }
    return value;
  }

  /**
   * Retrieves the specified columns and their values from the POJO.
   *
   * @author paouelle
   *
   * @param  object the non-<code>null</code> POJO object
   * @param  names the names of the columns to retrieve
   * @return a non-<code>null</code> map of all requested column/value pairs
   *         for the POJO
   * @throws IllegalArgumentException if any of the column names are not defined
   *         by the POJO or is mandatory and missing from the POJO
   * @throws ColumnPersistenceException if unable to persist a column's value
   */
  Map<String, Object> getColumnValues(T object, Iterable<CharSequence> names) {
    final Map<String, Object> values = new LinkedHashMap<>(columns.size());

    for (final CharSequence name: names) {
      final String n;

      if (name instanceof Utils.CNameSequence) {
        n = ((Utils.CNameSequence)name).getName();
      } else {
        n = name.toString();
      }
      values.put(n, getColumnValue(object, n));
    }
    return values;
  }

  /**
   * Retrieves the specified columns and their values from the POJO.
   *
   * @author paouelle
   *
   * @param  object the non-<code>null</code> POJO object
   * @param  names the names of the columns to retrieve
   * @return a non-<code>null</code> map of all requested column/value pairs
   *         for the POJO
   * @throws IllegalArgumentException if any of the column names are not defined
   *         by the POJO or is mandatory and missing from the POJO
   * @throws ColumnPersistenceException if unable to persist a column's value
   */
  Map<String, Object> getColumnValues(T object, CharSequence... names) {
    final Map<String, Object> values = new LinkedHashMap<>(columns.size());

    for (final CharSequence name: names) {
      final String n;

      if (name instanceof Utils.CNameSequence) {
        n = ((Utils.CNameSequence)name).getName();
      } else {
        n = name.toString();
      }
      values.put(n, getColumnValue(object, n));
    }
    return values;
  }

  /**
   * Gets the set of column fields for the POJO in this table.
   *
   * @author paouelle
   *
   * @return a non-<code>null</code> set of all column fields
   */
  protected Collection<FieldInfoImpl<T>> getColumnsImpl() {
    return columns.values();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.info.TableInfo#getObjectClass()
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
   * @see com.github.helenusdriver.driver.info.TableInfo#getClassInfo()
   */
  @Override
  public ClassInfo<T> getClassInfo() {
    return cinfo;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.info.TableInfo#getTable()
   */
  @Override
  public Table getTable() {
    return table;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.info.TableInfo#getName()
   */
  @Override
  public String getName() {
    return name;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.info.TableInfo#hasCollectionColumns()
   */
  @Override
  public boolean hasCollectionColumns() {
    return hasCollectionColumns;
  }

  /**
   * Checks if a column is defined in this table.
   *
   * @author paouelle
   *
   * @param  name the name of the column to check if it is defined in this table
   * @return <code>true</code> if the specified column is defined in this table;
   *         <code>false</code> otherwise
   */
  public boolean hasColumn(Object name) {
    org.apache.commons.lang3.Validate.notNull(name, "invalid null column name");
    if (name instanceof Utils.CNameSequence) {
      for (final String n: ((Utils.CNameSequence)name).getNames()) {
        if (!hasColumn(n)) { // recurse to validate
          return false;
        }
      }
      return true;
    }
    if (name instanceof Utils.FCall) {
      for (final Object parm: ((Utils.FCall)name)) {
        if (!hasColumn(parm)) {
          return false;
        }
      }
      return true;
    }
    final String n;

    if (name instanceof Utils.CName) {
      n = ((Utils.CName)name).getColumnName();
    } else if (name instanceof CharSequence) {
      n = name.toString();
    } else {
      return false;
    }
    return columns.containsKey(n);
  }

  /**
   * Checks if a column is defined as a primary key in this table.
   *
   * @author paouelle
   *
   * @param  name the name of the column to check if it is defined as a primary
   *         key in this table
   * @return <code>true</code> if the specified column is defined as a primary
   *         key in this table; <code>false</code> otherwise
   */
  public boolean hasPrimaryKey(Object name) {
    org.apache.commons.lang3.Validate.notNull(name, "invalid null column name");
    if (table == null) {
      return false;
    }
    if (name instanceof Utils.CNameSequence) {
      for (final String n: ((Utils.CNameSequence)name).getNames()) {
        if (!hasPrimaryKey(n)) { // recurse to validate
          return false;
        }
      }
      return true;
    }
    if (name instanceof Utils.FCall) {
      for (final Object parm: ((Utils.FCall)name)) {
        if (!hasPrimaryKey(parm)) {
          return false;
        }
      }
      return true;
    }
    final String n;

    if (name instanceof Utils.CName) {
      n = ((Utils.CName)name).getColumnName();
    } else if (name instanceof CharSequence) {
      n = name.toString();
    } else {
      return false;
    }
    return primaryKeyColumns.containsKey(n);
  }

  /**
   * Checks if a column is defined as the last partition key in this table.
   *
   * @author paouelle
   *
   * @param  name the name of the column to check if it is defined as the last
   *         partition key in this table
   * @return <code>true</code> if the specified column is defined as the last
   *         partition key in this table; <code>false</code> otherwise
   */
  public boolean isLastPartitionKey(Object name) {
    org.apache.commons.lang3.Validate.notNull(name, "invalid null column name");
    if (table == null) {
      return false;
    }
    if (name instanceof Utils.CNameSequence) {
      for (final String n: ((Utils.CNameSequence)name).getNames()) {
        if (!isLastPartitionKey(n)) { // recurse to validate
          return false;
        }
      }
      return true;
    }
    if (name instanceof Utils.FCall) {
      for (final Object parm: ((Utils.FCall)name)) {
        if (!isLastPartitionKey(parm)) {
          return false;
        }
      }
      return true;
    }
    final String n;

    if (name instanceof Utils.CName) {
      n = ((Utils.CName)name).getColumnName();
    } else if (name instanceof CharSequence) {
      n = name.toString();
    } else {
      return false;
    }
    final FieldInfoImpl<?> f = partitionKeyColumns.get(n);

    return (f != null) ? f.isLast() : false;
  }

  /**
   * Checks if a column is defined as the last clustering key in this table.
   *
   * @author paouelle
   *
   * @param  name the name of the column to check if it is defined as the last
   *         clustering key in this table
   * @return <code>true</code> if the specified column is defined as the last
   *         clustering key in this table; <code>false</code> otherwise
   */
  public boolean isLastClusteringKey(Object name) {
    org.apache.commons.lang3.Validate.notNull(name, "invalid null column name");
    if (table == null) {
      return false;
    }
    if (name instanceof Utils.CNameSequence) {
      for (final String n: ((Utils.CNameSequence)name).getNames()) {
        if (!isLastPartitionKey(n)) { // recurse to validate
          return false;
        }
      }
      return true;
    }
    if (name instanceof Utils.FCall) {
      for (final Object parm: ((Utils.FCall)name)) {
        if (!isLastPartitionKey(parm)) {
          return false;
        }
      }
      return true;
    }
    final String n;

    if (name instanceof Utils.CName) {
      n = ((Utils.CName)name).getColumnName();
    } else if (name instanceof CharSequence) {
      n = name.toString();
    } else {
      return false;
    }
    final FieldInfoImpl<?> f = clusteringKeyColumns.get(n);

    return (f != null) ? f.isLast() : false;
  }

  /**
   * Checks if a column is defined as a multi-key in this table.
   *
   * @author paouelle
   *
   * @param  name the name of the column to check if it is defined as a multi-key
   *         in this table
   * @return <code>true</code> if the specified column is defined as a multi-key
   *         in this table; <code>false</code> otherwise
   */
  public boolean isMultiKey(Object name) {
    org.apache.commons.lang3.Validate.notNull(name, "invalid null column name");
    if (table == null) {
      return false;
    }
    if (name instanceof Utils.CNameSequence) {
      for (final String n: ((Utils.CNameSequence)name).getNames()) {
        if (!isMultiKey(n)) { // recurse to validate
          return false;
        }
      }
      return true;
    }
    if (name instanceof Utils.FCall) {
      for (final Object parm: ((Utils.FCall)name)) {
        if (!isMultiKey(parm)) {
          return false;
        }
      }
      return true;
    }
    final String n;

    if (name instanceof Utils.CName) {
      n = ((Utils.CName)name).getColumnName();
    } else if (name instanceof CharSequence) {
      n = name.toString();
    } else {
      return false;
    }
    final FieldInfoImpl<?> f = multiKeyColumns.get(n);

    return (f != null);
  }

  /**
   * Gets a column field for the POJO in this table given its field name.
   *
   * @author paouelle
   *
   * @param  f the field to retrieve its column from this table
   * @return the corresponding column field or <code>null</code> if not defined
   */
  public FieldInfoImpl<T> getColumnByField(Field f) {
    return fields.get(Pair.of(f.getName(), f.getDeclaringClass()));
  }

  /**
   * Gets a column field for the POJO in this table given its column name.
   *
   * @author paouelle
   *
   * @param  name the name of the column to retrieve in this table
   * @return the corresponding column field or <code>null</code> if not defined
   */
  public FieldInfoImpl<T> getColumn(CharSequence name) {
    return (name != null) ? columns.get(name.toString()) : null;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.info.TableInfo#iterator()
   */
  @Override
  public Iterator<FieldInfo<T>> iterator() {
    final Iterator<FieldInfoImpl<T>> iterator = columns.values().iterator();

    return new Iterator<FieldInfo<T>>() {
      private FieldInfoImpl<T> current = null;

      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }
      @Override
      public FieldInfo<T> next() {
        this.current = iterator.next();
        return current;
      }
      @SuppressWarnings("synthetic-access")
      @Override
      public void remove() {
        iterator.remove();
        final String cname = current.getColumnName();

        // make sure the remove the field from all internal maps too
        if (table != null) {
          primaryKeyColumns.remove(cname);
          partitionKeyColumns.remove(cname);
          finalPrimaryKeyValues.remove(cname);
          clusteringKeyColumns.remove(cname);
          final FieldInfoImpl<T> old = typeKeyColumn.getValue();

          if ((old != null) && old.getColumnName().equals(cname)) {
            typeKeyColumn.setValue(null);
          }
          indexColumns.remove(cname);
        }
        mandatoryAndPrimaryKeyColumns.remove(cname);
        nonPrimaryKeyColumns.remove(cname);
        fields.remove(Pair.of(current.getName(), current.getDeclaringClass()));
        this.current = null; // clear the current pointer since it was removed!
      }
    };
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.info.TableInfo#getColumns()
   */
  @Override
  public Collection<FieldInfo<T>> getColumns() {
    return Collections.unmodifiableCollection(columns.values());
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.info.TableInfo#columns()
   */
  @Override
  @SuppressWarnings({"rawtypes", "cast", "unchecked"})
  public Stream<FieldInfo<T>> columns() {
    return (Stream<FieldInfo<T>>)(Stream)columns.values().stream();
  }

  /**
   * Gets the set of non-primary key column fields for the POJO in this table.
   *
   * @author paouelle
   *
   * @return a non-<code>null</code> set of all non-primary key column fields
   */
  public Collection<FieldInfoImpl<T>> getNonPrimaryKeys() {
    return nonPrimaryKeyColumns.values();
  }

  /**
   * Gets the set of partition key column fields for the POJO in this table.
   *
   * @author paouelle
   *
   * @return a non-<code>null</code> set of all partition key column fields
   */
  public Collection<FieldInfoImpl<T>> getPartitionKeys() {
    return partitionKeyColumns.values();
  }

  /**
   * Gets the set of suffix and partition column fields for the POJO in this table.
   *
   * @author paouelle
   *
   * @return a non-<code>null</code> set of all suffix and partition column fields
   */
  public Collection<FieldInfoImpl<T>> getSuffixAndPartitionKeys() {
    final Map<String, FieldInfoImpl<T>> skeys = cinfo.getSuffixKeys();
    final Map<String, FieldInfoImpl<T>> keys = new LinkedHashMap<>(
      primaryKeyColumns.size() + skeys.size()
    );

    // start with suffix keys
    keys.putAll(skeys);
    // now add partition keys (overriding suffixes if names clashes!!!)
    keys.putAll(partitionKeyColumns);
    return keys.values();
  }

  /**
   * Gets the set of clustering key column fields for the POJO in this table.
   *
   * @author paouelle
   *
   * @return a non-<code>null</code> set of all clustering key column fields
   */
  public Collection<FieldInfoImpl<T>> getClusteringKeys() {
    return clusteringKeyColumns.values();
  }

  /**
   * Gets the type key column fields for the POJO in this table.
   *
   * @author paouelle
   *
   * @return an optional type key column field
   */
  public Optional<FieldInfoImpl<T>> getTypeKey() {
    return Optional.ofNullable(typeKeyColumn.getValue());
  }

  /**
   * Gets the set of primary column fields for the POJO in this table.
   *
   * @author paouelle
   *
   * @return a non-<code>null</code> set of all primary column fields
   */
  public Collection<FieldInfoImpl<T>> getPrimaryKeys() {
    return primaryKeyColumns.values();
  }

  /**
   * Gets the multi-keys for the POJO in this table if one is defined.
   *
   * @author paouelle
   *
   * @return a non-<code>null</code> set of all multi-key column fields
   */
  public List<FieldInfoImpl<T>> getMultiKeys() {
    return multiKeyColumnsList;
  }

  /**
   * Gets the set of suffix and primary column fields for the POJO in this table.
   *
   * @author paouelle
   *
   * @return a non-<code>null</code> set of all suffix and primary column fields
   */
  public Collection<FieldInfoImpl<T>> getSuffixAndPrimaryKeys() {
    final Map<String, FieldInfoImpl<T>> skeys = cinfo.getSuffixKeys();
    final Map<String, FieldInfoImpl<T>> keys = new LinkedHashMap<>(
      primaryKeyColumns.size() + skeys.size()
    );

    // start with suffix keys
    keys.putAll(skeys);
    // now add primary keys (overriding suffixes if names clashes!!!)
    keys.putAll(primaryKeyColumns);
    return keys.values();
  }

  /**
   * Gets the a primary column fields for the POJO in this table given its
   * column name.
   *
   * @author paouelle
   *
   * @param  name the name of the primary column to retrieve in this table
   * @return the corresponding primary column field or <code>null</code> if
   *         not defined
   */
  public FieldInfoImpl<T> getPrimaryKey(CharSequence name) {
    return (name != null) ? primaryKeyColumns.get(name.toString()) : null;
  }

  /**
   * Gets the set of index column fields for the POJO in this table.
   *
   * @author paouelle
   *
   * @return a non-<code>null</code> set of all index column fields
   */
  public Collection<FieldInfoImpl<T>> getIndexes() {
    return indexColumns.values();
  }

  /**
   * Gets the set of mandatory and primary column fields for the POJO in this
   * table.
   *
   * @author paouelle
   *
   * @return a non-<code>null</code> set of all mandatory and primary column
   *         fields
   */
  public Collection<FieldInfoImpl<T>> getMandatoryAndPrimaryKeys() {
    return mandatoryAndPrimaryKeyColumns.values();
  }

  /**
   * Gets the set of mandatory and primary columns for the POJO in this table.
   *
   * @author paouelle
   *
   * @return a non-<code>null</code> set of all mandatory and primary column
   *         names
   */
  public Set<String> getMandatoryAndPrimaryKeyColumns() {
    return mandatoryAndPrimaryKeyColumns.keySet();
  }

  /**
   * Validates if a column is defined by the POJO in this table.
   *
   * @author paouelle
   *
   * @param  name the column name to validate
   * @throws NullPointerException if <code>name</code> is <code>null</code>
   * @throws IllegalArgumentException if the specified column is not defined
   *         by the POJO
   */
  public void validateColumn(Object name) {
    validateColumn(name, false);
  }

  /**
   * Validates if specified columns are defined by the POJO in this table.
   *
   * @author paouelle
   *
   * @param  names the names of the column to validate
   * @throws NullPointerException if any of the column names are <code>null</code>
   * @throws IllegalArgumentException if any of the column name are not
   *         defined by the POJO
   */
  public void validateColumns(Iterable<Object> names) {
    for (final Object name: names) {
      validateColumn(name);
    }
  }

  /**
   * Validates if specified columns are defined by the POJO in this table.
   *
   * @author paouelle
   *
   * @param  names the names of the column to validate
   * @throws NullPointerException if any of the column names are <code>null</code>
   * @throws IllegalArgumentException if any of the column name are not
   *         defined by the POJO
   */
  public void validateColumns(Object... names) {
    for (final Object name: names) {
      validateColumn(name);
    }
  }

  /**
   * Validates if a column is not defined as either a mandatory or a primary
   * key column by the POJO in this table.
   * <p>
   * <i>Note:</i> Only the column names passed as a {@link String} are
   * validated.
   *
   * @author paouelle
   *
   * @param  name the column name to validate
   * @throws NullPointerException if <code>name</code> is <code>null</code>
   * @throws IllegalArgumentException if the specified column is defined
   *         by the POJO as a mandatory or a primary key column
   */
  public void validateNotMandatoryOrPrimaryKeyColumn(Object name) {
    org.apache.commons.lang3.Validate.notNull(name, "invalid null column name");
    if (name instanceof Utils.CNameSequence) {
      for (final String n: ((Utils.CNameSequence)name).getNames()) {
        validateNotMandatoryOrPrimaryKeyColumn(n); // recurse to validate
      }
      return;
    }
    final String n;

    if (name instanceof Utils.CName) {
      n = ((Utils.CName)name).getColumnName();
    } else if (name instanceof CharSequence) {
      n = name.toString();
    } else {
      throw new IllegalArgumentException(
        "unexpected column name: " + name
      );
    }
    if (table != null) {
      org.apache.commons.lang3.Validate.isTrue(
        !mandatoryAndPrimaryKeyColumns.containsKey(n),
        "pojo '%s' defines mandatory or primary key column '%s' in table '%s'",
        clazz.getSimpleName(),
        n,
        table.name()
      );
    } else {
      org.apache.commons.lang3.Validate.isTrue(
        !mandatoryAndPrimaryKeyColumns.containsKey(n),
        "udt '%s' defines mandatory column '%s'",
        clazz.getSimpleName(),
        n
      );
    }
  }

  /**
   * Validates if a column is defined as either a primary key or an index column
   * by the POJO in this table.
   * <p>
   * <i>Note:</i> Only the column names passed as a {@link String} are
   * validated.
   *
   * @author paouelle
   *
   * @param  name the column name to validate
   * @throws NullPointerException if <code>name</code> is <code>null</code>
   * @throws IllegalArgumentException if the specified column is not defined
   *         by the POJO or is not a primary key or an index column
   */
  public void validatePrimaryKeyOrIndexColumn(Object name) {
    org.apache.commons.lang3.Validate.notNull(name, "invalid null column name");
    if (name instanceof Utils.CNameSequence) {
      for (final String n: ((Utils.CNameSequence)name).getNames()) {
        validatePrimaryKeyOrIndexColumn(n); // recurse to validate
      }
      return;
    }
    final String n;

    if (name instanceof Utils.CName) {
      n = ((Utils.CName)name).getColumnName();
    } else if (name instanceof CharSequence) {
      n = name.toString();
    } else {
      throw new IllegalArgumentException(
        "unexpected column name: " + name
      );
    }
    final FieldInfoImpl<T> field = columns.get(n);

    if (table != null) {
      org.apache.commons.lang3.Validate.isTrue(
        field != null,
        "pojo '%s' doesn't define column '%s' in table '%s'",
        clazz.getSimpleName(),
        n,
        table.name()
      );
      org.apache.commons.lang3.Validate.isTrue(
        field.isPartitionKey() || field.isClusteringKey() || field.isIndex(),
        "pojo '%s' doesn't define primary key or index column '%s' in table '%s'",
        clazz.getSimpleName(),
        n,
        table.name()
      );
    } else {
      org.apache.commons.lang3.Validate.isTrue(
        field != null,
        "udt '%s' doesn't define column '%s'",
        clazz.getSimpleName(),
        n
      );
    }
  }

  /**
   * Validates if a column is defined as either a suffix key, a primary key, or
   * an index column, by the POJO in this table.
   * <p>
   * <i>Note:</i> Only the column names passed as a {@link String} are
   * validated.
   *
   * @author paouelle
   *
   * @param  name the column name to validate
   * @throws NullPointerException if <code>name</code> is <code>null</code>
   * @throws IllegalArgumentException if the specified column is not defined
   *         by the POJO or is not a primary key or an index column
   */
  public void validateSuffixKeyOrPrimaryKeyOrIndexColumn(Object name) {
    org.apache.commons.lang3.Validate.notNull(name, "invalid null column name");
    if (name instanceof Utils.CNameSequence) {
      for (final String n: ((Utils.CNameSequence)name).getNames()) {
        validatePrimaryKeyOrIndexColumn(n); // recurse to validate
      }
      return;
    }
    final String n;

    if (name instanceof Utils.CName) {
      n = ((Utils.CName)name).getColumnName();
    } else if (name instanceof CharSequence) {
      n = name.toString();
    } else {
      throw new IllegalArgumentException(
        "unexpected column name: " + name
      );
    }
    FieldInfoImpl<T> field = columns.get(n);

    if (field == null) {
      // check suffixes
      field = (FieldInfoImpl<T>)cinfo.getSuffixKey(n);
    }
    if (table != null) {
      org.apache.commons.lang3.Validate.isTrue(
        field != null,
        "pojo '%s' doesn't define column or suffix key '%s' in table '%s'",
        clazz.getSimpleName(),
        n,
        table.name()
      );
      org.apache.commons.lang3.Validate.isTrue(
        field.isPartitionKey() || field.isClusteringKey() || field.isSuffixKey() || field.isIndex(),
        "pojo '%s' doesn't define suffix key, primary key, or index column '%s' in table '%s'",
        clazz.getSimpleName(),
        n,
        table.name()
      );
    } else {
      org.apache.commons.lang3.Validate.isTrue(
        field != null,
        "udt '%s' doesn't define column '%s'",
        clazz.getSimpleName(),
        n
      );
    }
  }

  /**
   * Validates if a column is defined as a counter by the POJO in this table.
   * <p>
   * <i>Note:</i> Only the column names passed as a {@link String} are
   * validated.
   *
   * @author paouelle
   *
   * @param  name the column name to validate
   * @throws NullPointerException if <code>name</code> is <code>null</code>
   * @throws IllegalArgumentException if the specified column is not defined
   *         by the POJO or is not a counter column
   */
  public void validateCounterColumn(Object name) {
    org.apache.commons.lang3.Validate.notNull(name, "invalid null column name");
    if (name instanceof Utils.CNameSequence) {
      for (final String n: ((Utils.CNameSequence)name).getNames()) {
        validateCounterColumn(n); // recurse to validate
      }
      return;
    }
    final String n;

    if (name instanceof Utils.CName) {
      n = ((Utils.CName)name).getColumnName();
    } else if (name instanceof CharSequence) {
      n = name.toString();
    } else {
      throw new IllegalArgumentException(
        "unexpected column name: " + name
      );
    }
    final FieldInfoImpl<T> field = columns.get(n);

    if (table != null) {
      org.apache.commons.lang3.Validate.isTrue(
        field != null,
        "pojo '%s' doesn't define column '%s' in table '%s'",
        clazz.getSimpleName(),
        n,
        table.name()
      );
      org.apache.commons.lang3.Validate.isTrue(
        field.isCounter(),
        "pojo '%s' doesn't define counter column '%s' in table '%s'",
        clazz.getSimpleName(),
        n,
        table.name()
      );
    } else {
      org.apache.commons.lang3.Validate.isTrue(
        field != null,
        "udt '%s' doesn't define column '%s'",
        clazz.getSimpleName(),
        n
      );
    }
  }

  /**
   * Validates if a column is defined by the POJO and its potential value in
   * this table.
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
  public void validateColumnAndValue(CharSequence name, Object value) {
    org.apache.commons.lang3.Validate.notNull(name, "invalid null column name");
    if (name instanceof Utils.CNameSequence) {
      for (final String n: ((Utils.CNameSequence)name).getNames()) {
        validateColumnAndValue(n, value); // recurse to validate
      }
      return;
    }
    final String n = name.toString();
    final FieldInfoImpl<T> field = columns.get(n);

    if (table != null) {
      org.apache.commons.lang3.Validate.isTrue(
        field != null,
        "pojo '%s' doesn't define column '%s' in table '%s'",
        clazz.getSimpleName(),
        n,
        table.name()
      );
    } else {
      org.apache.commons.lang3.Validate.isTrue(
        field != null,
        "udt '%s' doesn't define column '%s'",
        clazz.getSimpleName(),
        n
      );
    }
    field.validateValue(value);
  }

  /**
   * Validates if a column or suffix is defined by the POJO as a column or as a
   * suffix and its potential value in this table.
   *
   * @author paouelle
   *
   * @param  name the column or suffix name to validate
   * @param  value the value to validate for the column
   * @throws NullPointerException if <code>name</code> is <code>null</code>
   * @throws IllegalArgumentException if the specified column or suffix is not
   *         defined by the POJO or if the specified value is not of the
   *         right type or is <code>null</code> when the column is mandatory
   */
  public void validateSuffixOrColumnAndValue(CharSequence name, Object value) {
    org.apache.commons.lang3.Validate.notNull(
      name, "invalid null column name or suffix key"
    );
    if (name instanceof Utils.CNameSequence) {
      for (final String n: ((Utils.CNameSequence)name).getNames()) {
        validateSuffixOrColumnAndValue(n, value); // recurse to validate
      }
      return;
    }
    final String n = name.toString();
    FieldInfoImpl<T> field = columns.get(n);

    if (field == null) {
      // check suffixes
      field = (FieldInfoImpl<T>)cinfo.getSuffixKey(n);
    }
    if (table != null) {
      org.apache.commons.lang3.Validate.isTrue(
        field != null,
        "pojo '%s' doesn't define column or suffix key '%s' in table '%s'",
        clazz.getSimpleName(),
        n,
        table.name()
      );
    } else {
      org.apache.commons.lang3.Validate.isTrue(
        field != null,
        "udt '%s' doesn't define column '%s'",
        clazz.getSimpleName(),
        n
      );
    }
    field.validateValue(value);
  }

  /**
   * Validates if a column is defined by the POJO and its potential values in
   * this table.
   *
   * @author paouelle
   *
   * @param  name the column name to validate
   * @param  values the values to validate for the column
   * @throws NullPointerException if <code>name</code> is <code>null</code>
   * @throws IllegalArgumentException if the specified column is not defined
   *         by the POJO or if any of the specified values is not of the
   *         right type or is <code>null</code> when the column is mandatory
   */
  public void validateColumnAndValues(
    CharSequence name, Collection<?> values
  ) {
    org.apache.commons.lang3.Validate.notNull(name, "invalid null column name");
    if (name instanceof Utils.CNameSequence) {
      for (final String n: ((Utils.CNameSequence)name).getNames()) {
        validateColumnAndValues(n, values); // recurse to validate
      }
      return;
    }
    final String n = name.toString();
    final FieldInfoImpl<T> field = columns.get(n);

    if (table != null) {
      org.apache.commons.lang3.Validate.isTrue(
        field != null,
        "pojo '%s' doesn't define column '%s' in table '%s'",
        clazz.getSimpleName(),
        n,
        table.name()
      );
    } else {
      org.apache.commons.lang3.Validate.isTrue(
        field != null,
        "udt '%s' doesn't define column '%s'",
        clazz.getSimpleName(),
        n
      );
    }
    for (final Object value: values) {
      field.validateValue(value);
    }
  }

  /**
   * Validates if a column is defined as the given collection data type by the
   * POJO and its potential element value in this table.
   *
   * @author paouelle
   *
   * @param  name the column name to validate
   * @param  type the collection data type of the column to validate
   * @param  value the element value to be validated for the collection
   * @throws NullPointerException if <code>name</code> or <code>type</code> is
   *         <code>null</code>
   * @throws IllegalArgumentException if the specified column is not defined
   *         by the POJO as the provided data type or if the specified value is
   *         not of the right element type or is <code>null</code> when the
   *         column is mandatory
   */
  public void validateCollectionColumnAndValue(
    CharSequence name, DataType type, Object value
  ) {
    org.apache.commons.lang3.Validate.notNull(name, "invalid null column name");
    if (name instanceof Utils.CNameSequence) {
      for (final String n: ((Utils.CNameSequence)name).getNames()) {
        validateCollectionColumnAndValue(n, type, value); // recurse to validate
      }
      return;
    }
    org.apache.commons.lang3.Validate.notNull(type, "invalid null column type");
    org.apache.commons.lang3.Validate.isTrue(type.isCollection(), "%s is not a collection type", name);
    final String n = name.toString();
    final FieldInfoImpl<T> field = columns.get(n);

    if (table != null) {
      org.apache.commons.lang3.Validate.isTrue(
        field != null,
        "pojo '%s' doesn't define column '%s' in table '%s'",
        clazz.getSimpleName(),
        n,
        table.name()
      );
    } else {
      org.apache.commons.lang3.Validate.isTrue(
        field != null,
        "udt '%s' doesn't define column '%s'",
        clazz.getSimpleName(),
        n
      );
    }
    field.validateCollectionValue(type, value);
  }

  /**
   * Validates if a column is defined as the given collection data type by the
   * POJO and its potential element value in this table.
   *
   * @author paouelle
   *
   * @param  name the column name to validate
   * @param  type the collection data type of the column to validate
   * @param  values the list of element values to be validated for the collection
   * @throws NullPointerException if <code>name</code> or <code>type</code> is
   *         <code>null</code>
   * @throws IllegalArgumentException if the specified column is not
   *         defined by the POJO as the specified data type or if any of the
   *         specified values are not of the right element type or are
   *         <code>null</code> when the column is mandatory
   */
  public void validateCollectionColumnAndValues(
    CharSequence name, DataType type, Iterable<?> values
  ) {
    org.apache.commons.lang3.Validate.notNull(name, "invalid null column name");
    if (name instanceof Utils.CNameSequence) {
      for (final String n: ((Utils.CNameSequence)name).getNames()) {
        validateCollectionColumnAndValues(n, type, values); // recurse to validate
      }
      return;
    }
    org.apache.commons.lang3.Validate.notNull(values, "invalid null list of values");
    org.apache.commons.lang3.Validate.isTrue(type.isCollection(), "%s is not a collection type", name);
    final String n = name.toString();
    final FieldInfoImpl<T> field = columns.get(n);

    if (table != null) {
      org.apache.commons.lang3.Validate.isTrue(
        field != null,
        "pojo '%s' doesn't define column '%s' in table '%s'",
        clazz.getSimpleName(),
        n,
        table.name()
      );
    } else {
      org.apache.commons.lang3.Validate.isTrue(
        field != null,
        "udt '%s' doesn't define column '%s'",
        clazz.getSimpleName(),
        n
      );
    }
    for (final Object value: values) {
      field.validateCollectionValue(type, value);
    }
  }

  /**
   * Validates if a column is defined as a map by the POJO and its potential
   * mapping key/value in this table.
   *
   * @author paouelle
   *
   * @param  name the column name to validate
   * @param  key the mapping key to be validated for the map
   * @param  value the mapping value to be validated for the map
   * @throws NullPointerException if <code>name</code> is <code>null</code>
   * @throws IllegalArgumentException if the specified column is not defined
   *         by the POJO as a map or if the specified key/value are not of
   *         the right mapping types or the value is <code>null</code> when the
   *         column is mandatory
   */
  public void validateMapColumnAndKeyValue(
    CharSequence name, Object key, Object value
  ) {
    org.apache.commons.lang3.Validate.notNull(name, "invalid null column name");
    if (name instanceof Utils.CNameSequence) {
      for (final String n: ((Utils.CNameSequence)name).getNames()) {
        validateMapColumnAndKeyValue(n, key, value); // recurse to validate
      }
      return;
    }
    final String n = name.toString();
    final FieldInfoImpl<T> field = columns.get(n);

    if (table != null) {
      org.apache.commons.lang3.Validate.isTrue(
        field != null,
        "pojo '%s' doesn't define column '%s' in table '%s'",
        clazz.getSimpleName(),
        n,
        table.name()
      );
    } else {
      org.apache.commons.lang3.Validate.isTrue(
        field != null,
        "udt '%s' doesn't define column '%s'",
        clazz.getSimpleName(),
        n
      );
    }
    field.validateMapKeyValue(key, value);
  }

  /**
   * Validates if a column is defined as a map by the POJO and its potential
   * mapping key/value in this table.
   *
   * @author paouelle
   *
   * @param  name the column name to validate
   * @param  mappings the mappings to be validated for the map
   * @throws NullPointerException if <code>name</code> is <code>null</code>
   * @throws IllegalArgumentException if the specified column is not defined
   *         by the POJO as a map or if the specified key/value are not of
   *         the right mapping types or the value is <code>null</code> when the
   *         column is mandatory
   */
  public void validateMapColumnAndKeyValues(
    CharSequence name, Map<?, ?> mappings
  ) {
    org.apache.commons.lang3.Validate.notNull(name, "invalid null column name");
    if (name instanceof Utils.CNameSequence) {
      for (final String n: ((Utils.CNameSequence)name).getNames()) {
        validateMapColumnAndKeyValues(n, mappings); // recurse to validate
      }
      return;
    }
    org.apache.commons.lang3.Validate.notNull(mappings, "invalid null collection of mappings");
    final String n = name.toString();
    final FieldInfoImpl<T> field = columns.get(n);

    if (table != null) {
      org.apache.commons.lang3.Validate.isTrue(
        field != null,
        "pojo '%s' doesn't define column '%s' in table '%s'",
        clazz.getSimpleName(),
        n,
        table.name()
      );
    } else {
      org.apache.commons.lang3.Validate.isTrue(
        field != null,
        "udt '%s' doesn't define column '%s'",
        clazz.getSimpleName(),
        n
      );
    }
    for (final Map.Entry<?, ?> e: mappings.entrySet()) {
      field.validateMapKeyValue(e.getKey(), e.getValue());
    }
  }

  /**
   * Gets all user-defined types the pojo class represented by this table is
   * dependent on.
   *
   * @author paouelle
   *
   * @return a stream of all class infos for the user-defined types the pojo
   *         class depends on
   */
  public Stream<UDTClassInfoImpl<?>> udts() {
    return columns.values().stream().flatMap(f -> f.udts());
  }
}

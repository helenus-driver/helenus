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
package org.helenus.driver.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;

import com.datastax.driver.core.DataTypeParser;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;

import org.helenus.driver.StatementBridge;
import org.helenus.driver.persistence.CQLDataType;
import org.helenus.driver.persistence.Ordering;

/**
 * The <code>AlterCreateTableImpl</code> class defines a ALTER TABLE or a
 * CREATE TABLE statement based on the current schema defined in Cassandra.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Apr 1, 2015 - paouelle - Creation
 *
 * @param <T> The type of POJO associated with this statement.
 *
 * @since 1.0
 */
public class AlterCreateTableImpl<T> extends CreateTableImpl<T> {
  /**
   * Holds the queried table infos for the POJO type keyed by the table name.
   *
   * @author paouelle
   */
  private final Map<String, List<Row>> tinfos;

  /**
   * Instantiates a new <code>AlterCreateTableImpl</code> object.
   *
   * @author paouelle
   *
   * @param  context the non-<code>null</code> class info context for the POJO
   *         associated with this statement
   * @param  mgr the non-<code>null</code> statement manager
   * @param  bridge the non-<code>null</code> statement bridge
   * @throws IllegalArgumentException if any of the specified tables are not
   *         defined in the POJO
   */
  public AlterCreateTableImpl(
    ClassInfoImpl<T>.Context context,
    StatementManagerImpl mgr,
    StatementBridge bridge
  ) {
    super(context, null, mgr, bridge);
    ifNotExists(); // in case we have 2 alter statements in a sequence
    // start by querying Cassandra system table to figure out the existing schema
    // for the required tables
    this.tinfos = super.tables.stream()
      .collect(Collectors.toMap(
        t -> t.getName(),
        t -> mgr.getSession().executeAsync(new SimpleStatement(
          "SELECT column_name,component_index,type,validator,index_name FROM system.schema_columns WHERE keyspace_name='"
              + getKeyspace()
              + "' and columnfamily_name='"
              + t.getName()
              + "'"
            )).getUninterruptibly().all()
      ));
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.CreateTableImpl#buildQueryStrings(org.helenus.driver.impl.TableInfoImpl)
   */
  @Override
  protected StringBuilder[] buildQueryStrings(TableInfoImpl<T> table) {
    final List<Row> rows = tinfos.get(table.getName());

    if (CollectionUtils.isEmpty(rows)) {
      // that would mean the table doesn't exist, so just create it brand new
      return super.buildQueryStrings(table);
    }
    final Map<String, CQLDataType> columns = new HashMap<>(table.getColumns().size() * 3 / 2);
    final List<String> pkeys = new ArrayList<>(table.getPartitionKeys().size());
    final Map<String, Ordering> ckeys = new LinkedHashMap<>(table.getClusteringKeys().size() * 3 / 2);

    for (final FieldInfoImpl<?> field: table.getColumnsImpl()) {
      columns.put(field.getColumnName(), field.getDataType());
      if (field.isMultiKey()) {
        // we need to add a new column to represent a single value from the set
        // to be the clustering key in addition to the field's column
        columns.put(
          StatementImpl.MK_PREFIX + field.getColumnName(),
          field.getDataType().getFirstArgumentType()
        );
      }
    }
    for (final FieldInfoImpl<?> field: table.getPartitionKeys()) {
      if (field.isMultiKey()) {
        // we need to add the special new column to represent a single value from the set
        // as the partition key instead of the annotated one
        pkeys.add(StatementImpl.MK_PREFIX + field.getColumnName());
      } else {
        pkeys.add(field.getColumnName());
      }
    }
    for (final FieldInfoImpl<?> field: table.getClusteringKeys()) {
      if (field.isMultiKey()) {
        // we need to add the special new column to represent a single value from the set
        // as the clustering key instead of the annotated one
        ckeys.put(
          StatementImpl.MK_PREFIX + field.getColumnName(),
          field.getClusteringKey().order()
        );
      } else {
        ckeys.put(field.getColumnName(), field.getClusteringKey().order());
      }
    }
    final List<String> columns0 = new ArrayList<>(table.getColumns().size());
    final List<String> pkeys0 = new ArrayList<>(table.getPartitionKeys().size());
    final List<Pair<String, Ordering>> ckeys0 = new ArrayList<>(table.getClusteringKeys().size());

    for (final Row row: rows) {
      final String name0 = row.getString(0);
      final String val0 = row.getString(3);
      final CQLDataType type = columns.remove(name0);

      if (type == null) { // no longer exist
        columns0.add("DROP " + name0);
      } else { // still exist
        final CQLDataType type0 = DataTypeParser.validatorToCQL(val0);
        final String type0_cql = type0.toCQL();
        final String type_cql = type.toCQL();

        if (!type_cql.equals(type0_cql)) { // type changed
          org.apache.commons.lang3.Validate.isTrue(
            type0.isAlterableTo(type),
            "column '%s' for entity '%s' cannot be altered from %s to %s",
            name0, getObjectClass().getName(), type0_cql, type_cql
          );
          columns0.add("ALTER " + name0 + " TYPE " + type_cql);
        } // else - no change for the column
      }
      final int index = row.isNull(1) ? 0 : row.getInt(1);

      switch (row.getString(2)) {
        case "regular":
          break;
        case "clustering_key":
          while (ckeys0.size() <= index) {
            ckeys0.add(null);
          }
          ckeys0.set(index, Pair.of(name0, DataTypeParser.getOrderingFrom(val0)));
          break;
        case "partition_key":
          while (pkeys0.size() <= index) {
            pkeys0.add(null);
          }
          pkeys0.set(index, name0);
          break;
      }
    }
    // validate partition keys
    org.apache.commons.lang3.Validate.isTrue(
      pkeys0.equals(pkeys),
      "partition key definition has changed; expecting '%s'", pkeys0
    );
    // validate clustering keys
    int i = 0;

    for (final Map.Entry<String, Ordering> e: ckeys.entrySet()) {
      final Pair<String, Ordering> p = ckeys0.get(i++);

      org.apache.commons.lang3.Validate.isTrue(
        p.getLeft().equals(e.getKey()) && p.getRight().equals(e.getValue()),
        "clustering key definition has changed; expecting '%s'",
        ckeys0.stream()
          .map(pp -> pp.getLeft() + '=' + pp.getRight())
          .collect(Collectors.joining(" ,", "{", "}"))
      );
    }
    // check if there are any new columns left
    columns.forEach((n, t) -> columns0.add("ADD " + n + " " + t.toCQL()));
    if (columns.isEmpty()) { // nothing to do!!!!
      return null;
    }
    final StringBuilder builder = new StringBuilder();

    builder.append("ALTER TABLE ");
    if (getKeyspace() != null) {
      Utils.appendName(getKeyspace(), builder).append('.');
    }
    Utils.appendName(table.getName(), builder);
    builder.append(' ');
    return columns0.stream()
      .map(inst -> new StringBuilder(builder).append(inst))
      .toArray(StringBuilder[]::new);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.StatementImpl#appendGroupSubType(java.lang.StringBuilder)
   */
  @Override
  protected void appendGroupSubType(StringBuilder builder) {
    builder.append(" ALTER TABLE");
  }

  /**
   * Gets the table information for the tables defined by the POJO class as
   * queried from Cassandra.
   *
   * @author paouelle
   *
   * @return the table info from Cassandra for the POJO class
   */
  public Map<String, List<Row>> getTableInfos() {
    return tinfos;
  }
}

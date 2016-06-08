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
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.DataTypeParser;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;

import org.helenus.driver.StatementBridge;
import org.helenus.driver.persistence.CQLDataType;

/**
 * The <code>AlterCreateTypeImpl</code> class defines a ALTER TYPE or CREATE TYPE
 * statement based on the current schema defined in Cassandra.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Apr 2, 2015 - paouelle - Creation
 *
 * @param <T> The type of POJO associated with this statement.
 *
 * @since 1.0
 */
public class AlterCreateTypeImpl<T> extends CreateTypeImpl<T> {
  /**
   * Instantiates a new <code>AlterCreateTypeImpl</code> object.
   *
   * @author paouelle
   *
   * @param context the non-<code>null</code> class info context for the POJO
   *        associated with this statement
   * @param mgr the non-<code>null</code> statement manager
   * @param bridge the non-<code>null</code> statement bridge
   */
  public AlterCreateTypeImpl(
    ClassInfoImpl<T>.Context context,
    StatementManagerImpl mgr,
    StatementBridge bridge
  ) {
    super(context, mgr, bridge);
    ifNotExists(); // in case we have 2 alter statements in a sequence
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.CreateTypeImpl#buildQueryStrings(org.helenus.driver.impl.UDTClassInfoImpl)
   */
  @Override
  protected StringBuilder[] buildQueryStrings(UDTClassInfoImpl<T> ucinfo) {
    final TableInfoImpl<T> table = ucinfo.getTableImpl();

    // start by querying Cassandra system table to figure out the existing schema
    // for the required type
    final Row row = mgr.getSession().executeAsync(new SimpleStatement(
      "SELECT field_names,field_types FROM system.schema_usertypes WHERE keyspace_name='"
      + getKeyspace()
      + "' and type_name='"
      + ucinfo.getName()
      + "' LIMIT 1"
    )).getUninterruptibly().one();

    if (row == null) {
      // that would mean the type doesn't exist, so just created it brand new
      return super.buildQueryStrings(ucinfo);
    }
    final Map<String, CQLDataType> columns = new HashMap<>(table.getColumns().size() * 3 / 2);

    for (final FieldInfoImpl<?> field: table.getColumnsImpl()) {
      if (field.isTypeKey() && (ucinfo instanceof UDTTypeClassInfoImpl)) {
        // don't persist type keys for those (only for UDT root entities)
        continue;
      }
      columns.put(field.getColumnName(), field.getDataType());
    }
    final List<String> names0 = row.getList(0, String.class);
    final List<String> utypes0 = row.getList(1, String.class);
    final List<String> columns0 = new ArrayList<>(names0.size());

    for (int i = 0; i < names0.size(); i++) {
      final String name0 = names0.get(i);
      final String utype0 = utypes0.get(i);
      final CQLDataType type = columns.remove(name0);

      if (type != null) {
        final CQLDataType type0 = DataTypeParser.typeToCQL(utype0);
        final String type0_cql = type0.toCQL();
        final String type_cql = type.toCQL();

        if (!type_cql.equals(type0_cql)) { // type changed
          org.apache.commons.lang3.Validate.isTrue(
            type0.isAlterableTo(type),
            "column '%s' for udt entity '%s' cannot be altered from %s to %s",
            name0, getObjectClass().getName(), type0_cql, type_cql
          );
          columns0.add("ALTER " + name0 + " TYPE " + type_cql);
        } // else - no change for the column
      } // // no longer exist - ignore and leave field there!!!
    }
    // check if there are any new columns left
    columns.forEach((n, t) -> columns0.add("ADD " + n + " " + t.toCQL()));
    if (columns.isEmpty()) { // nothing to do!!!!
      return null;
    }
    final StringBuilder builder = new StringBuilder();

    builder.append("ALTER TYPE ");
    if (getKeyspace() != null) {
      Utils.appendName(getKeyspace(), builder).append(".");
    }
    Utils.appendName(ucinfo.getName(), builder);
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
    builder.append(" ALTER TYPE");
  }
}

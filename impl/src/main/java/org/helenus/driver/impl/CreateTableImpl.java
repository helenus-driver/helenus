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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import org.helenus.driver.Clause;
import org.helenus.driver.CreateTable;
import org.helenus.driver.StatementBridge;
import org.helenus.driver.VoidFuture;
import org.helenus.driver.WithOptions;
import org.helenus.driver.persistence.Ordering;

/**
 * The <code>CreateTableImpl</code> class defines a CREATE TABLE statement.
 *
 * @copyright 2015-2016 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 19, 2015 - paouelle - Creation
 *
 * @param <T> The type of POJO associated with this statement.
 *
 * @since 1.0
 */
public class CreateTableImpl<T>
  extends GroupStatementImpl<Void, VoidFuture, T>
  implements CreateTable<T> {
  /**
   * List of tables to be created.
   *
   * @author paouelle
   */
  protected final List<TableInfoImpl<T>> tables = new ArrayList<>(8);

  /**
   * Holds the "WITH" options.
   *
   * @author paouelle
   */
  private final OptionsImpl<T> with;

  /**
   * Flag indicating if the "IF NOT EXIST" option has been selected.
   *
   * @author paouelle
   */
  private volatile boolean ifNotExists;

  /**
   * Holds the where statement part.
   *
   * @author paouelle
   */
  private final WhereImpl<T> where;

  /**
   * Instantiates a new <code>CreateTableImpl</code> object.
   *
   * @author paouelle
   *
   * @param context the non-<code>null</code> class info context for the POJO
   *        associated with this statement
   * @param mgr the non-<code>null</code> statement manager
   * @param bridge the non-<code>null</code> statement bridge
   */
  public CreateTableImpl(
    ClassInfoImpl<T>.Context context,
    StatementManagerImpl mgr,
    StatementBridge bridge
  ) {
    this(context, null, mgr, bridge);
  }

  /**
   * Instantiates a new <code>CreateTableImpl</code> object.
   *
   * @author paouelle
   *
   * @param  context the non-<code>null</code> class info context for the POJO
   *         associated with this statement
   * @param  tables the tables to create
   * @param  mgr the non-<code>null</code> statement manager
   * @param  bridge the non-<code>null</code> statement bridge
   * @throws IllegalArgumentException if any of the specified tables are not
   *         defined in the POJO
   */
  @SuppressWarnings({"unchecked", "cast", "rawtypes"})
  public CreateTableImpl(
    ClassInfoImpl<T>.Context context,
    String[] tables,
    StatementManagerImpl mgr,
    StatementBridge bridge
  ) {
    super(Void.class, context, mgr, bridge);
    ClassInfoImpl<?> cinfo = context.getClassInfo();

    if (cinfo instanceof TypeClassInfoImpl) {
      // fallback to root entity to create the proper table
      cinfo = ((TypeClassInfoImpl<?>)cinfo).getRoot();
    }
    if (tables != null) {
      for (final String table: tables) {
        if (table != null) {
          this.tables.add((TableInfoImpl<T>)cinfo.getTable(table)); // will throw IAE
        } // else - skip
      }
    } else { // fallback to all
      this.tables.addAll(
        (Collection<TableInfoImpl<T>>)(Collection)cinfo.getTablesImpl()
      );
    }
    this.with = new OptionsImpl<>(this);
    this.where = new WhereImpl<>(this);
  }

  /**
   * Builds query strings for the specified table.
   *
   * @author paouelle
   *
   * @param  table the non-<code>null</code> table for which to build a query
   *         string
   * @return the string builders used to build the query strings for
   *         the specified table or <code>null</code> if there is none for the
   *         specified table
   * @throws IllegalArgumentException if the keyspace has not yet been computed
   *         and cannot be computed with the provided keyspace keys yet or if
   *         assignments reference columns not defined in the POJO or invalid
   *         values or if missing mandatory columns are referenced for the
   *         specified table
   */
  @SuppressWarnings("synthetic-access")
  protected StringBuilder[] buildQueryStrings(TableInfoImpl<T> table) {
    final List<String> columns = new ArrayList<>(table.getColumns().size());
    final List<String> pkeys = new ArrayList<>(table.getPartitionKeys().size());
    final Map<String, Ordering> ckeys = new LinkedHashMap<>(table.getClusteringKeys().size() * 3 / 2);

    for (final FieldInfoImpl<?> field: table.getColumnsImpl()) {
      String cql = field.getColumnName() + " " + field.getDataType().toCQL();

      if (field.isStatic()) {
        cql += " STATIC";
      }
      columns.add(cql);
      if (field.isMultiKey()) {
        // we need to add a new column to represent a single value from the set
        // to be the clustering key in addition to the field's column
        columns.add(
          StatementImpl.MK_PREFIX
          + field.getColumnName()
          + " "
          + field.getDataType().getFirstArgumentType().toCQL()
        );
      } else if (field.isCaseInsensitiveKey()) {
        // we need to add a new column to represent the lower case value of
        // the key in addition to the field's column
        columns.add(
          StatementImpl.CI_PREFIX
          + field.getColumnName()
          + " "
          + field.getDataType().toCQL()
        );
      }
    }
    for (final FieldInfoImpl<?> field: table.getPartitionKeys()) {
      if (field.isMultiKey()) {
        // we need to add the special new column to represent a single value from the set
        // as the partition key instead of the annotated one
        pkeys.add(StatementImpl.MK_PREFIX + field.getColumnName());
      } else if (field.isCaseInsensitiveKey()) {
        // we need to add the special new column to represent the lower case value
        // of the partition key instead of the annotated one
        pkeys.add(StatementImpl.CI_PREFIX + field.getColumnName());
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
      } else if (field.isCaseInsensitiveKey()) {
        // we need to add the special new column to represent the lower case value
        // of the clustering key instead of the annotated one
        ckeys.put(
          StatementImpl.CI_PREFIX + field.getColumnName(),
          field.getClusteringKey().order()
        );
      } else {
        ckeys.put(field.getColumnName(), field.getClusteringKey().order());
      }
    }
    final List<String> keys = new ArrayList<>(ckeys.size() + 1);

    if (pkeys.size() > 1) {
      keys.add("(" + StringUtils.join(pkeys, ",") + ")");
    } else {
      keys.add(pkeys.get(0));
    }
    keys.addAll(ckeys.keySet());
    columns.add("PRIMARY KEY (" + StringUtils.join(keys, ",") + ")");
    final StringBuilder builder = new StringBuilder();

    builder.append("CREATE TABLE ");
    if (ifNotExists) {
      builder.append("IF NOT EXISTS ");
    }
    if (getKeyspace() != null) {
      Utils.appendName(builder, getKeyspace()).append('.');
    }
    Utils.appendName(builder, table.getName());
    builder
      .append(" (")
      .append(StringUtils.join(columns, ","))
      .append(')');
    boolean withAdded = false; // until proven otherwise

    if (!ckeys.isEmpty()) {
      final List<String> order = new ArrayList<>(ckeys.size());

      for (final Map.Entry<String, Ordering> e: ckeys.entrySet()) {
        order.add(e.getKey() + " " + e.getValue().CQL);
      }
      builder
        .append(" WITH CLUSTERING ORDER BY (")
        .append(StringUtils.join(order, ","))
        .append(")");
      withAdded = true;
    }
    if (!with.options.isEmpty() ) {
      builder.append(withAdded ? " AND " : " WITH ");
      Utils.joinAndAppend(
        getKeyspace(), table, null, mgr.getCodecRegistry(), builder, " AND ", with.options, null
      );
    }
    builder.append(';');
    return new StringBuilder[] { builder };
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.GroupStatementImpl#buildGroupedStatements()
   */
  @Override
  protected final List<StatementImpl<?, ?, ?>> buildGroupedStatements() {
    return tables.stream()
      .map(t -> buildQueryStrings(t))
      .filter(bs -> bs != null)
      .flatMap(bs -> Arrays.stream(bs))
      .filter(b -> (b != null) && (b.length() != 0))
      .map(b -> init(new SimpleStatementImpl(b.toString(), mgr, bridge)))
      .collect(Collectors.toList());
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
    builder.append(" CREATE TABLE");
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.CreateTable#ifNotExists()
   */
  @Override
  public CreateTable<T> ifNotExists() {
    this.ifNotExists = true;
    setDirty();
    return this;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.CreateTable#with(org.helenus.driver.WithOptions)
   */
  @Override
  public Options<T> with(WithOptions option) {
    return with.and(option);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.CreateTable#with()
   */
  @Override
  public Options<T> with() {
    return with;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.CreateTable#where(org.helenus.driver.Clause)
   */
  @Override
  public Where<T> where(Clause clause) {
    return where.and(clause);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.CreateTable#where()
   */
  @Override
  public Where<T> where() {
    return where;
  }

  /**
   * The <code>OptionsImpl</code> class defines the options of an CREATE
   * TABLE statement.
   *
   * @copyright 2015-2016 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 19, 2015 - paouelle - Creation
   *
   * @param <T> The type of POJO associated with the statement.
   *
   * @since 1.0
   */
  public static class OptionsImpl<T>
    extends ForwardingStatementImpl<Void, VoidFuture, T, CreateTableImpl<T>>
    implements Options<T> {
    /**
     * Holds options for this statement.
     *
     * @author paouelle
     */
    private final List<WithOptionsImpl> options = new ArrayList<>(8);

    /**
     * Instantiates a new <code>OptionsImpl</code> object.
     *
     * @author paouelle
     *
     * @param statement the non-<code>null</code> statement for which we are
     *        creating options
     */
    OptionsImpl(CreateTableImpl<T> statement) {
      super(statement);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.CreateTable.Options#and(org.helenus.driver.WithOptions)
     */
    @Override
    public Options<T> and(WithOptions option) {
      org.apache.commons.lang3.Validate.notNull(option, "invalid null with");
      org.apache.commons.lang3.Validate.isTrue(
        option instanceof WithOptionsImpl,
        "unsupported class of withs: %s",
        option.getClass().getName()
      );
      options.add((WithOptionsImpl)option);
      setDirty();
      return this;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.CreateTable.Options#where(org.helenus.driver.Clause)
     */
    @SuppressWarnings("synthetic-access")
    @Override
    public Where<T> where(Clause clause) {
      return statement.where.and(clause);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.CreateTable.Options#where()
     */
    @SuppressWarnings("synthetic-access")
    @Override
    public org.helenus.driver.CreateTable.Where<T> where() {
      return statement.where;
    }
  }

  /**
   * The <code>WhereImpl</code> class defines a WHERE clause for the CREATE
   * TABLE statement which can be used to specify keyspace keys used for the
   * keyspace name.
   *
   * @copyright 2015-2016 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 19, 2015 - paouelle - Creation
   *
   * @param <T> The type of POJO associated with the statement.
   *
   * @since 1.0
   */
  public static class WhereImpl<T>
    extends ForwardingStatementImpl<Void, VoidFuture, T, CreateTableImpl<T>>
    implements Where<T> {
    /**
     * Instantiates a new <code>WhereImpl</code> object.
     *
     * @author paouelle
     *
     * @param statement the encapsulated statement
     */
    WhereImpl(CreateTableImpl<T> statement) {
      super(statement);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.CreateTable.Where#and(org.helenus.driver.Clause)
     */
    @Override
    public Where<T> and(Clause clause) {
      org.apache.commons.lang3.Validate.notNull(clause, "invalid null clause");
      org.apache.commons.lang3.Validate.isTrue(
        clause instanceof ClauseImpl,
        "unsupported class of clauses: %s",
        clause.getClass().getName()
      );
      org.apache.commons.lang3.Validate.isTrue(
        !(clause instanceof ClauseImpl.DelayedWithObject),
        "unsupported clause '%s' for a CREATE TABLE statement",
        clause
      );
      final ClassInfoImpl<T>.Context context = getContext();
      final ClassInfoImpl<T> cinfo = context.getClassInfo();

      if (clause instanceof ClauseImpl.Delayed) {
        for (final Clause c: ((ClauseImpl.Delayed)clause).processWith(cinfo)) {
          and(c); // recurse to add the processed clause
        }
      } else {
        org.apache.commons.lang3.Validate.isTrue(
          clause instanceof Clause.Equality,
          "unsupported class of clauses: %s",
          clause.getClass().getName()
        );
        final ClauseImpl c = (ClauseImpl)clause;

        if (c instanceof ClauseImpl.CompoundEqClauseImpl) {
          final ClauseImpl.Compound cc = (ClauseImpl.Compound)c;
          final List<String> names = cc.getColumnNames();
          final List<?> values = cc.getColumnValues();

          for (int i = 0; i < names.size(); i++) {
            context.addKeyspaceKey(names.get(i), values.get(i));
          }
        } else {
          context.addKeyspaceKey(c.getColumnName().toString(), c.firstValue());
        }
        setDirty();
      }
      return this;
    }
  }
}

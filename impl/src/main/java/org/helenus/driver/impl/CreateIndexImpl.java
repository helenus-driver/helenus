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
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import org.helenus.driver.Clause;
import org.helenus.driver.ColumnPersistenceException;
import org.helenus.driver.CreateIndex;
import org.helenus.driver.StatementBridge;
import org.helenus.driver.VoidFuture;
import org.helenus.driver.info.ClassInfo;
import org.helenus.driver.persistence.Index;

/**
 * The <code>CreateIndexImpl</code> class defines a CREATE INDEX statement.
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
public class CreateIndexImpl<T>
  extends GroupStatementImpl<Void, VoidFuture, T>
  implements CreateIndex<T> {
  /**
   * Holds the custom class to use for the index.
   *
   * @author paouelle
   */
  private final String customClass;

  /**
   * List of tables to be created.
   *
   * @author paouelle
   */
  private final List<TableInfoImpl<T>> tables = new ArrayList<>(8);

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
   * Instantiates a new <code>CreateIndexImpl</code> object.
   *
   * @author paouelle
   *
   * @param  context the non-<code>null</code> class info context for the POJO
   *         associated with this statement
   * @param  customClass the custom class for the index
   * @param  tables the tables to create
   * @param  mgr the non-<code>null</code> statement manager
   * @param  bridge the non-<code>null</code> statement bridge
   * @throws IllegalArgumentException if any of the specified tables are not
   *         defined in the POJO
   */
  @SuppressWarnings({"unchecked", "cast", "rawtypes"})
  public CreateIndexImpl(
    ClassInfoImpl<T>.Context context,
    String customClass,
    String[] tables,
    StatementManagerImpl mgr,
    StatementBridge bridge
  ) {
    super(Void.class, context, mgr, bridge);
    this.customClass = customClass;
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
    this.where = new WhereImpl<>(this);
  }

  /**
   * Creates the given index for the specified column.
   *
   * @author paouelle
   *
   * @param  field the non-<code>null</code> field for which to build an index
   *         query string
   * @param  table the non-<code>null</code> table for which to build an index
   *         query string
   * @return the the string builder used to build the index query string
   */
  private StringBuilder buildIndexQueryString(
    FieldInfoImpl<T> field, TableInfoImpl<T> table
  ) {
    final StringBuilder builder = new StringBuilder(80);
    final Index index = field.getIndex();
    final String iname = index.name();
    final String customClass = (
      (this.customClass != null) ? this.customClass : index.customClass()
    );

    builder.append("CREATE ");
    if (!StringUtils.isEmpty(customClass)) {
      builder.append("CUSTOM ");
    }
    if (ifNotExists) {
      builder.append("INDEX IF NOT EXISTS");
    }
    if (!StringUtils.isEmpty(iname)) {
      builder.append(' ').append(iname);
    }
    builder.append(" ON ");
    if (getKeyspace() != null) {
      Utils.appendName(getKeyspace(), builder).append('.');
    }
    Utils.appendName(table.getName(), builder);
    builder
      .append(" (")
      .append(field.getColumnName())
      .append(')');
    if (!StringUtils.isEmpty(customClass)) {
      builder.append(" USING ").append(customClass);
    }
    builder.append(';');
    return builder;
  }

  /**
   * Builds query strings for the specified table.
   *
   * @author paouelle
   *
   * @param  table the non-<code>null</code> table for which to build a query
   *         string
   * @return the string builders used to build the query string for the specified
   *         table or <code>null</code> if there is none for the specified table
   * @throws IllegalArgumentException if the keyspace has not yet been computed
   *         and cannot be computed with the provided keyspace keys yet or if
   *         assignments reference columns not defined in the POJO or invalid
   *         values or if missing mandatory columns are referenced for the
   *         specified table
   * @throws ColumnPersistenceException if unable to persist a column's value
   */
  List<StringBuilder> buildQueryStrings(TableInfoImpl<T> table) {
    final List<StringBuilder> builders = new ArrayList<>(2);

    // process the indexes for this table
    for (final FieldInfoImpl<T> field: table.getIndexes()) {
      builders.add(buildIndexQueryString(field, table));
    }
    return builders;
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
    builder.append(" CREATE INDEX");
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.GroupStatementImpl#buildGroupedStatements()
   */
  @Override
  protected List<StatementImpl<?, ?, ?>> buildGroupedStatements() {
    return tables.stream()
      .map(t -> buildQueryStrings(t))
      .filter(bs -> bs != null)
      .flatMap(bs -> bs.stream())
      .filter(b -> (b != null) && (b.length() != 0))
      .map(b -> init(new SimpleStatementImpl(b.toString(), mgr, bridge)))
      .collect(Collectors.toList());
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.CreateIndex#ifNotExists()
   */
  @Override
  public CreateIndex<T> ifNotExists() {
    this.ifNotExists = true;
    setDirty();
    return this;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.CreateIndex#where(org.helenus.driver.Clause)
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
   * @see org.helenus.driver.CreateIndex#where()
   */
  @Override
  public Where<T> where() {
    return where;
  }

  /**
   * The <code>BuilderImpl</code> class defines an in-construction CREATE INDEX
   * statement.
   *
   * @copyright 2015-2015 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 19, 2015 - paouelle - Creation
   *
   * @param <T> The type of POJO associated with the statement.
   *
   * @since 1.0
   */
  public static class BuilderImpl<T> implements Builder<T> {
    /**
     * Holds the statement manager.
     *
     * @author paouelle
     */
    private final StatementManagerImpl mgr;

    /**
     * Holds the statement bridge.
     *
     * @author paouelle
     */
    private final StatementBridge bridge;

    /**
     * Holds the context associated with the POJO class for this statement.
     *
     * @author paouelle
     */
    private final ClassInfoImpl<T>.Context context;

    /**
     * Holds the custom class for the index
     *
     * @author paouelle
     */
    private volatile String customClass;

    /**
     * Instantiates a new <code>BuilderImpl</code> object.
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
    public BuilderImpl(
      ClassInfoImpl<T>.Context context,
      StatementManagerImpl mgr,
      StatementBridge bridge
    ) {
      this.mgr = mgr;
      this.bridge = bridge;
      this.context = context;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.CreateIndex.Builder#getObjectClass()
     */
    @Override
    public Class<T> getObjectClass() {
      return context.getObjectClass();
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.CreateIndex.Builder#getClassInfo()
     */
    @Override
    public ClassInfo<T> getClassInfo() {
      return context.getClassInfo();
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.CreateIndex.Builder#usingClass(java.lang.String)
     */
    @Override
    public Builder<T> usingClass(String customClass) {
      this.customClass = customClass;
      return this;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.CreateIndex.Builder#on(java.lang.String[])
     */
    @Override
    public CreateIndex<T> on(String... tables) {
      return new CreateIndexImpl<>(
        context,
        customClass,
        tables,
        mgr,
        bridge
      );
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.CreateIndex.Builder#onAll()
     */
    @Override
    public CreateIndex<T> onAll() {
      return new CreateIndexImpl<>(
        context,
        customClass,
        null,
        mgr,
        bridge
      );
    }
  }

  /**
   * The <code>WhereImpl</code> class defines a WHERE clause for the CREATE
   * INDEX statement which can be used to specify keyspace keys used for the
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
    extends ForwardingStatementImpl<Void, VoidFuture, T, CreateIndexImpl<T>>
    implements Where<T> {
    /**
     * Instantiates a new <code>WhereImpl</code> object.
     *
     * @author paouelle
     *
     * @param statement the encapsulated statement
     */
    WhereImpl(CreateIndexImpl<T> statement) {
      super(statement);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.CreateIndex.Where#and(org.helenus.driver.Clause)
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
        "unsupported clause '%s' for a CREATE INDEX statement",
        clause
      );
      if (clause instanceof ClauseImpl.Delayed) {
        for (final Clause c: ((ClauseImpl.Delayed)clause).processWith(statement.getContext().getClassInfo())) {
          and(c); // recurse to add the processed clause
        }
      } else {
        final ClauseImpl c = (ClauseImpl)clause;

        org.apache.commons.lang3.Validate.isTrue(
          clause instanceof Clause.Equality,
          "unsupported class of clauses: %s",
          clause.getClass().getName()
        );
        statement.getContext().addKeyspaceKey(c.getColumnName().toString(), c.firstValue());
        setDirty();
      }
      return this;
    }
  }
}

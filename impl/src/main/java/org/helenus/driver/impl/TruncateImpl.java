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
import java.util.List;

import org.helenus.driver.Clause;
import org.helenus.driver.StatementBridge;
import org.helenus.driver.Truncate;
import org.helenus.driver.VoidFuture;

/**
 * The <code>TruncateImpl</code> class defines a TRUNCATE statement.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 19, 2015 - paouelle - Creation
 *
 * @param <T> The type of POJO associated with this statement.
 *
 * @since 1.0
 */
public class TruncateImpl<T>
  extends SequenceStatementImpl<Void, VoidFuture, T>
  implements Truncate<T> {
  /**
   * List of tables to be truncated.
   *
   * @author paouelle
   */
  private final List<TableInfoImpl<T>> tables = new ArrayList<>(8);

  /**
   * Holds the where statement part.
   *
   * @author paouelle
   */
  private final WhereImpl<T> where;

  /**
   * Instantiates a new <code>TruncateImpl</code> object.
   *
   * @author paouelle
   *
   * @param context the non-<code>null</code> class info context for the POJO
   *        associated with this statement
   * @param mgr the non-<code>null</code> statement manager
   * @param bridge the non-<code>null</code> statement bridge
   */
  public TruncateImpl(
    ClassInfoImpl<T>.Context context,
    StatementManagerImpl mgr,
    StatementBridge bridge
  ) {
    this(context, null, mgr, bridge);
  }

  /**
   * Instantiates a new <code>TruncateImpl</code> object.
   *
   * @author paouelle
   *
   * @param  context the non-<code>null</code> class info context for the POJO
   *         associated with this statement
   * @param  tables the tables to truncate
   * @param  mgr the non-<code>null</code> statement manager
   * @param  bridge the non-<code>null</code> statement bridge
   * @throws IllegalArgumentException if any of the specified tables are not
   *         defined in the POJO
   */
  public TruncateImpl(
    ClassInfoImpl<T>.Context context,
    String[] tables,
    StatementManagerImpl mgr,
    StatementBridge bridge
  ) {
    super(Void.class, context, mgr, bridge);
    if (tables != null) {
      for (final String table: tables) {
        if (table != null) {
          this.tables.add((TableInfoImpl<T>)context.getClassInfo().getTable(table)); // will throw IAE
        } // else - skip
      }
    } else { // fallback to all
      this.tables.addAll(context.getClassInfo().getTablesImpl());
    }
    this.where = new WhereImpl<>(this);
  }

  /**
   * Builds a query string for the specified table.
   *
   * @author paouelle
   *
   * @param  table the non-<code>null</code> table for which to build a query
   *         string
   * @return the string builder used to build the query string for the specified
   *         table or <code>null</code> if there is none for the specified table
   * @throws IllegalArgumentException if the keyspace has not yet been computed
   *         and cannot be computed with the provided suffixes yet
   */
  StringBuilder buildQueryString(TableInfoImpl<T> table) {
    final StringBuilder builder = new StringBuilder();

    builder.append("TRUNCATE ");
    try {
      if (getKeyspace() != null) {
        Utils.appendName(getKeyspace(), builder).append(".");
      }
    } catch (ExcludedSuffixKeyException e) { // just skip this one since we were asked to skip the current keyspace suffix
      return null;
    }
    Utils.appendName(table.getName(), builder);
    builder.append(';');
    return builder;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.StatementImpl#buildQueryStrings()
   */
  @Override
  protected StringBuilder[] buildQueryStrings() {
    if (!isEnabled()) {
      return null;
    }
    final List<StringBuilder> builders = new ArrayList<>(tables.size());

    for (final TableInfoImpl<T> table: tables) {
      final StringBuilder builder = buildQueryString(table);

      if (builder != null) {
        builders.add(builder);
      }
    }
    if (builders.isEmpty()) {
      return null;
    }
    return builders.toArray(new StringBuilder[builders.size()]);
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
    builder.append(" TRUNCATE");
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
   * The <code>WhereImpl</code> class defines a WHERE clause for the TRUNCATE
   * statement which can be used to specify suffix keys used for the
   * keyspace name.
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
  public static class WhereImpl<T>
    extends ForwardingStatementImpl<Void, VoidFuture, T, TruncateImpl<T>>
    implements Where<T> {
    /**
     * Instantiates a new <code>WhereImpl</code> object.
     *
     * @author paouelle
     *
     * @param statement the encapsulated statement
     */
    WhereImpl(TruncateImpl<T> statement) {
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
        "unsupported clause '%s' for a TRUCNATE statement",
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
        try {
          statement.getContext().addSuffix(c.getColumnName().toString(), c.firstValue());
          setDirty();
        } catch (ExcludedSuffixKeyException e) { // ignore and continue without clause
        }
      }
      return this;
    }
  }
}

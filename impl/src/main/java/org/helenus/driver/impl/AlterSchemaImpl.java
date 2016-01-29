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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.datastax.driver.core.Row;

import org.helenus.driver.AlterSchema;
import org.helenus.driver.Clause;
import org.helenus.driver.ExcludedSuffixKeyException;
import org.helenus.driver.GroupableStatement;
import org.helenus.driver.StatementBridge;
import org.helenus.driver.VoidFuture;
import org.helenus.driver.info.ClassInfo;
import org.helenus.driver.persistence.Keyspace;
import org.helenus.driver.persistence.Table;

/**
 * The <code>AlterSchemaImpl</code> class defines a ALTER SCHEMA statement.
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
public class AlterSchemaImpl<T>
  extends GroupStatementImpl<Void, VoidFuture, T>
  implements AlterSchema<T> {
  /**
   * Holds the where statement part.
   *
   * @author paouelle
   */
  private final WhereImpl<T> where;

  /**
   * Instantiates a new <code>AlterSchemaImpl</code> object.
   *
   * @author paouelle
   *
   * @param context the non-<code>null</code> class info context for the POJO
   *        associated with this statement
   * @param mgr the non-<code>null</code> statement manager
   * @param bridge the non-<code>null</code> statement bridge
   */
  public AlterSchemaImpl(
    ClassInfoImpl<T>.Context context,
    StatementManagerImpl mgr,
    StatementBridge bridge
  ) {
    super(Void.class, context, mgr, bridge);
    this.where = new WhereImpl<>(this);
  }

  /**
   * Gets all underlying sequenced statements for this create schemas with the
   * ability to optionally tracked and skipped already created keyspaces and/or
   * tables.
   *
   * @author paouelle
   *
   * @param  keyspaces an optional set of keyspaces already created when
   *         used as part of a create schemas statement or <code>null</code>
   * @param  tables an optional map of tables already for given keyspaces created
   *         when used as part of a create schemas statement or <code>null</code>
   * @param  group a group where to place all insert statements for initial
   *         objects or <code>null</code> if a separate one should be created
   *         and returned as part of the list
   * @return a non-<code>null</code> list of all underlying statements from this
   *         statement
   */
  List<StatementImpl<?, ?, ?>> buildGroupedStatements(
    Set<Keyspace> keyspaces, Map<Keyspace, Set<Table>> tables, GroupImpl group
  ) {
    if (!isEnabled()) {
      return Collections.emptyList();
    }
    // make sure we have a valid keyspace (handling suffix exclusions if any)
    try {
      getKeyspace();
    } catch (ExcludedSuffixKeyException e) { // skip it
      return Collections.emptyList();
    }
    final List<StatementImpl<?, ?, ?>> statements = new ArrayList<>(32);
    final Keyspace keyspace = getContext().getClassInfo().getKeyspace();

    // start by generating the keyspace
    // --- do not attempt to create or alter the same keyspace twice when a set
    // --- of keyspaces is provided in the method calls (used by alter schemas)
    if ((keyspaces == null) || !keyspaces.contains(keyspace)) {
      final AlterCreateKeyspaceImpl<T> ak = init(new AlterCreateKeyspaceImpl<>(
        getContext(), mgr, bridge
      ));

      statements.add(ak);
      if (keyspaces != null) { // keep track of created keyspaces if requested
        keyspaces.add(keyspace);
      }
    }
    if (getClassInfo().supportsTablesAndIndexes()) {
      boolean alterTable = true;

      if (tables != null) {
        alterTable = false; // until proven otherwise
        for (final TableInfoImpl<T> table: getContext().getClassInfo().getTablesImpl()) {
          Set<Table> stables = tables.get(keyspace);

          if (stables == null) {
            stables = new HashSet<>(8);
            tables.put(keyspace,  stables);
          }
          if (stables.add(table.getTable())) {
            alterTable = true;
          }
        }
      }
      if (alterTable) {
        final AlterCreateTableImpl<T> at = init(
          new AlterCreateTableImpl<>(getContext(), mgr, bridge)
        );

        statements.add(at);
        // for indexes, we blindly drop all of them and re-create them
        for (final Map.Entry<String, List<Row>> e: at.getTableInfos().entrySet()) {
          e.getValue().stream()
            .map(r -> r.getString(4))
            .filter(i -> i != null)
            .distinct()
            .forEach(i -> {
              final StringBuilder builder = new StringBuilder("DROP INDEX ");

              if (getKeyspace() != null) {
                Utils.appendName(getKeyspace(), builder).append('.');
              }
              Utils.appendName(i, builder);
              builder.append(';');
              statements.add(new SimpleStatementImpl(builder.toString(), mgr, bridge));
            });
        }
        statements.add(
          init(new CreateIndexImpl<>(getContext(), null, null, mgr, bridge))
        );
      }
    } else {
      statements.add(init(new AlterCreateTypeImpl<>(getContext(), mgr, bridge)));
    }
    // finish with initial objects
    final Collection<T> objs = getContext().getInitialObjects();

    if (!objs.isEmpty()) {
      final GroupImpl g;

      if (group != null) {
        g = group;
      } else {
        g = init(new GroupImpl(
          Optional.empty(), new GroupableStatement<?, ?>[0], mgr, bridge
        ));
      }

      for (final T io: objs) {
        g.add(init(new InsertImpl<>(
          getContext().getClassInfo().newContext(io),
          (String[])null,
          mgr,
          bridge
        )));
      }
      if (group == null) {
        statements.add(g);
      }
    }
    return statements;
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
    builder.append(" ALTER");
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.SequenceStatementImpl#appendGroupType(java.lang.StringBuilder)
   */
  @Override
  protected void appendGroupType(StringBuilder builder) {
    builder.append("SCHEMA");
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
    return buildGroupedStatements(null, null, null);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.CreateSchema#getObjectClasses()
   */
  @Override
  public Set<Class<?>> getObjectClasses() {
    return getContext().getClassInfo().objectClasses()
      .collect(Collectors.toCollection(LinkedHashSet::new));
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.CreateSchema#getClassInfos()
   */
  @Override
  public Set<ClassInfo<?>> getClassInfos() {
    return getContext().getClassInfo().classInfos()
      .collect(Collectors.toCollection(LinkedHashSet::new));
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.CreateSchema#where(org.helenus.driver.Clause)
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
   * @see org.helenus.driver.CreateSchema#where()
   */
  @Override
  public Where<T> where() {
    return where;
  }

  /**
   * The <code>WhereImpl</code> class defines a WHERE clause for the CREATE
   * SCHEMA statement which can be used to specify suffix keys used for the
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
    extends ForwardingStatementImpl<Void, VoidFuture, T, AlterSchemaImpl<T>>
    implements Where<T> {
    /**
     * Instantiates a new <code>WhereImpl</code> object.
     *
     * @author paouelle
     *
     * @param statement the encapsulated statement
     */
    WhereImpl(AlterSchemaImpl<T> statement) {
      super(statement);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.AlterSchema.Where#and(org.helenus.driver.Clause)
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
        "unsupported clause '%s' for a CREATE SCHEMA statement",
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
        statement.getContext().addSuffix(c.getColumnName().toString(), c.firstValue());
        setDirty();
      }
      return this;
    }
  }
}

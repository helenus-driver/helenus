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

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;

import com.datastax.driver.core.Row;

import org.helenus.driver.AlterSchema;
import org.helenus.driver.Clause;
import org.helenus.driver.ExcludedKeyspaceKeyException;
import org.helenus.driver.GroupableStatement;
import org.helenus.driver.SequenceableStatement;
import org.helenus.driver.StatementBridge;
import org.helenus.driver.VoidFuture;
import org.helenus.driver.info.ClassInfo;
import org.helenus.driver.persistence.Keyspace;
import org.helenus.driver.persistence.Table;

/**
 * The <code>AlterSchemaImpl</code> class defines a ALTER SCHEMA statement.
 *
 * @copyright 2015-2016 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Apr 1, 2015 - paouelle - Creation
 *
 * @param <T> The type of POJO associated with this statement.
 *
 * @since 1.0
 */
public class AlterSchemaImpl<T>
  extends SequenceStatementImpl<Void, VoidFuture, T>
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
   * Gets all underlying sequenced statements for this alter schemas with the
   * ability to optionally tracked and skipped already created keyspaces and/or
   * tables.
   *
   * @author paouelle
   *
   * @param  keyspaces an optional set of keyspaces already created when
   *         used as part of a create schemas statement or <code>null</code>
   * @param  tables an optional map of tables already for given keyspaces created
   *         when used as part of a create schemas statement or <code>null</code>
   * @param kgroup a group where to place all create keyspace statements
   *        recursively expanded
   * @param tgroup a group where to place all create table statements
   *        recursively expanded
   *        one should be created and returned as part of the list
   * @param igroup a group where to place all create index statements
   *        recursively expanded
   *        one should be created and returned as part of the list
   * @param yseq a sequence where to place all create type statements
   *        recursively expanded
   *        one should be created and returned as part of the list
   * @param group a group where to place all insert statements for initial
   *        objects
   */
  protected void buildSequencedStatements(
    Set<Pair<String, Keyspace>> keyspaces,
    Map<Pair<String, Keyspace>, Set<Table>> tables,
    GroupImpl kgroup,
    GroupImpl tgroup,
    GroupImpl igroup,
    SequenceImpl yseq,
    GroupImpl group
  ) {
    if (!isEnabled()) {
      return;
    }
    // make sure we have a valid keyspace (handling keyspace key exclusions if any)
    final String ks;

    try {
      ks = getKeyspace();
    } catch (ExcludedKeyspaceKeyException e) { // skip it
      return;
    }
    final Keyspace keyspace = getContext().getClassInfo().getKeyspace();
    final Pair<String, Keyspace> pk = Pair.of(ks, keyspace);

    // start by generating the keyspace
    // --- do not attempt to create or alter the same keyspace twice when a set
    // --- of keyspaces is provided in the method calls (used by alter schemas)
    if ((keyspaces == null) || !keyspaces.contains(keyspace)) {
      final AlterCreateKeyspaceImpl<T> ak = init(new AlterCreateKeyspaceImpl<>(
        getContext(), mgr, bridge
      ));

      kgroup.add(ak);
      if (keyspaces != null) { // keep track of created keyspaces if requested
        keyspaces.add(pk);
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
            tables.put(pk, stables);
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
        at.buildGroupedStatements().forEach(s -> tgroup.addInternal(s));
        // for indexes, we blindly drop all of them and re-create them
        for (final Map.Entry<String, List<Row>> e: at.getTableInfos().entrySet()) {
          e.getValue().stream()
            .map(r -> r.getString(4))
            .filter(i -> i != null)
            .distinct()
            .forEach(i -> {
              final StringBuilder builder = new StringBuilder("DROP INDEX ");

              if (getKeyspace() != null) {
                Utils.appendName(builder, getKeyspace()).append('.');
              }
              Utils.appendName(builder, i);
              builder.append(';');
              // add to the table group
              tgroup.add(new SimpleStatementImpl(builder.toString(), mgr, bridge));
            });
        }
        igroup.add(
          init(new CreateIndexImpl<>(getContext(), null, null, mgr, bridge))
        );
      }
    } else {
      final AlterCreateTypeImpl<T> at = init(
        new AlterCreateTypeImpl<>(getContext(), mgr, bridge)
      );
      yseq.add(at);
    }
    // we do not do initial objects when altering since that can potentially override changes done to the DB since schema was created
//    // finish with initial objects
//    final Collection<T> objs = getContext().getInitialObjects();
//
//    if (!objs.isEmpty()) {
//      for (final T io: objs) {
//        group.add(init(new InsertImpl<>(
//          getContext().getClassInfo().newContext(io),
//          (String[])null,
//          mgr,
//          bridge
//        )));
//      }
//    }
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
   * @see org.helenus.driver.impl.SequenceStatementImpl#buildSequencedStatements()
   */
  @Override
  protected List<StatementImpl<?, ?, ?>> buildSequencedStatements() {
    final GroupImpl kgroup = init(new GroupImpl(
      Optional.empty(), new GroupableStatement<?, ?>[0], mgr, bridge
    ));
    final GroupImpl tgroup = init(new GroupImpl(
      Optional.empty(), new GroupableStatement<?, ?>[0], mgr, bridge
    ));
    final GroupImpl igroup = init(new GroupImpl(
      Optional.empty(), new GroupableStatement<?, ?>[0], mgr, bridge
    ));
    final SequenceImpl yseq = init(new SequenceImpl(
      Optional.empty(), new SequenceableStatement<?, ?>[0], mgr, bridge
    ));
    final GroupImpl group = init(new GroupImpl(
      Optional.empty(), new GroupableStatement<?, ?>[0], mgr, bridge
    ));

    buildSequencedStatements(null, null, kgroup, tgroup, igroup, yseq, group);
    return Stream.of(kgroup, yseq, tgroup, igroup, group)
      .filter(g -> !g.isEmpty())
      .collect(Collectors.toList());
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
   * SCHEMA statement which can be used to specify keyspace keys used for the
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
        statement.getContext().addKeyspaceKey(c.getColumnName().toString(), c.firstValue());
        setDirty();
      }
      return this;
    }
  }
}

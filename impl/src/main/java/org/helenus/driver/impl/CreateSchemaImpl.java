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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.helenus.driver.Clause;
import org.helenus.driver.CreateSchema;
import org.helenus.driver.ExcludedSuffixKeyException;
import org.helenus.driver.GroupableStatement;
import org.helenus.driver.StatementBridge;
import org.helenus.driver.VoidFuture;
import org.helenus.driver.info.ClassInfo;
import org.helenus.driver.persistence.Keyspace;
import org.helenus.driver.persistence.Table;

/**
 * The <code>CreateSchemaImpl</code> class defines a CREATE SCHEMA statement.
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
public class CreateSchemaImpl<T>
  extends SequenceStatementImpl<Void, VoidFuture, T>
  implements CreateSchema<T> {
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
   * Instantiates a new <code>CreateSchemaImpl</code> object.
   *
   * @author paouelle
   *
   * @param context the non-<code>null</code> class info context for the POJO
   *        associated with this statement
   * @param mgr the non-<code>null</code> statement manager
   * @param bridge the non-<code>null</code> statement bridge
   */
  public CreateSchemaImpl(
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
  List<StatementImpl<?, ?, ?>> buildSequencedStatements(
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
    // --- do not attempt to create the same keyspace twice when a set of keyspaces
    // --- is provided in the method calls (used by create schemas)
    if ((keyspaces == null) || !keyspaces.contains(keyspace)) {
      final CreateKeyspaceImpl<T> ck = init(new CreateKeyspaceImpl<>(
        getContext(), mgr, bridge
      ));

      if (ifNotExists) {
        ck.ifNotExists();
      }
      statements.add(ck);
      if (keyspaces != null) { // keep track of created keyspaces if requested
        keyspaces.add(keyspace);
      }
    }
    if (getClassInfo().supportsTablesAndIndexes()) {
      boolean createTable = true;

      if (tables != null) {
        createTable = false; // until proven otherwise
        for (final TableInfoImpl<T> table: getContext().getClassInfo().getTablesImpl()) {
          Set<Table> stables = tables.get(keyspace);

          if (stables == null) {
            stables = new HashSet<>(8);
            tables.put(keyspace,  stables);
          }
          if (stables.add(table.getTable())) {
            createTable = true;
          }
        }
      }
      if (createTable) {
        final CreateTableImpl<T> ct = init(new CreateTableImpl<>(getContext(), null, mgr, bridge));
        final CreateIndexImpl<T> ci = init(new CreateIndexImpl<>(getContext(), null, null, mgr, bridge));

        if (ifNotExists) {
          ct.ifNotExists();
          ci.ifNotExists();
        }
        statements.add(ct);
        statements.add(ci);
      }
    } else {
      final CreateTypeImpl<T> ct = init(new CreateTypeImpl<>(getContext(), mgr, bridge));

      if (ifNotExists) {
        ct.ifNotExists();
      }
      statements.add(ct);
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
    builder.append(" CREATE");
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
    return buildSequencedStatements(null, null, null);
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
      .collect(Collectors.toSet());
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
      .collect(Collectors.toSet());
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.CreateSchema#ifNotExists()
   */
  @Override
  public CreateSchema<T> ifNotExists() {
    this.ifNotExists = true;
    setDirty();
    return this;
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
    extends ForwardingStatementImpl<Void, VoidFuture, T, CreateSchemaImpl<T>>
    implements Where<T> {
    /**
     * Instantiates a new <code>WhereImpl</code> object.
     *
     * @author paouelle
     *
     * @param statement the encapsulated statement
     */
    WhereImpl(CreateSchemaImpl<T> statement) {
      super(statement);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.CreateSchema.Where#and(org.helenus.driver.Clause)
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

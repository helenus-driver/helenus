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
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.helenus.driver.BatchableStatement;
import org.helenus.driver.Clause;
import org.helenus.driver.CreateSchema;
import org.helenus.driver.StatementBridge;
import org.helenus.driver.VoidFuture;
import org.helenus.driver.info.ClassInfo;
import org.helenus.driver.persistence.Keyspace;

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
   * Builds the query strings for this create schemas with the ability to
   * optionally tracked and skipped already created keyspaces.
   *
   * @author paouelle
   *
   * @param  keyspaces an optional set of keyspaces already created when
   *         used as part of a create schemas statement or <code>null</code>
   * @return the string builders used to build the query strings or
   *         <code>null</code> if nothing to be done
   */
  StringBuilder[] buildQueryStrings(Set<Keyspace> keyspaces) {
    if (!isEnabled()) {
      return null;
    }
    final List<StringBuilder> builders
      = new ArrayList<>(getContext().getClassInfo().getTables().size() + 2);
    final CreateTypeImpl<T> cy;
    final CreateTableImpl<T> ct;
    final CreateIndexImpl<T> ci;

    // make sure we have a valid keyspace (handling suffix exclusions if any)
    try {
      getKeyspace();
    } catch (ExcludedSuffixKeyException e) { // skip it
      return null;
    }
    if (getClassInfo().supportsTablesAndIndexes()) {
      cy = null;
      ct = new CreateTableImpl<>(getContext(), null, mgr, bridge);
      ci = new CreateIndexImpl<>(getContext(), null, null, mgr, bridge);
      if (isTracing()) {
        ct.enableTracing();
        ci.enableTracing();
      } else {
        ct.disableTracing();
        ci.disableTracing();
      }
    } else {
      cy = new CreateTypeImpl<>(getContext(), mgr, bridge);
      ct = null;
      ci = null;
      if (isTracing()) {
        cy.enableTracing();
      } else {
        cy.disableTracing();
      }
    }
    final Keyspace keyspace = getContext().getClassInfo().getKeyspace();
    StringBuilder[] cbuilders;

    if (ifNotExists) {
      if (cy != null) {
        cy.ifNotExists();
      }
      if (ct != null) {
        ct.ifNotExists();
      }
      if (ci != null) {
        ci.ifNotExists();
      }
    }
    // start by generating the keyspace
    // --- do not attempt to create the same keyspace twice when a set of keyspaces
    // --- is provided in the method calls (used by create schemas)
    if ((keyspaces == null) || !keyspaces.contains(keyspace)) {
      final CreateKeyspaceImpl<T> ck = new CreateKeyspaceImpl<>(
        getContext(), mgr, bridge
      );

      if (ifNotExists) {
        ck.ifNotExists();
      }
      if (isTracing()) {
        ck.enableTracing();
      } else {
        ck.disableTracing();
      }
      cbuilders = ck.buildQueryStrings();
      if (cbuilders != null) {
        for (final StringBuilder builder: cbuilders) {
          if (builder != null) {
            builders.add(builder);
          }
        }
      }
      if (keyspaces != null) { // keep track of created keyspaces if requested
        keyspaces.add(keyspace);
      }
    }
    if (cy != null) { // now deal with types
      cbuilders = cy.buildQueryStrings();
      if (cbuilders != null) {
        for (final StringBuilder builder: cbuilders) {
          if (builder != null) {
            builders.add(builder);
          }
        }
      }
    }
    if (ct != null) { // now deal with tables
      cbuilders = ct.buildQueryStrings();
      if (cbuilders != null) {
        for (final StringBuilder builder: cbuilders) {
          if (builder != null) {
            builders.add(builder);
          }
        }
      }
    }
    if (ci != null) { // now deal with indexes
      cbuilders = ci.buildQueryStrings();
      if (cbuilders != null) {
        for (final StringBuilder builder: cbuilders) {
          if (builder != null) {
            builders.add(builder);
          }
        }
      }
    }
    // finish with initial objects
    final BatchImpl batch = new BatchImpl(
      Optional.empty(), new BatchableStatement[0], true, mgr, bridge
    );

    if (isTracing()) {
      batch.enableTracing();
    } else {
      batch.disableTracing();
    }
    for (final T io: getContext().getInitialObjects()) {
      final InsertImpl<T> insert = new InsertImpl<>(
        getContext().getClassInfo().newContext(io),
        null,
        mgr,
        bridge
      );

      if (isTracing()) {
        insert.enableTracing();
      } else {
        insert.disableTracing();
      }
      batch.add(insert);
    }
    final StringBuilder builder = batch.buildQueryString();

    if (builder != null) {
      builders.add(builder);
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
   * @see org.helenus.driver.impl.StatementImpl#buildQueryStrings()
   */
  @Override
  protected StringBuilder[] buildQueryStrings() {
    return buildQueryStrings(null);
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

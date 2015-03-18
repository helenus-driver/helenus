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

import java.util.ArrayList;
import java.util.List;

import com.github.helenusdriver.driver.Clause;
import com.github.helenusdriver.driver.CreateKeyspace;
import com.github.helenusdriver.driver.KeyspaceWith;
import com.github.helenusdriver.driver.StatementBridge;
import com.github.helenusdriver.driver.VoidFuture;

/**
 * The <code>CreateKeyspaceImpl</code> class defines a CREATE KEYSPACE statement.
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
public class CreateKeyspaceImpl<T>
  extends StatementImpl<Void, VoidFuture, T>
  implements CreateKeyspace<T> {
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
   * Instantiates a new <code>CreateKeyspaceImpl</code> object.
   *
   * @author paouelle
   *
   * @param context the non-<code>null</code> class info context for the POJO
   *        associated with this statement
   * @param mgr the non-<code>null</code> statement manager
   * @param bridge the non-<code>null</code> statement bridge
   */
  public CreateKeyspaceImpl(
    ClassInfoImpl<T>.Context context,
    StatementManagerImpl mgr,
    StatementBridge bridge
  ) {
    super(Void.class, context, mgr, bridge);
    this.with = new OptionsImpl<>(this);
    this.where = new WhereImpl<>(this);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.impl.StatementImpl#buildQueryStrings()
   */
  @Override
  protected StringBuilder[] buildQueryStrings() {
    return new StringBuilder[] { buildQueryString() };
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.impl.StatementImpl#buildQueryString()
   */
  @SuppressWarnings("synthetic-access")
  @Override
  protected StringBuilder buildQueryString() {
    final StringBuilder builder = new StringBuilder();

    builder.append("CREATE KEYSPACE ");
    if (ifNotExists) {
      builder.append("IF NOT EXISTS ");
    }
    Utils.appendName(getKeyspace(), builder);
    KeyspaceWithImpl.ReplicationWithImpl replication = with.replication;

    if (replication == null) { // default to POJO's details
      replication = new KeyspaceWithImpl.ReplicationWithImpl(
        getContext().getClassInfo(), mgr
      );
    }
    final List<KeyspaceWithImpl> options = new ArrayList<>(with.options.size() + 1);

    options.add(replication);
    options.addAll(with.options);
    builder.append(" WITH ");
    Utils.joinAndAppend(null, builder, " AND ", options);
    builder
      .append(" AND DURABLE_WRITES = ")
      .append(getContext().getClassInfo().getKeyspace().durableWrites());
    return builder;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.CreateKeyspace#ifNotExists()
   */
  @Override
  public CreateKeyspace<T> ifNotExists() {
    this.ifNotExists = true;
    setDirty();
    return this;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.CreateKeyspace#with(com.github.helenusdriver.driver.KeyspaceWith)
   */
  @Override
  public Options<T> with(KeyspaceWith option) {
    return with.and(option);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.CreateKeyspace#with()
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
   * @see com.github.helenusdriver.driver.CreateKeyspace#where(com.github.helenusdriver.driver.Clause)
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
   * @see com.github.helenusdriver.driver.CreateKeyspace#where()
   */
  @Override
  public Where<T> where() {
    return where;
  }

  /**
   * The <code>OptionsImpl</code> class defines the options of a CREATE
   * KEYSPACE statement.
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
  public static class OptionsImpl<T>
    extends ForwardingStatementImpl<Void, VoidFuture, T, CreateKeyspaceImpl<T>>
    implements Options<T> {
    /**
     * Holds the "REPLICATION" option for this statement.
     *
     * @author paouelle
     */
    private KeyspaceWithImpl.ReplicationWithImpl replication;

    /**
     * Holds options for this statement.
     *
     * @author paouelle
     */
    private final List<KeyspaceWithImpl> options = new ArrayList<>(2);

    /**
     * Instantiates a new <code>OptionsImpl</code> object.
     *
     * @author paouelle
     *
     * @param statement the non-<code>null</code> statement for which we are
     *        creating options
     */
    OptionsImpl(CreateKeyspaceImpl<T> statement) {
      super(statement);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.github.helenusdriver.driver.CreateKeyspace.Options#and(com.github.helenusdriver.driver.KeyspaceWith)
     */
    @Override
    public Options<T> and(KeyspaceWith option) {
      org.apache.commons.lang3.Validate.notNull(option, "invalid null with");
      org.apache.commons.lang3.Validate.isTrue(
        option instanceof KeyspaceWithImpl,
        "unsupported class of withs: %s",
        option.getClass().getName()
      );
      if (option instanceof KeyspaceWithImpl.ReplicationWithImpl) {
        this.replication = (KeyspaceWithImpl.ReplicationWithImpl)option;
      } else {
        options.add((KeyspaceWithImpl)option);
      }
      setDirty();
      return this;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.github.helenusdriver.driver.CreateKeyspace.Options#where(com.github.helenusdriver.driver.Clause)
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
     * @see com.github.helenusdriver.driver.CreateKeyspace.Options#where()
     */
    @SuppressWarnings("synthetic-access")
    @Override
    public com.github.helenusdriver.driver.CreateKeyspace.Where<T> where() {
      return statement.where;
    }
  }

  /**
   * The <code>WhereImpl</code> class defines a WHERE clause for the CREATE
   * KEYSPACE statement which can be used to specify suffix keys used for the
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
    extends ForwardingStatementImpl<Void, VoidFuture, T, CreateKeyspaceImpl<T>>
    implements Where<T> {
    /**
     * Instantiates a new <code>WhereImpl</code> object.
     *
     * @author paouelle
     *
     * @param statement the encapsulated statement
     */
    WhereImpl(CreateKeyspaceImpl<T> statement) {
      super(statement);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.github.helenusdriver.driver.CreateKeyspace.Where#and(com.github.helenusdriver.driver.Clause)
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
        "unsupported clause '%s' for a CREATE KEYSPACE statement",
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

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

import com.datastax.driver.core.RegularStatement;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;

import com.github.helenusdriver.driver.Batch;
import com.github.helenusdriver.driver.BatchableStatement;
import com.github.helenusdriver.driver.StatementBridge;
import com.github.helenusdriver.driver.Using;
import com.github.helenusdriver.driver.VoidFuture;

/**
 * The <code>BatchImpl</code> class extends the functionality of Cassandra's
 * {@link com.datastax.driver.core.querybuilder.Batch} class to provide
 * support for POJOs.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 19, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
public class BatchImpl
  extends StatementImpl<Void, VoidFuture, Void>
  implements Batch {
  /**
   * Holds the statements to be batched together.
   *
   * @author paouelle
   */
  private final List<StatementImpl<?, ?, ?>> statements;

  /**
   * Flag indicating if batches were added to this batch.
   *
   * @author paouelle
   */
  private volatile boolean includesBatches = false;

  /**
   * Flag indicating if the batch is logged or not.
   *
   * @author paouelle
   */
  private final boolean logged;

  /**
   * Holds the "USING" options.
   *
   * @author paouelle
   */
  private final OptionsImpl usings;

  /**
   * Instantiates a new <code>BatchImpl</code> object.
   *
   * @author paouelle
   *
   * @param  statements the statements to batch
   * @param  logged <code>true</code> if the batch should be logged; <code>false</code>
   *         if it should be unlogged
   * @param  mgr the non-<code>null</code> statement manager
   * @param  bridge the non-<code>null</code> statement bridge
   * @throws NullPointerException if <code>statement</code> or any of the
   *         statements are <code>null</code>
   * @throws IllegalArgumentException if counter and non-counter operations
   *         are mixed or if any statement represents a "select" statement or a
   *         "batch" statement
   */
  public BatchImpl(
    BatchableStatement<?, ?>[] statements,
    boolean logged,
    StatementManagerImpl mgr,
    StatementBridge bridge
  ) {
    super(Void.class, (String)null, mgr, bridge);
    this.statements = new ArrayList<>(Math.max(statements.length, 8));
    this.logged = logged;
    this.usings = new OptionsImpl(this);
    for (final BatchableStatement<?, ?> statement: statements) {
      add(statement);
    }
  }

  /**
   * Instantiates a new <code>BatchImpl</code> object.
   *
   * @author paouelle
   *
   * @param  statements the statements to batch
   * @param  logged <code>true</code> if the batch should be logged; <code>false</code>
   *         if it should be unlogged
   * @param  mgr the non-<code>null</code> statement manager
   * @param  bridge the non-<code>null</code> statement bridge
   * @throws NullPointerException if <code>statement</code> or any of the
   *         statements are <code>null</code>
   * @throws IllegalArgumentException if counter and non-counter operations
   *         are mixed or if any statement represents a "select" statement or a
   *         "batch" statement
   */
  public BatchImpl(
    Iterable<BatchableStatement<?, ?>> statements,
    boolean logged,
    StatementManagerImpl mgr,
    StatementBridge bridge
  ) {
    super(Void.class, (String)null, mgr, bridge);
    this.statements = new ArrayList<>(32);
    this.logged = logged;
    this.usings = new OptionsImpl(this);
    for (final BatchableStatement<?, ?> statement: statements) {
      add(statement);
    }
  }

  /**
   * Instantiates a new <code>BatchImpl</code> object.
   *
   * @author paouelle
   *
   * @param b the non-<code>null</code> batch statement to duplicate
   */
  private BatchImpl(BatchImpl b) {
    super(Void.class, (String)null, b.mgr, b.bridge);
    this.statements = new ArrayList<>(b.statements);
    this.logged = b.logged;
    this.usings = new OptionsImpl(this, b.usings);
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
    final List<StringBuilder> builders = new ArrayList<>(statements.size());

    for (final StatementImpl<?, ?, ?> statement: statements) {
      // recurse into the batch statement in case it is also batching stuff
      final StringBuilder[] sbs = statement.buildQueryStrings();

      if (sbs != null) {
        for (final StringBuilder sb: sbs) {
          if (sb != null) {
            builders.add(sb);
          }
        }
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
   * @see com.github.helenusdriver.driver.impl.StatementImpl#appendGroupType(java.lang.StringBuilder)
   */
  @Override
  protected void appendGroupType(StringBuilder builder) {
    builder.append("BATCH");
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.impl.StatementImpl#appendGroupSubType(java.lang.StringBuilder)
   */
  @Override
  protected void appendGroupSubType(StringBuilder builder) {
    if (isCounterOp()) {
      builder.append(" COUNTER");
    } else if (!logged) {
      builder.append(" UNLOGGED");
    }
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.impl.StatementImpl#appendOptions(java.lang.StringBuilder)
   */
  @SuppressWarnings("synthetic-access")
  @Override
  protected void appendOptions(StringBuilder builder) {
    if (!usings.usings.isEmpty()) {
      builder.append(" USING ");
      Utils.joinAndAppend(null, builder, " AND ", usings.usings);
    }
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.impl.StatementImpl#setDirty(boolean)
   */
  @Override
  protected void setDirty(boolean recurse) {
    super.setDirty(recurse);
    if (recurse) {
      for (final StatementImpl<?,?, ?> s: statements) {
        s.setDirty(recurse);
      }
    }
  }

  /**
   * {@inheritDoc}
   * <p>
   * Gets the keyspace of the first statement in this batch.
   *
   * @author paouelle
   *
   * @return the keyspace from the first statement in this batch or
   *         <code>null</code> if the batch is empty
   *
   * @see com.github.helenusdriver.driver.impl.StatementImpl#getKeyspace()
   */
  @Override
  public String getKeyspace() {
    return statements.isEmpty() ? null : statements.get(0).getKeyspace();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.Batch#isEmpty()
   */
  @Override
  public boolean isEmpty() {
    return statements.isEmpty();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.Batch#hasReachedRecommendedSize()
   */
  @Override
  public boolean hasReachedRecommendedSize() {
    return size() >= Batch.RECOMMENDED_MAX;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.Batch#size()
   */
  @Override
  public int size() {
    // simple case - if no added batches then size if just the number we have added
    if (!includesBatches) {
      return statements.size();
    }
    int size = 0;

    for (final StatementImpl<?, ?, ?> s: statements) {
      if (s instanceof BatchImpl) {
        size += ((BatchImpl)s).size();
      } else {
        size++;
      }
    }
    return size;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.Batch#clear()
   */
  @Override
  public void clear() {
    statements.clear();
    this.includesBatches = false;
    setDirty();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.Batch#add(com.github.helenusdriver.driver.BatchableStatement)
   */
  @Override
  public <R, F extends ListenableFuture<R>> Batch add(
    BatchableStatement<R, F> statement
  ) {
    org.apache.commons.lang3.Validate.notNull(statement, "invalid null statement");
    org.apache.commons.lang3.Validate.isTrue(
      statement instanceof StatementImpl,
      "unsupported class of statements: %s",
      statement.getClass().getName()
    );
    @SuppressWarnings("unchecked")
    final StatementImpl<R, F, ?> s = (StatementImpl<R, F, ?>)statement;
    final boolean isCounterOp = s.isCounterOp();

    if (this.isCounterOp == null) {
      setCounterOp(isCounterOp);
    } else if (isCounterOp() != isCounterOp) {
      throw new IllegalArgumentException(
        "cannot mix counter operations and non-counter operations in a batch statement"
      );
    }
    // special validation for simple statements
    if (s instanceof SimpleStatementImpl) {
      final SimpleStatementImpl ss = (SimpleStatementImpl)s;

      org.apache.commons.lang3.Validate.isTrue(
        !ss.isSelect(),
        "select statements are not supported in batch statements"
      );
      org.apache.commons.lang3.Validate.isTrue(
        !ss.isBatch(),
        "batch simple statements are not supported in batch statements"
      );
    }
    if (s instanceof BatchImpl) {
      this.includesBatches = true;
    }
    this.statements.add(s);
    setDirty();
    return this;
  }

  /**
   * Adds a new raw Cassandra statement to this batch.
   *
   * @author paouelle
   *
   * @param  statement the new statement to add
   * @return this batch
   * @throws NullPointerException if <code>statement</code> is <code>null</code>
   * @throws IllegalArgumentException if counter and non-counter operations
   *         are mixed or if the statement represents a "select" statement or a
   *         "batch" statement
   */
  @Override
  public Batch add(com.datastax.driver.core.RegularStatement statement) {
    final SimpleStatementImpl s = new SimpleStatementImpl(statement, mgr, bridge);

    org.apache.commons.lang3.Validate.isTrue(
      !s.isSelect(),
      "select raw statements are not supported in batch statements"
    );
    org.apache.commons.lang3.Validate.isTrue(
      !s.isBatch(),
      "batch raw statements are not supported in batch statements"
    );
    return add(s);
  }

  /**
   * Adds a new options for this BATCH statement.
   *
   * @author paouelle
   *
   * @param  using the option to add
   * @return the options of this BATCH statement
   */
  @Override
  public Options using(Using using) {
    return usings.and(using);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.Batch#duplicate()
   */
  @Override
  public Batch duplicate() {
    return new BatchImpl(this);
  }

  /**
   * The <code>OptionsImpl</code> class defines the options of a BATCH statement.
   *
   * @copyright 2015-2015 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 19, 2015 - paouelle - Creation
   *
   * @since 1.0
   */
  public static class OptionsImpl
    extends ForwardingStatementImpl<Void, VoidFuture, Void, BatchImpl>
    implements Options {
    /**
     * Holds the list of "USINGS" options.
     *
     * @author paouelle
     */
    private final List<UsingImpl> usings = new ArrayList<>(5);

    /**
     * Instantiates a new <code>OptionsImpl</code> object.
     *
     * @author paouelle
     *
     * @param statement the non-<code>null</code> statement for which we are
     *        creating options
     */
    OptionsImpl(BatchImpl statement) {
      super(statement);
    }

    /**
     * Instantiates a new <code>OptionsImpl</code> object.
     *
     * @author paouelle
     *
     * @param statement the non-<code>null</code> statement for which we are
     *        creating options
     * @param os the non-<code>null</code> options to duplicate
     */
    OptionsImpl(BatchImpl statement, OptionsImpl os) {
      super(statement);
      usings.addAll(os.usings);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.github.helenusdriver.driver.Batch.Options#and(com.github.helenusdriver.driver.Using)
     */
    @Override
    public Options and(Using using) {
      org.apache.commons.lang3.Validate.notNull(using, "invalid null using");
      org.apache.commons.lang3.Validate.isTrue(
        using instanceof UsingImpl,
        "unsupported class of usings: %s",
        using.getClass().getName()
      );
      usings.add((UsingImpl)using);
      setDirty();
      return this;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.github.helenusdriver.driver.Batch.Options#add(com.github.helenusdriver.driver.BatchableStatement)
     */
    @Override
    public <R, F extends AbstractFuture<R>> Batch add(
      BatchableStatement<R, F> statement) {
      return this.statement.add(statement);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.github.helenusdriver.driver.Batch.Options#add(com.datastax.driver.core.RegularStatement)
     */
    @Override
    public Batch add(RegularStatement statement) {
      return this.statement.add(statement);
    }
  }
}

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
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.datastax.driver.core.RegularStatement;
import com.google.common.util.concurrent.ListenableFuture;

import org.helenus.driver.Batch;
import org.helenus.driver.BatchableStatement;
import org.helenus.driver.GenericStatement;
import org.helenus.driver.Group;
import org.helenus.driver.ObjectStatement;
import org.helenus.driver.ParentStatement;
import org.helenus.driver.Recorder;
import org.helenus.driver.StatementBridge;
import org.helenus.driver.Using;
import org.helenus.driver.VoidFuture;
import org.helenus.driver.info.ClassInfo;
import org.helenus.util.Inhibit;
import org.helenus.util.function.ERunnable;

/**
 * The <code>BatchImpl</code> class extends the functionality of Cassandra's
 * {@link com.datastax.driver.core.querybuilder.Batch} class to provide
 * support for POJOs.
 *
 * @copyright 2015-2016 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 19, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
public class BatchImpl
  extends StatementImpl<Void, VoidFuture, Void>
  implements Batch, ParentStatementImpl {
  /**
   * Holds the logger.
   *
   * @author paouelle
   */
  private final static Logger logger = LogManager.getFormatterLogger(BatchImpl.class);

  /**
   * Holds the statements to be batched together.
   *
   * @author paouelle
   */
  private final List<StatementImpl<?, ?, ?>> statements;

  /**
   * Holds the parent of this batch. This is the sequence or batch this batch
   * was last added to.
   *
   * @author paouelle
   */
  private volatile ParentStatementImpl parent = null;

  /**
   * Holds the registered recorder.
   *
   * @author paouelle
   */
  private final Optional<Recorder> recorder;

  /**
   * Holds the registered error handlers.
   *
   * @author paouelle
   */
  private final LinkedList<ERunnable<?>> errorHandlers;

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
   * Holds the cached batched statements.
   *
   * @author paouelle
   */
  private volatile List<StatementImpl<?, ?, ?>> cacheList = null;

  /**
   * Holds the cached query strings for the batched statements.
   *
   * @author paouelle
   */
  private volatile StringBuilder[] cacheSB = null;

  /**
   * Instantiates a new <code>BatchImpl</code> object.
   *
   * @author paouelle
   *
   * @param  recorder the optional recorder to register with this batch
   * @param  statements the statements to batch
   * @param  logged <code>true</code> if the batch should be logged; <code>false</code>
   *         if it should be unlogged
   * @param  mgr the non-<code>null</code> statement manager
   * @param  bridge the non-<code>null</code> statement bridge
   * @throws NullPointerException if <code>statement</code> or any of the
   *         statements are <code>null</code>
   * @throws IllegalArgumentException if counter and non-counter operations
   *         are mixed
   */
  public BatchImpl(
    Optional<Recorder> recorder,
    BatchableStatement<?, ?>[] statements,
    boolean logged,
    StatementManagerImpl mgr,
    StatementBridge bridge
  ) {
    super(Void.class, (String)null, mgr, bridge);
    this.statements = new ArrayList<>(Math.max(statements.length, 8));
    this.logged = logged;
    this.usings = new OptionsImpl(this);
    this.recorder = recorder;
    this.errorHandlers = new LinkedList<>();
    for (final BatchableStatement<?, ?> statement: statements) {
      add(statement);
    }
  }

  /**
   * Instantiates a new <code>BatchImpl</code> object.
   *
   * @author paouelle
   *
   * @param  recorder the optional recorder to register with this batch
   * @param  statements the statements to batch
   * @param  logged <code>true</code> if the batch should be logged; <code>false</code>
   *         if it should be unlogged
   * @param  mgr the non-<code>null</code> statement manager
   * @param  bridge the non-<code>null</code> statement bridge
   * @throws NullPointerException if <code>statement</code> or any of the
   *         statements are <code>null</code>
   * @throws IllegalArgumentException if counter and non-counter operations
   *         are mixed
   */
  public BatchImpl(
    Optional<Recorder> recorder,
    Iterable<BatchableStatement<?, ?>> statements,
    boolean logged,
    StatementManagerImpl mgr,
    StatementBridge bridge
  ) {
    super(Void.class, (String)null, mgr, bridge);
    this.statements = new ArrayList<>(32);
    this.logged = logged;
    this.usings = new OptionsImpl(this);
    this.recorder = recorder;
    this.errorHandlers = new LinkedList<>();
    for (final BatchableStatement<?, ?> statement: statements) {
      add(statement);
    }
  }

  /**
   * Instantiates a new <code>BatchImpl</code> object.
   *
   * @author paouelle
   *
   * @param recorder the optional recorder to register with this batch
   * @param b the non-<code>null</code> batch statement to duplicate
   */
  private BatchImpl(BatchImpl b, Optional<Recorder> recorder) {
    super(b);
    this.statements = new ArrayList<>(b.statements);
    this.logged = b.logged;
    this.recorder = recorder;
    this.errorHandlers = new LinkedList<>(b.errorHandlers);
    this.usings = new OptionsImpl(this, b.usings);
    this.cacheList = b.cacheList;
    this.cacheSB = b.cacheSB;
  }

  /**
   * Gets all underlying batched statements recursively in the proper order for
   * this statement.
   *
   * @author paouelle
   *
   * @return a non-<code>null</code> list of all underlying statements from this
   *         statement
   */
  private List<StatementImpl<?, ?, ?>> buildBatchedStatements() {
    return statements.stream()
      .flatMap(s -> (
        (s instanceof BatchImpl)
        ? ((BatchImpl)s).buildBatchedStatements().stream()
        : Stream.of(s)
      ))
      .collect(Collectors.toList());
  }

  /**
   * Adds the specified statement to this batch.
   *
   * @author paouelle
   *
   * @param  s the non-<code>null</code> statement to add to this batch
   * @return this sequence
   */
  @SuppressWarnings({"cast"})
  private Batch addInternal(StatementImpl<?, ?, ?> s) {
    // special validation for simple statements
    if (s instanceof SimpleStatementImpl) {
      final SimpleStatementImpl ss = (SimpleStatementImpl)s;

      org.apache.commons.lang3.Validate.isTrue(
        !ss.isSelect(),
        "select statements are not supported in batch statements"
      );
      org.apache.commons.lang3.Validate.isTrue(
        !ss.isBatch(),
        "batch raw statements are not supported in batch statements"
      );
    }
    final boolean isCounterOp = s.isCounterOp();

    if (this.isCounterOp == null) {
      setCounterOp(isCounterOp);
    } else if (isCounterOp() != isCounterOp) {
      throw new IllegalArgumentException(
        "cannot mix counter operations and non-counter operations in a batch statement"
      );
    }
    this.statements.add(s);
    // cast to Object required to compile on the cmdline :-(
    if (((Object)s) instanceof ParentStatementImpl) {
      final ParentStatementImpl ps = (ParentStatementImpl)(Object)s;

      ps.setParent(this); // set us as their parent going forward
      // now recurse all contained object statements for the parent and report them as recorded
      ps.objectStatements().forEach(cs -> recorded(cs, ps));
    } else if (s instanceof ObjectStatement) {
      recorded((ObjectStatement<?>)(Object)s, this);
    }
    setDirty();
    return this;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.StatementImpl#appendGroupType(java.lang.StringBuilder)
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
   * @see org.helenus.driver.impl.StatementImpl#appendGroupSubType(java.lang.StringBuilder)
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
   * @see org.helenus.driver.impl.StatementImpl#buildStatements()
   */
  @Override
  protected final List<StatementImpl<?, ?, ?>> buildStatements() {
    if (!isEnabled()) {
      this.cacheList = null;
      this.cacheSB = null;
      super.simpleSize = 0;
    } else if (isDirty() || (cacheList == null)) {
      this.cacheList = buildBatchedStatements();
      this.cacheSB = null;
      super.simpleSize = cacheList.stream()
        .mapToInt(StatementImpl::simpleSize)
        .sum();
    }
    return (cacheList != null) ? cacheList : Collections.emptyList();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.StatementImpl#simpleSize()
   */
  @Override
  protected int simpleSize() {
    buildStatements();
    return super.simpleSize;
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
    if (isDirty() || (cacheSB == null)) {
      final List<StatementImpl<?, ?, ?>> slist = buildStatements();

      if (cacheList == null) {
        this.cacheSB = null;
      } else {
        final List<StringBuilder> builders = new ArrayList<>(slist.size());

        for (final StatementImpl<?, ?, ?> statement: slist) {
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
        this.cacheSB = builders.toArray(new StringBuilder[builders.size()]);
      }
    }
    return ((cacheSB != null) && (cacheSB.length > 0)) ? cacheSB : null;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.StatementImpl#setDirty()
   */
  @Override
  protected void setDirty() {
    super.setDirty();
    this.cacheList = null;
    this.cacheSB = null;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.StatementImpl#setDirty(boolean)
   */
  @Override
  protected void setDirty(boolean recurse) {
    super.setDirty(recurse);
    if (recurse) {
      statements.forEach(s -> s.setDirty(recurse));
    }
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.StatementImpl#appendOptions(java.lang.StringBuilder)
   */
  @SuppressWarnings("synthetic-access")
  @Override
  protected void appendOptions(StringBuilder builder) {
    if (!usings.usings.isEmpty()) {
      builder.append(" USING ");
      Utils.joinAndAppend(null, null, mgr.getCodecRegistry(), builder, " AND ", usings.usings, null);
    }
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.ParentStatementImpl#setParent(org.helenus.driver.impl.ParentStatementImpl)
   */
  @Override
  public void setParent(ParentStatementImpl parent) {
    this.parent = parent;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.ParentStatementImpl#recorded(org.helenus.driver.ObjectStatement, org.helenus.driver.ParentStatement)
   */
  @Override
  public void recorded(ObjectStatement<?> statement, ParentStatement pstatement) {
    if (statement.getObject() == null) { // not associated with a single POJO so skip it
      return;
    }
    // start by notifying the registered recorder
    recorder.ifPresent(r ->  r.recorded(statement, pstatement));
    // now notify our parent if any
    final ParentStatementImpl p = parent;

    if (p != null) {
      p.recorded(statement, pstatement);
    }
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.ParentStatementImpl#objectStatements()
   */
  @SuppressWarnings("rawtypes")
  @Override
  public Stream<ObjectStatement<?>> objectStatements() {
    return statements.stream()
      .flatMap(s -> {
        final StatementImpl si = s; // required to make sure it compiles on cmdline

        if (si instanceof ParentStatementImpl) {
          return ((ParentStatementImpl)si).objectStatements();
        } else if (s instanceof ObjectStatement) {
          return Stream.of((ObjectStatement<?>)(ObjectStatement)s); // typecast is required for cmd line compilation
        } else {
          return Stream.empty();
        }
      });
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.ParentStatementImpl#statements()
   */
  @SuppressWarnings("rawtypes")
  @Override
  public Stream<GenericStatement<?, ?>> statements() {
    return Stream.concat(Stream.of(this), statements.stream()
      .flatMap(s -> {
        final StatementImpl si = s; // required to make sure it compiles on cmdline

        if (si instanceof ParentStatementImpl) {
          return ((ParentStatementImpl)si).statements();
        }
        return Stream.of(s);
      }));
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
   * @see org.helenus.driver.impl.StatementImpl#getKeyspace()
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
   * @see org.helenus.driver.ParentStatement#getRecorder()
   */
  @Override
  public Optional<Recorder> getRecorder() {
    final ParentStatementImpl p = parent;

    return Optional.ofNullable(
      recorder.orElseGet(() -> (p != null) ? p.getRecorder().orElse(null) : null)
    );
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.ParentStatement#isEmpty()
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
   * @see org.helenus.driver.Batch#hasReachedRecommendedSize()
   */
  @Override
  public boolean hasReachedRecommendedSize() {
    return simpleSize() >= Batch.RECOMMENDED_MAX;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.Batch#hasReachedRecommendedSizeFor(java.lang.Class)
   */
  @Override
  public boolean hasReachedRecommendedSizeFor(Class<?> clazz) {
    if (clazz == null) {
      return hasReachedRecommendedSize();
    }
    try {
      return hasReachedRecommendedSizeFor(mgr.getClassInfo(clazz));
    } catch (IllegalArgumentException e) { // fallback to what we can :-(
      return hasReachedRecommendedSize();
    }
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.Batch#hasReachedRecommendedSizeFor(org.helenus.driver.info.ClassInfo)
   */
  @Override
  public boolean hasReachedRecommendedSizeFor(ClassInfo<?> cinfo) {
    if (cinfo == null) {
      return hasReachedRecommendedSize();
    }
    final int free = Batch.RECOMMENDED_MAX - simpleSize();
    final int num_tables = cinfo.getNumTables(); // worst case for adding an insert or update or delete

    return free < ((num_tables == 0) ? 1 : num_tables);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.ParentStatement#size()
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
   * @see org.helenus.driver.ParentStatement#clear()
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
   * @see org.helenus.driver.Batch#add(org.helenus.driver.BatchableStatement)
   */
  @SuppressWarnings("unchecked")
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
    return addInternal((StatementImpl<R, F, ?>)statement);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.Batch#add(com.datastax.driver.core.RegularStatement)
   */
  @Override
  public Batch add(com.datastax.driver.core.RegularStatement statement) {
    org.apache.commons.lang3.Validate.notNull(statement, "invalid null statement");
    return addInternal(new SimpleStatementImpl(statement, mgr, bridge));
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.Batch#addErrorHandler(org.helenus.util.function.ERunnable)
   */
  @Override
  public Batch addErrorHandler(ERunnable<?> run) {
    org.apache.commons.lang3.Validate.notNull(run, "invalid null error handler");
    // add at the beginning of the list to ensure reverse order when running them
    errorHandlers.addFirst(run);
    return this;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.ParentStatement#runErrorHandlers()
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public void runErrorHandlers() {
    for (final ERunnable<?> run: errorHandlers) {
      Inhibit.throwables((ERunnable<Throwable>)run, t -> logger.catching(t));
    }
    // now recurse in contained batches that have been added
    for (final StatementImpl s: statements) {
      if (s instanceof Group) {
        ((Group)s).runErrorHandlers();
      }
    }
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
  public Options using(Using<?> using) {
    return usings.and(using);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.Insert#usings()
   */
  @SuppressWarnings({"rawtypes", "cast", "unchecked", "synthetic-access"})
  @Override
  public Stream<Using<?>> usings() {
    return (Stream<Using<?>>)(Stream)usings.usings.stream();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.Insert#getUsing(java.lang.String)
   */
  @SuppressWarnings({"rawtypes", "cast", "unchecked"})
  @Override
  public <U> Optional<Using<U>> getUsing(String name) {
    return (Optional<Using<U>>)(Optional)usings()
      .filter(u -> u.getName().equals(name)).findAny();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.Batch#duplicate()
   */
  @Override
  public Batch duplicate() {
    return init(new BatchImpl(this, Optional.empty()));
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.Batch#duplicate(org.helenus.driver.Recorder)
   */
  @Override
  public Batch duplicate(Recorder recorder) {
    return init(new BatchImpl(this, Optional.of(recorder)));
  }

  /**
   * The <code>OptionsImpl</code> class defines the options of a BATCH statement.
   *
   * @copyright 2015-2016 The Helenus Driver Project Authors
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
    private final List<UsingImpl<?>> usings = new ArrayList<>(5);

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
      os.usings.forEach(o -> usings.add(new UsingImpl<>(o, statement)));
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.Batch.Options#and(org.helenus.driver.Using)
     */
    @Override
    public Options and(Using<?> using) {
      org.apache.commons.lang3.Validate.notNull(using, "invalid null using");
      org.apache.commons.lang3.Validate.isTrue(
        using instanceof UsingImpl,
        "unsupported class of usings: %s",
        using.getClass().getName()
      );
      usings.add(((UsingImpl<?>)using).setStatement(statement));
      setDirty();
      return this;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.Batch.Options#add(org.helenus.driver.BatchableStatement)
     */
    @Override
    public <R, F extends ListenableFuture<R>> Batch add(
      BatchableStatement<R, F> statement
    ) {
      return this.statement.add(statement);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.Batch.Options#add(com.datastax.driver.core.RegularStatement)
     */
    @Override
    public Batch add(RegularStatement statement) {
      return this.statement.add(statement);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.Batch.Options#addErrorHandler(ERunnable)
     */
    @Override
    public Batch addErrorHandler(ERunnable<?> run) {
      return statement.addErrorHandler(run);
    }
  }
}

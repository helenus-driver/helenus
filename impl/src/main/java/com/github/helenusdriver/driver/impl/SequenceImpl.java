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
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.datastax.driver.core.RegularStatement;
import com.github.helenusdriver.driver.Batch;
import com.github.helenusdriver.driver.BatchableStatement;
import com.github.helenusdriver.driver.ObjectStatement;
import com.github.helenusdriver.driver.Recorder;
import com.github.helenusdriver.driver.RecordingStatement;
import com.github.helenusdriver.driver.Sequence;
import com.github.helenusdriver.driver.SequenceableStatement;
import com.github.helenusdriver.driver.StatementBridge;
import com.github.helenusdriver.driver.VoidFuture;
import com.github.helenusdriver.util.Inhibit;
import com.github.helenusdriver.util.function.ERunnable;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * The <code>SequenceImpl</code> class defines support for a statement that
 * can execute a set of statements in sequence returning the result set from
 * the last one only.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 19, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
public class SequenceImpl
  extends SequenceStatementImpl<Void, VoidFuture, Void>
  implements Sequence, ParentStatementImpl {
  /**
   * Holds the logger.
   *
   * @author paouelle
   */
  private final static Logger logger = LogManager.getFormatterLogger(SequenceImpl.class);

  /**
   * Holds the statements to be sequenced together.
   *
   * @author paouelle
   */
  private final List<StatementImpl<?, ?, ?>> statements;

  /**
   * Holds the parent of this sequence. This is the sequence this one was last
   * added to.
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
  private final List<ERunnable<?>> errorHandlers;

  /**
   * Instantiates a new <code>SequenceImpl</code> object.
   *
   * @author paouelle
   *
   * @param  recorder the optional recorder to register with this sequence
   * @param  statements the statements to sequence
   * @param  mgr the non-<code>null</code> statement manager
   * @param  bridge the non-<code>null</code> statement bridge
   * @throws NullPointerException if <code>statement</code> or any of the
   *         statements are <code>null</code>
   */
  public SequenceImpl(
    Optional<Recorder> recorder,
    SequenceableStatement<?, ?>[] statements,
    StatementManagerImpl mgr,
    StatementBridge bridge
  ) {
    super(Void.class, (String)null, mgr, bridge);
    this.statements = new ArrayList<>(Math.max(statements.length, 8));
    for (final SequenceableStatement<?, ?> statement: statements) {
      add(statement);
    }
    this.recorder = recorder;
    this.errorHandlers = new LinkedList<>();
  }

  /**
   * Instantiates a new <code>SequenceImpl</code> object.
   *
   * @author paouelle
   *
   * @param  recorder the optional recorder to register with this sequence
   * @param  statements the statements to sequence
   * @param  mgr the non-<code>null</code> statement manager
   * @param  bridge the non-<code>null</code> statement bridge
   * @throws NullPointerException if <code>statement</code> or any of the
   *         statements are <code>null</code>
   */
  public SequenceImpl(
    Optional<Recorder> recorder,
    Iterable<SequenceableStatement<?, ?>> statements,
    StatementManagerImpl mgr,
    StatementBridge bridge
  ) {
    super(Void.class, (String)null, mgr, bridge);
    this.statements = new ArrayList<>(32);
    for (final SequenceableStatement<?, ?> statement: statements) {
      add(statement);
    }
    this.recorder = recorder;
    this.errorHandlers = new LinkedList<>();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.impl.StatementImpl#buildQueryStrings()
   */
  @SuppressWarnings("cast")
  @Override
  protected StringBuilder[] buildQueryStrings() {
    final List<StringBuilder> builders = new ArrayList<>(statements.size());

    for (final StatementImpl<?, ?, ?> s: statements) {
      // paouelle: well without the cast to Object, this doesn't compile
      // on the cmd line but it does in eclipse (and works!!!!)
      // why couldn't we check instance of on anything????
      // error: inconvertible types
      //        [ERROR] required: com.github.helenusdriver.driver.Batch
      //        [ERROR] found:    com.github.helenusdriver.driver.impl.StatementImpl<capture#1 of ?,capture#2 of ?,capture#3 of ?>
      if (((Object)s) instanceof Batch) {
        // do not do batch using the batched statements as we still want
        // to deal with the batch statement as a single statement
        final StringBuilder sb = s.buildQueryString();

        if (sb != null) {
          builders.add(sb);
        }
      } else {
        final StringBuilder[] sbs = s.buildQueryStrings();

        if (sbs != null) {
          for (final StringBuilder sb: sbs) {
            if (sb != null) {
              builders.add(sb);
            }
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
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.impl.ParentStatementImpl#setParent(com.github.helenusdriver.driver.impl.ParentStatementImpl)
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
   * @see com.github.helenusdriver.driver.impl.ParentStatementImpl#recorded(com.github.helenusdriver.driver.ObjectStatement)
   */
  @Override
  public void recorded(ObjectStatement<?> statement) {
    // start by notifying the registered recorder
    recorder.ifPresent(
      r -> Inhibit.throwables(
        () -> r.recorded(statement), t -> logger.catching(t)
      )
    );
    // now notify our parent if any
    final ParentStatementImpl p = parent;

    Inhibit.throwables(
      () -> {
        if (p != null) {
          p.recorded(statement);
        }
      }, t -> logger.catching(t)
    );
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.impl.ParentStatementImpl#objectStatements()
   */
  @SuppressWarnings("rawtypes")
  @Override
  public Stream<ObjectStatement<?>> objectStatements() {
    return statements.stream()
      .flatMap(s -> {
        if (s instanceof ParentStatementImpl) {
          return ((ParentStatementImpl)s).objectStatements();
        } else if (s instanceof ObjectStatement) {
          return Stream.of((ObjectStatement<?>)(ObjectStatement)s); // typecast is required for cmd line compilation
        } else {
          return Stream.empty();
        }
      });
  }

  /**
   * {@inheritDoc}
   * <p>
   * Gets the keyspace of the first statement in this sequence.
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
   * @see com.github.helenusdriver.driver.RecordingStatement#getRecorder()
   */
  @Override
  public Optional<Recorder> getRecorder() {
    return recorder;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.RecordingStatement#isEmpty()
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
   * @see com.github.helenusdriver.driver.RecordingStatement#size()
   */
  @Override
  public int size() {
    return statements.size();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.RecordingStatement#clear()
   */
  @Override
  public void clear() {
    statements.clear();
    setDirty();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.Sequence#add(com.github.helenusdriver.driver.SequenceableStatement)
   */
  @SuppressWarnings("unchecked")
  @Override
  public <R, F extends ListenableFuture<R>> Sequence add(
    SequenceableStatement<R, F> statement
  ) {
    org.apache.commons.lang3.Validate.notNull(statement, "invalid null statement");
    org.apache.commons.lang3.Validate.isTrue(
      statement instanceof StatementImpl,
      "unsupported class of statements: %s",
      statement.getClass().getName()
    );
    this.statements.add((StatementImpl<R, F, ?>)statement);
    if (statement instanceof ParentStatementImpl) {
      final ParentStatementImpl ps = (ParentStatementImpl)statement;

      ps.setParent(this); // set us as their parent going forward
      // now recurse all contained object statements for the parent and report them as recorded
      ps.objectStatements().forEach(cs -> recorded(cs));
    } else if (statement instanceof ObjectStatement) {
      recorded((ObjectStatement<?>)statement);
    }
    setDirty();
    return this;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.RecordingStatement#add(com.github.helenusdriver.driver.BatchableStatement)
   */
  @SuppressWarnings("unchecked")
  @Override
  public <R, F extends ListenableFuture<R>> Sequence add(
    BatchableStatement<R, F> statement
  ) {
    return add((SequenceableStatement<R, F>)statement);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.RecordingStatement#add(com.datastax.driver.core.RegularStatement)
   */
  @Override
  public Sequence add(RegularStatement statement) {
    return add(new SimpleStatementImpl(statement, mgr, bridge));
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.RecordingStatement#addErrorHandler(com.github.helenusdriver.util.function.ERunnable)
   */
  @Override
  public Sequence addErrorHandler(ERunnable<?> run) {
    org.apache.commons.lang3.Validate.notNull(run, "invalid null error handler");
    errorHandlers.add(run);
    return this;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.RecordingStatement#runErrorHandlers()
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public void runErrorHandlers() {
    for (final ERunnable<?> run: errorHandlers) {
      Inhibit.throwables((ERunnable<Throwable>)run, t -> logger.catching(t));
    }
    // now recurse in contained sequences and batches that have been added
    statements.stream()
      .filter(s -> s instanceof RecordingStatement)
      .forEach(s -> ((RecordingStatement)s).runErrorHandlers());
  }
}

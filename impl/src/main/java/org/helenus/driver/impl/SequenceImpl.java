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
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
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
import org.helenus.driver.Recorder;
import org.helenus.driver.Sequence;
import org.helenus.driver.SequenceableStatement;
import org.helenus.driver.StatementBridge;
import org.helenus.driver.VoidFuture;
import org.helenus.util.Inhibit;
import org.helenus.util.function.ERunnable;

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
  private final LinkedList<ERunnable<?>> errorHandlers;

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
    this.recorder = recorder;
    this.errorHandlers = new LinkedList<>();
    for (final SequenceableStatement<?, ?> statement: statements) {
      add(statement);
    }
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
    this.recorder = recorder;
    this.errorHandlers = new LinkedList<>();
    for (final SequenceableStatement<?, ?> statement: statements) {
      add(statement);
    }
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.StatementImpl#buildQueryStrings()
   */
  @SuppressWarnings("cast")
  @Override
  protected StringBuilder[] buildQueryStrings() {
    if (!isEnabled()) {
      return null;
    }
    final List<StringBuilder> builders = new ArrayList<>(statements.size());

    for (final StatementImpl<?, ?, ?> s: statements) {
      // paouelle: well without the cast to Object, this doesn't compile
      // on the cmd line but it does in eclipse (and works!!!!)
      // why couldn't we check instance of on anything????
      // error: inconvertible types
      //        [ERROR] required: com.github.helenusdriver.driver.Batch
      //        [ERROR] found:    com.github.helenusdriver.driver.impl.StatementImpl<capture#1 of ?,capture#2 of ?,capture#3 of ?>
      if (((Object)s) instanceof Batch) {
        // do not do sequence using the individual batched statements as we
        // still want to deal with the batch statement as a single statement
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
   * @see org.helenus.driver.impl.StatementImpl#setDirty(boolean)
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
   * @see org.helenus.driver.impl.ParentStatementImpl#setParent(org.helenus.driver.impl.ParentStatementImpl)
   */
  @Override
  public void setParent(ParentStatementImpl parent) {
    this.parent = parent;
  }

  /**
   * {@inheritDoc}
   *
   * @author <a href="mailto:paouelle@enlightedinc.com">paouelle</a>
   *
   * @see org.helenus.driver.impl.ParentStatementImpl#recorded(org.helenus.driver.ObjectStatement, org.helenus.driver.Group)
   */
  @Override
  public void recorded(ObjectStatement<?> statement, Group group) {
    if (statement.getObject() == null) { // not associated with a single POJO so skip it
      return;
    }
    // start by notifying the registered recorder
    recorder.ifPresent(r -> r.recorded(statement, group));
    // now notify our parent if any
    final ParentStatementImpl p = parent;

    if (p != null) {
      p.recorded(statement, group);
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
   * Gets the keyspace of the first statement in this sequence.
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
   * @see org.helenus.driver.Group#getRecorder()
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
   * @see org.helenus.driver.Group#isEmpty()
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
   * @see org.helenus.driver.Group#size()
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
   * @see org.helenus.driver.Group#clear()
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
   * @see org.helenus.driver.Sequence#add(org.helenus.driver.SequenceableStatement)
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
      ps.objectStatements().forEach(cs -> recorded(cs, ps));
    } else if (statement instanceof ObjectStatement) {
      recorded((ObjectStatement<?>)statement, this);
    }
    setDirty();
    return this;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.Group#add(org.helenus.driver.BatchableStatement)
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
   * @see org.helenus.driver.Group#add(com.datastax.driver.core.RegularStatement)
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
   * @see org.helenus.driver.Group#addErrorHandler(org.helenus.util.function.ERunnable)
   */
  @Override
  public Sequence addErrorHandler(ERunnable<?> run) {
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
   * @see org.helenus.driver.Group#runErrorHandlers()
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public void runErrorHandlers() {
    for (final ERunnable<?> run: errorHandlers) {
      Inhibit.throwables((ERunnable<Throwable>)run, t -> logger.catching(t));
    }
    // now recurse in contained sequences and batches that have been added
    for (final StatementImpl s: statements) {
      if (s instanceof Group) {
        ((Group)s).runErrorHandlers();
      }
    }
  }
}

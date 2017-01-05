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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.datastax.driver.core.RegularStatement;
import com.google.common.util.concurrent.ListenableFuture;

import org.helenus.driver.BatchableStatement;
import org.helenus.driver.GenericStatement;
import org.helenus.driver.Group;
import org.helenus.driver.GroupableStatement;
import org.helenus.driver.ObjectStatement;
import org.helenus.driver.ParentStatement;
import org.helenus.driver.Recorder;
import org.helenus.driver.StatementBridge;
import org.helenus.driver.VoidFuture;
import org.helenus.util.Inhibit;
import org.helenus.util.function.ERunnable;

/**
 * The <code>GroupImpl</code> class defines support for a statement that can
 * execute a set of statements in parallel returning the result from the first
 * one that fails. Basically, a group will sends multiple requests to Cassandra
 * at the same time up to either its parallel factor or until it reaches a
 * sequence statement and then wait for all results before continuing to the
 * next set of statements to be sent in parallel.
 * <p>
 * The group's algorithm will allow the distributed nature of Cassandra to work
 * for you and distribute the writes to the optimal destination which leads to
 * not only fastest inserts/updates (assuming different partitions are being
 * updated), but it’ll cause the least load on the cluster.
 *
 * @copyright 2015-2016 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 20, 2016 - paouelle - Creation
 *
 * @since 1.0
 */
public class GroupImpl
  extends GroupStatementImpl<Void, VoidFuture, Void>
  implements Group, ParentStatementImpl {
  /**
   * Holds the logger.
   *
   * @author paouelle
   */
  private final static Logger logger = LogManager.getFormatterLogger(GroupImpl.class);

  /**
   * Holds the statements to be grouped together.
   *
   * @author paouelle
   */
  private final List<StatementImpl<?, ?, ?>> statements;

  /**
   * Holds the parent of this group. This is the sequence or group this one
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
   * Instantiates a new <code>GroupImpl</code> object.
   *
   * @author paouelle
   *
   * @param  recorder the optional recorder to register with this group
   * @param  statements the statements to group
   * @param  mgr the non-<code>null</code> statement manager
   * @param  bridge the non-<code>null</code> statement bridge
   * @throws NullPointerException if <code>statement</code> or any of the
   *         statements are <code>null</code>
   */
  public GroupImpl(
    Optional<Recorder> recorder,
    GroupableStatement<?, ?>[] statements,
    StatementManagerImpl mgr,
    StatementBridge bridge
  ) {
    super(Void.class, (String)null, mgr, bridge);
    this.statements = new ArrayList<>(Math.max(statements.length, 8));
    this.recorder = recorder;
    this.errorHandlers = new LinkedList<>();
    for (final GroupableStatement<?, ?> statement: statements) {
      add(statement);
    }
  }

  /**
   * Instantiates a new <code>GroupImpl</code> object.
   *
   * @author paouelle
   *
   * @param  recorder the optional recorder to register with this group
   * @param  statements the statements to group
   * @param  mgr the non-<code>null</code> statement manager
   * @param  bridge the non-<code>null</code> statement bridge
   * @throws NullPointerException if <code>statement</code> or any of the
   *         statements are <code>null</code>
   */
  public GroupImpl(
    Optional<Recorder> recorder,
    Iterable<GroupableStatement<?, ?>> statements,
    StatementManagerImpl mgr,
    StatementBridge bridge
  ) {
    super(Void.class, (String)null, mgr, bridge);
    this.statements = new ArrayList<>(32);
    this.recorder = recorder;
    this.errorHandlers = new LinkedList<>();
    for (final GroupableStatement<?, ?> statement: statements) {
      add(statement);
    }
  }

  /**
   * Adds the specified statement to this group.
   *
   * @author paouelle
   *
   * @param  s the non-<code>null</code> statement to add to this group
   * @return this group
   */
  @SuppressWarnings("cast")
  Group addInternal(StatementImpl<?, ?, ?> s) {
    // special validation for simple statements
    if (s instanceof SimpleStatementImpl) {
      final SimpleStatementImpl ss = (SimpleStatementImpl)s;

      org.apache.commons.lang3.Validate.isTrue(
        !ss.isSelect(),
        "select statements are not supported in group statements"
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
   * @see org.helenus.driver.impl.GroupStatementImpl#buildGroupedStatements()
   */
  @Override
  protected List<StatementImpl<?, ?, ?>> buildGroupedStatements() {
    return statements.stream()
      .flatMap(s -> (
        (s instanceof GroupImpl)
        ? ((GroupImpl)s).buildGroupedStatements().stream()
        : Stream.of(s)
      ))
      .collect(Collectors.toList());
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
    recorder.ifPresent(r -> r.recorded(statement, pstatement));
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
   * @see org.helenus.driver.ParentStatement#size()
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
   * @see org.helenus.driver.ParentStatement#clear()
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
   * @see org.helenus.driver.Group#add(org.helenus.driver.GroupableStatement)
   */
  @SuppressWarnings("unchecked")
  @Override
  public <R, F extends ListenableFuture<R>> Group add(
    GroupableStatement<R, F> statement
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
   * @see org.helenus.driver.Group#add(org.helenus.driver.BatchableStatement)
   */
  @SuppressWarnings("unchecked")
  @Override
  public <R, F extends ListenableFuture<R>> Group add(
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
   * @see org.helenus.driver.Group#add(com.datastax.driver.core.RegularStatement)
   */
  @Override
  public Group add(RegularStatement statement) {
    org.apache.commons.lang3.Validate.notNull(statement, "invalid null statement");
    return addInternal(new SimpleStatementImpl(statement, mgr, bridge));
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.Group#addErrorHandler(org.helenus.util.function.ERunnable)
   */
  @Override
  public Group addErrorHandler(ERunnable<?> run) {
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
    // now recurse in contained sequences and batches that have been added
    for (final StatementImpl s: statements) {
      if (s instanceof Group) {
        ((Group)s).runErrorHandlers();
      }
    }
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.GroupStatementImpl#setParallelFactor(int)
   */
  @SuppressWarnings("unchecked")
  @Override
  public Group setParallelFactor(int factor) {
    return super.setParallelFactor(factor);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.GroupStatementImpl#isIdempotent()
   */
  @Override
  public Boolean isIdempotent() {
    final Boolean idem = idempotent;

    if (idem != null) {
      return idem;
    }
    boolean nullFound = false;

    for (final StatementImpl<?, ?, ?> s: statements) {
      final Boolean iidem = s.isIdempotent();

      if (iidem == null) {
        nullFound = true;
      } else if (!iidem) {
        return false;
      }
    }
    return (nullFound ? null : true);
  }
}

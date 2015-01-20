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
package com.github.helenus.driver.impl;

import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.RegularStatement;
import com.google.common.util.concurrent.ListenableFuture;

import com.github.helenus.driver.Batch;
import com.github.helenus.driver.Sequence;
import com.github.helenus.driver.SequenceableStatement;
import com.github.helenus.driver.StatementBridge;
import com.github.helenus.driver.VoidFuture;

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
  implements Sequence {
  /**
   * Holds the statements to be sequenced together.
   *
   * @author paouelle
   */
  private final List<StatementImpl<?, ?, ?>> statements;

  /**
   * Instantiates a new <code>SequenceImpl</code> object.
   *
   * @author paouelle
   *
   * @param  statements the statements to sequence
   * @param  mgr the non-<code>null</code> statement manager
   * @param  bridge the non-<code>null</code> statement bridge
   * @throws NullPointerException if <code>statement</code> or any of the
   *         statements are <code>null</code>
   */
  public SequenceImpl(
    SequenceableStatement<?, ?>[] statements,
    StatementManagerImpl mgr,
    StatementBridge bridge
  ) {
    super(Void.class, (String)null, mgr, bridge);
    this.statements = new ArrayList<>(Math.max(statements.length, 8));
    for (final SequenceableStatement<?, ?> statement: statements) {
      add(statement);
    }
  }

  /**
   * Instantiates a new <code>SequenceImpl</code> object.
   *
   * @author paouelle
   *
   * @param  statements the statements to sequence
   * @param  mgr the non-<code>null</code> statement manager
   * @param  bridge the non-<code>null</code> statement bridge
   * @throws NullPointerException if <code>statement</code> or any of the
   *         statements are <code>null</code>
   */
  public SequenceImpl(
    Iterable<SequenceableStatement<?, ?>> statements,
    StatementManagerImpl mgr,
    StatementBridge bridge
  ) {
    super(Void.class, (String)null, mgr, bridge);
    this.statements = new ArrayList<>(32);
    for (final SequenceableStatement<?, ?> statement: statements) {
      add(statement);
    }
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenus.driver.impl.StatementImpl#buildQueryStrings()
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
      //        [ERROR] required: com.github.helenus.driver.Batch
      //        [ERROR] found:    com.github.helenus.driver.impl.StatementImpl<capture#1 of ?,capture#2 of ?,capture#3 of ?>
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
   * @see com.github.helenus.driver.impl.StatementImpl#setDirty(boolean)
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
   * Gets the keyspace of the first statement in this sequence.
   *
   * @author paouelle
   *
   * @return the keyspace from the first statement in this batch or
   *         <code>null</code> if the batch is empty
   *
   * @see com.github.helenus.driver.impl.StatementImpl#getKeyspace()
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
   * @see com.github.helenus.driver.Sequence#isEmpty()
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
   * @see com.github.helenus.driver.Sequence#size()
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
   * @see com.github.helenus.driver.Sequence#clear()
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
   * @see com.github.helenus.driver.Sequence#add(com.github.helenus.driver.SequenceableStatement)
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
    setDirty();
    return this;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenus.driver.Sequence#add(com.datastax.driver.core.RegularStatement)
   */
  @Override
  public Sequence add(RegularStatement statement) {
    return add(new SimpleStatementImpl(statement, mgr, bridge));
  }
}

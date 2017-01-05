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

import java.util.Collections;
import java.util.List;

import com.datastax.driver.core.EmptyResultSetFuture;
import com.datastax.driver.core.LastResultSequentialSetFuture;
import com.datastax.driver.core.ResultSetFuture;
import com.google.common.util.concurrent.ListenableFuture;

import org.helenus.driver.StatementBridge;

/**
 * The <code>SequenceStatementImpl</code> abstract class extends the {@link StatementImpl}
 * class to provide support for a statement that represents a sequence (not a
 * batch) of statements that must be executed one after the other.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 19, 2015 - paouelle - Creation
 *
 * @param <R> The type of result returned when executing this statement
 * @param <F> The type of future result returned when executing this statement
 * @param <T> The type of POJO associated with this statement
 *
 * @since 1.0
 */
public abstract class SequenceStatementImpl<R, F extends ListenableFuture<R>, T>
  extends StatementImpl<R, F, T> {
  /**
   * Holds the cached sequenced statements.
   *
   * @author paouelle
   */
  private volatile List<StatementImpl<?, ?, ?>> cacheList = null;

  /**
   * Holds the cached query strings for the sequenced statements.
   *
   * @author paouelle
   */
  private volatile StringBuilder[] cacheSB = null;

  /**
   * Instantiates a new <code>SequenceStatementImpl</code> object.
   *
   * @author paouelle
   *
   * @param  resultClass the result class from the execution of this statement
   * @param  context the class info context for the POJO associated with this
   *         statement or <code>null</code> if this statement is not associated
   *         with a POJO
   * @param  mgr the non-<code>null</code> statement manager
   * @param  bridge the non-<code>null</code> statement bridge
   * @throws NullPointerException if <code>resultClass</code> is <code>null</code>
   * @throws IllegalArgumentException if the result class is not a supported one
   */
  protected SequenceStatementImpl(
    Class<R> resultClass,
    ClassInfoImpl<T>.Context context,
    StatementManagerImpl mgr,
    StatementBridge bridge
  ) {
    super(resultClass, context, mgr, bridge);
  }

  /**
   * Instantiates a new <code>SequenceStatementImpl</code> object.
   *
   * @author paouelle
   *
   * @param  resultClass the result class from the execution of this statement
   * @param  keyspace the keyspace for this statement if known
   * @param  mgr the non-<code>null</code> statement manager
   * @param  bridge the non-<code>null</code> statement bridge
   * @throws NullPointerException if <code>resultClass</code> is <code>null</code>
   * @throws IllegalArgumentException if the result class is not a supported one
   */
  protected SequenceStatementImpl(
    Class<R> resultClass,
    String keyspace,
    StatementManagerImpl mgr,
    StatementBridge bridge
  ) {
    super(resultClass, keyspace, mgr, bridge);
  }

  /**
   * Instantiates a new <code>SequenceStatementImpl</code> object.
   *
   * @author paouelle
   *
   * @param sequence the non-<code>null</code> statement being copied or
   *        wrapped
   */
  protected SequenceStatementImpl(SequenceStatementImpl<R, F, T> sequence) {
    super(sequence);
    this.cacheList = sequence.cacheList;
    this.cacheSB = sequence.cacheSB;
  }

  /**
   * Gets all underlying sequenced statements recursively in the proper order
   * for this statement.
   *
   * @author paouelle
   *
   * @return a non-<code>null</code> list of all underlying statements from this
   *         statement
   */
  protected abstract List<StatementImpl<?, ?, ?>> buildSequencedStatements();

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.StatementImpl#appendGroupType(java.lang.StringBuilder)
   */
  @Override
  protected void appendGroupType(StringBuilder builder) {
    builder.append("SEQUENCE");
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
      this.cacheList = buildSequencedStatements();
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
  protected final int simpleSize() {
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
  protected final StringBuilder[] buildQueryStrings() {
    if (isDirty() || (cacheSB == null)) {
      final List<StatementImpl<?, ?, ?>> slist = buildStatements();

      if (cacheList == null) {
        this.cacheSB = null;
      } else {
        this.cacheSB = slist.stream()
          .map(StatementImpl::getQueryString)
          .filter(s -> s != null)
          .map(s -> {
            final StringBuilder sb = new StringBuilder(s);
            // Use the same test that String#trim() uses to determine
            // if a character is a whitespace character.
            int l = sb.length();

            while (l > 0 && sb.charAt(l - 1) <= ' ') {
              l -= 1;
            }
            if (l != sb.length()) {
              sb.setLength(l);
            }
            if (l == 0 || sb.charAt(l - 1) != ';') {
              sb.append(';');
            }
            return sb;
          })
          .toArray(StringBuilder[]::new);
      }
    }
    return cacheSB;
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
   * @see org.helenus.driver.impl.StatementImpl#executeAsyncRaw0()
   */
  @Override
  protected ResultSetFuture executeAsyncRaw0() {
    if (!isEnabled()) {
      return new EmptyResultSetFuture(mgr);
    }
    try {
      // we do not want to rely on query strings to determine the set of statements
      // to execute as some of those might actually be groups in which case, we
      // want to rely on its implementation to deal with the execution. In addition
      // each statements being executed might be customized differently when it comes
      // to the consistency and serial consistency levels and tracing and retry policies.
      // In this case, we want to keep their own defined values if overridden otherwise
      // we want to fallback to the values from this sequence statement
      final List<StatementImpl<?, ?, ?>> slist = buildStatements();

      if (slist.isEmpty()) { // nothing to query
        return mgr.sent(this, new EmptyResultSetFuture(mgr));
      } else if (slist.size() == 1) { // only one so execute it directly
        return mgr.sent(this, slist.get(0).executeAsyncRaw());
      }
      return mgr.sent(this, new LastResultSequentialSetFuture(slist, mgr));
    } finally {
      // let's recursively clear the query string that gets cache to reduce
      // the memory impact chance are now that it got executed, it won't be
      // needed anymore
      setDirty(true);
    }
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.StatementImpl#isIdempotent()
   */
  @Override
  public Boolean isIdempotent() {
    return false;
  }
}

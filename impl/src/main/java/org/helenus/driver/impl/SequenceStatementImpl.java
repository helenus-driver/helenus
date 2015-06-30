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

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import com.datastax.driver.core.EmptyResultSetFuture;
import com.datastax.driver.core.LastResultSetFuture;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.SimpleStatement;
import com.google.common.util.concurrent.AbstractFuture;

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
public abstract class SequenceStatementImpl<R, F extends AbstractFuture<R>, T>
  extends StatementImpl<R, F, T> {
  /**
   * Holds the cached query strings.
   *
   * @author paouelle
   */
  private volatile StringBuilder[] cache;

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
   * @param statement the non-<code>null</code> statement being copied or
   *        wrapped
   */
  protected SequenceStatementImpl(SequenceStatementImpl<R, F, T> statement) {
    super(statement);
    this.cache = statement.cache;
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
    builder.append("SEQUENCE");
  }

  /**
   * Gets the query strings for this sequence statement.
   *
   * @author paouelle
   *
   * @return the valid CQL query strings for this sequence statement or
   *         <code>null</code> if nothing to sequence
   * @throws IllegalArgumentException if the statement is associated with a POJO
   *         and the keyspace has not yet been computed and cannot be
   *         computed with the provided suffixes yet
   */
  protected StringBuilder[] getQueryStrings() {
    if (isDirty() || (cache == null)) {
      final StringBuilder[] sbs = buildQueryStrings();

      if (sbs == null) {
        this.cache = null;
      } else {
        this.cache = new StringBuilder[sbs.length];
        for (int i = 0; i < sbs.length; i++) {
          final StringBuilder sb = sbs[i];

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
          this.cache[i] = sb;
        }
      }
    }
    return cache;
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
    this.cache = null;
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
    try {
      final StringBuilder[] queries = getQueryStrings();

      if (ArrayUtils.isEmpty(queries)) { // nothing to query
        return new EmptyResultSetFuture(mgr);
      }
      final List<SimpleStatement> raws = new ArrayList<>(queries.length);

      for (final StringBuilder sb: queries) {
        if ((sb == null) || (sb.length() == 0)) { // nothing to query
          continue;
        }
        final String query = sb.toString();

        if (StringUtils.isEmpty(query)) { // nothing to query
          continue;
        }
        final SimpleStatement raw = new SimpleStatement(query);

        // propagate statement properties
        if (getConsistencyLevel() != null) {
          raw.setConsistencyLevel(getConsistencyLevel());
        }
        if (getSerialConsistencyLevel() != null) {
          raw.setSerialConsistencyLevel(getSerialConsistencyLevel());
        }
        if (isTracing()) {
          raw.enableTracing();
        } else {
          raw.disableTracing();
        }
        raw.setRetryPolicy(getRetryPolicy());
        raw.setFetchSize(getFetchSize());
        raws.add(raw);
      }
      return new LastResultSetFuture(raws, mgr);
    } finally {
      // let's recursively clear the query string that gets cache to reduce
      // the memory impact chance are now that it got executed, it won't be
      // needed anymore
      setDirty(true);
    }
  }
}

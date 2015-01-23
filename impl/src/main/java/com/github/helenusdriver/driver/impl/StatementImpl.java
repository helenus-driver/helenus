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

import java.util.concurrent.ExecutionException;

import org.apache.commons.lang3.StringUtils;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.EmptyResultSetFuture;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.DriverInternalError;
import com.datastax.driver.core.policies.RetryPolicy;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;

import com.github.helenusdriver.driver.Batch;
import com.github.helenusdriver.driver.GenericStatement;
import com.github.helenusdriver.driver.ObjectSet;
import com.github.helenusdriver.driver.ObjectSetFuture;
import com.github.helenusdriver.driver.Sequence;
import com.github.helenusdriver.driver.StatementBridge;
import com.github.helenusdriver.driver.VoidFuture;
import com.github.helenusdriver.driver.info.ClassInfo;

/**
 * The <code>StatementImpl</code> abstract class extends the functionality of
 * Cassandra's {@link com.datastax.driver.core.Statement} class to provide support
 * for POJOs.
 * <p>
 * An executable statement.
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
public abstract class StatementImpl<R, F extends ListenableFuture<R>, T>
  implements GenericStatement<R, F> {
  /**
   * Holds the logger.
   *
   * @author paouelle
   */
  private final static Logger logger = LogManager.getFormatterLogger(StatementImpl.class);

  /**
   * Holds the column name prefix used for the special clustering column key
   * created for multi-clustering keys.
   *
   * @author paouelle
   */
  public final static String MK_PREFIX = "mk_";

  /**
   * Holds the statement manager.
   *
   * @author paouelle
   */
  protected final StatementManagerImpl mgr;

  /**
   * Holds the statement bridge.
   *
   * @author paouelle
   */
  protected final StatementBridge bridge;

  /**
   * Holds the result class from the execution of this statement.
   *
   * @author paouelle
   */
  protected final Class<R> resultClass;

  /**
   * Holds the POJO class for this statement or <code>null</code> if this
   * statement is not associated with a POJO class.
   *
   * @author paouelle
   */
  private final Class<T> pojoClass;

  /**
   * Holds the context associated with the POJO class for this statement or
   * <code>null</code> if this statement is not associated with a POJO class.
   *
   * @author paouelle
   */
  private final ClassInfoImpl<T>.Context context;

  /**
   * Holds the POJO context associated with the POJO class for this statement
   * or <code>null</code> if this statement is not associated with a POJO object.
   *
   * @author paouelle
   */
  private final ClassInfoImpl<T>.POJOContext pojoContext;

  /**
   * Holds the keyspace name for this statement if known.
   *
   * @author paouelle
   */
  private volatile String keyspace;

  /**
   * Holds the consistency level for this statement
   *
   * @author paouelle
   */
  private volatile ConsistencyLevel consistency;

  /**
   * Holds the serial consistency level for this statement.
   *
   * @author paouelle
   */
  private volatile ConsistencyLevel serialConsistency;

  /**
   * Flag indicating if tracing is enabled for this statement.
   *
   * @author paouelle
   */
  private volatile boolean traceQuery = true;

  /**
   * Holds the query fetch size for this statement.
   *
   * @author paouelle
   */
  private volatile int fetchSize;

  /**
   * Holds the retry policy for this statement.
   *
   * @author paouelle
   */
  private volatile RetryPolicy retryPolicy;

  /**
   * Holds a dirty bit for the cached query string.
   *
   * @author paouelle
   */
  private volatile boolean dirty;

  /**
   * Holds the cached query string.
   *
   * @author paouelle
   */
  private volatile StringBuilder cache;

  /**
   * Flag indicating if we encountered a counter assignment operation.
   *
   * @author paouelle
   */
  protected volatile Boolean isCounterOp;

  /**
   * Instantiates a new <code>StatementImpl</code> object.
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
  @SuppressWarnings("unchecked")
  protected StatementImpl(
    Class<R> resultClass,
    ClassInfoImpl<T>.Context context,
    StatementManagerImpl mgr,
    StatementBridge bridge
  ) {
    org.apache.commons.lang3.Validate.notNull(
      resultClass, "invalid null result class"
    );
    org.apache.commons.lang3.Validate.isTrue(
      (resultClass == ObjectSet.class)
      || (resultClass == Void.class)
      || (resultClass == ResultSet.class),
      "unsupported result class: " + resultClass.getName()
    );
    this.mgr = mgr;
    this.bridge = bridge;
    this.resultClass = resultClass;
    this.context = context;
    if (context != null) {
      this.pojoClass = context.getObjectClass();
      this.pojoContext = (
        (context instanceof ClassInfoImpl<?>.POJOContext)
        ? (ClassInfoImpl<T>.POJOContext)context
        : null
      );
    } else {
      this.pojoClass = null;
      this.pojoContext = null;
      this.keyspace = null;
    }
    this.keyspace = null;
  }

  /**
   * Instantiates a new <code>StatementImpl</code> object.
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
  protected StatementImpl(
    Class<R> resultClass,
    String keyspace,
    StatementManagerImpl mgr,
    StatementBridge bridge
  ) {
    org.apache.commons.lang3.Validate.notNull(
      resultClass, "invalid null result class"
    );
    org.apache.commons.lang3.Validate.isTrue(
      (resultClass == ObjectSet.class)
      || (resultClass == Void.class)
      || (resultClass == ResultSet.class),
      "unsupported result class: " + resultClass.getName()
    );
    this.mgr = mgr;
    this.bridge = bridge;
    this.resultClass = resultClass;
    this.pojoClass = null;
    this.context = null;
    this.pojoContext = null;
    this.keyspace = keyspace;
  }

  /**
   * Instantiates a new <code>StatementImpl</code> object.
   *
   * @author paouelle
   *
   * @param statement the non-<code>null</code> statement being copied or
   *        wrapped
   */
  protected StatementImpl(StatementImpl<R, F, T> statement) {
    this.mgr = statement.mgr;
    this.bridge = statement.bridge;
    this.resultClass = statement.resultClass;
    this.pojoClass = statement.pojoClass;
    this.context = statement.context;
    this.pojoContext = statement.pojoContext;
    this.keyspace = statement.keyspace;
    this.consistency = statement.consistency;
    this.serialConsistency = statement.serialConsistency;
    this.traceQuery = statement.traceQuery;
    this.fetchSize = statement.fetchSize;
    this.retryPolicy = statement.retryPolicy;
    this.dirty = statement.dirty;
    this.cache = statement.cache;
    this.isCounterOp = statement.isCounterOp;
  }

  /**
   * Builds the query strings (one per underlying statement) to be batched if
   * the statement represents some form of batch statement.
   *
   * @author paouelle
   *
   * @return the string builders used to build the query strings to be batched
   *         or <code>null</code> if nothing to batch
   */
  protected abstract StringBuilder[] buildQueryStrings();

  /**
   * Appends the type of group this is; used when building a query string.
   *
   * @author paouelle
   *
   * @param builder the builder to which to append the type of group
   */
  protected void appendGroupType(StringBuilder builder) {}

  /**
   * Appends the sub-type of group this is; used when building a query string.
   *
   * @author paouelle
   *
   * @param builder the builder to which to append the sub-type of group
   */
  protected void appendGroupSubType(StringBuilder builder) {}

  /**
   * Called to append options to the query string when building a batch.
   *
   * @author paouelle
   *
   * @param builder the builder to which to append options
   */
  protected void appendOptions(StringBuilder builder) {}

  /**
   * Builds the query string.
   *
   * @author paouelle
   *
   * @return the string builder used to build the query string or <code>null</code>
   *         if there is no query built
   */
  @SuppressWarnings("cast")
  protected StringBuilder buildQueryString() {
    final StringBuilder[] builders = buildQueryStrings();

    if ((builders == null) || (builders.length == 0)) {
      // nothing to update so return null
      return null;
    } else if ((builders.length == 1) && !(((Object)this) instanceof Batch)) {
      // shortcut since there is only 1 query string returned
      // except if this is a batch in which case we want to treat it still
      // as a batch so we can take advantage of the options defined for it
      return builders[0];
    }
    final StringBuilder builder = new StringBuilder();

    builder.append("BEGIN");
    appendGroupSubType(builder);
    builder.append(' ');
    appendGroupType(builder);
    appendOptions(builder);
    builder.append(' ');
    for (final StringBuilder sb: builders) {
      final String s = sb.toString();

      builder.append(s);
      if (!s.trim().endsWith(";")) {
        builder.append(';');
      }
      builder.append(' ');
    }
    builder.append("APPLY ");
    appendGroupType(builder);
    builder.append(';');
    return builder;
  }

  /**
   * Sets the dirty bit for the cached query string.
   *
   * @author paouelle
   */
  protected void setDirty() {
    this.dirty = true;
    this.cache = null;
  }

  /**
   * Sets the dirty bit for the cached query string potentially recursively for
   * all contained statements.
   *
   * @author paouelle
   *
   * @param recurse <code>true</code> to recursively set the dirty bit;
   *        <code>false</code> to simply set this statement dirty bit
   */
  protected void setDirty(boolean recurse) {
    setDirty();
  }

  /**
   * Gets the dirty bit for the cached query string.
   *
   * @author paouelle
   *
   * @return <code>true</code> if the cache is dirty; <code>false</code>
   *         otherwise
   */
  protected boolean isDirty() {
    return dirty;
  }

  /**
   * Checks if we encountered a counter assignment operation.
   *
   * @author paouelle
   *
   * @return <code>true</code> if we encountered a counter assignment operation;
   *         <code>false</code> otherwise
   */
  protected boolean isCounterOp() {
    return isCounterOp == null ? false : isCounterOp;
  }

  /**
   * Sets a flag indicating we have a counter assignment operation.
   *
   * @author paouelle
   *
   * @param isCounterOp <code>true</code> if we are encountering a counter
   *        assignment operation; <code>false</code> otherwise
   */
  protected void setCounterOp(boolean isCounterOp) {
    this.isCounterOp = isCounterOp;
  }

  /**
   * Gets the class of POJO for this statement or <code>null</code> if this
   * statement is not associated with a POJO class.
   *
   * @author paouelle
   *
   * @return the class for the POJO if assocaited with one
   */
  public Class<T> getObjectClass() {
    return pojoClass;
  }

  /**
   * Gets the POJO class information associated with this statement.
   *
   * @author paouelle
   *
   * @return the POJO class info associated with this statement
   */
  public ClassInfo<T> getClassInfo() {
    return context.getClassInfo();
  }

  /**
   * Gets the context associated with the POJO class for this statement or
   * <code>null</code> if this statement is not associated with a POJO class.
   *
   * @author paouelle
   *
   * @return the context for the POJO class if associated with one
   */
  public ClassInfoImpl<T>.Context getContext() {
    return context;
  }

  /**
   * Gets the POJO context associated with the POJO class for this statement
   * or <code>null</code> if this statement is not associated with a POJO object.
   *
   * @author paouelle
   *
   * @return the POJO context for the POJO object if associated with one
   */
  public ClassInfoImpl<T>.POJOContext getPOJOContext() {
    return pojoContext;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.GenericStatement#getKeyspace()
   */
  @Override
  public String getKeyspace() {
    if ((keyspace == null) && (context != null)) {
      this.keyspace = context.getKeyspace();
    }
    return keyspace;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.GenericStatement#setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel)
   */
  @Override
  public GenericStatement<R, F> setConsistencyLevel(ConsistencyLevel consistency) {
    this.consistency = consistency;
    return this;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.GenericStatement#getConsistencyLevel()
   */
  @Override
  public ConsistencyLevel getConsistencyLevel() {
    return consistency;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.GenericStatement#setSerialConsistencyLevel(com.datastax.driver.core.ConsistencyLevel)
   */
  @Override
  public GenericStatement<R, F> setSerialConsistencyLevel(ConsistencyLevel serialConsistency) {
    org.apache.commons.lang3.Validate.isTrue(
      (serialConsistency == ConsistencyLevel.SERIAL)
      || (serialConsistency == ConsistencyLevel.LOCAL_SERIAL),
      "invalid serial consistency level: %s",
      serialConsistency
    );
    this.serialConsistency = serialConsistency;
    return this;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.GenericStatement#getSerialConsistencyLevel()
   */
  @Override
  public ConsistencyLevel getSerialConsistencyLevel() {
    return serialConsistency;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.GenericStatement#enableTracing()
   */
  @Override
  public GenericStatement<R, F> enableTracing() {
    this.traceQuery = true;
    return this;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.GenericStatement#disableTracing()
   */
  @Override
  public GenericStatement<R, F> disableTracing() {
    this.traceQuery = false;
    return this;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.GenericStatement#isTracing()
   */
  @Override
  public boolean isTracing() {
    return traceQuery;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.GenericStatement#setRetryPolicy(com.datastax.driver.core.policies.RetryPolicy)
   */
  @Override
  public GenericStatement<R, F> setRetryPolicy(RetryPolicy policy) {
    this.retryPolicy = policy;
    return this;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.GenericStatement#getRetryPolicy()
   */
  @Override
  public RetryPolicy getRetryPolicy() {
    return retryPolicy;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.GenericStatement#setFetchSize(int)
   */
  @Override
  public GenericStatement<R, F> setFetchSize(int fetchSize) {
    this.fetchSize = fetchSize;
    return this;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.GenericStatement#getFetchSize()
   */
  @Override
  public int getFetchSize() {
    return fetchSize;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.GenericStatement#getQueryString()
   */
  @Override
  public String getQueryString() {
    if (dirty || (cache == null)) {
      final StringBuilder sb = buildQueryString();

      if (sb == null) {
        this.cache = null;
      } else {
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
        this.cache = sb;
      }
      this.dirty = false;
    }
    return (cache != null) ? cache.toString() : null;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.GenericStatement#execute()
   */
  @SuppressWarnings("unchecked")
  @Override
  public R execute() {
    final ListenableFuture<R> future = executeAsync();

    if (future instanceof ObjectSetFuture) {
      return (R)((ObjectSetFuture<?>)future).getUninterruptibly();
    }
    if (future instanceof VoidFuture) {
      return (R)((VoidFuture)future).getUninterruptibly();
    }
    if (future instanceof ResultSetFuture) {
      return (R)((ResultSetFuture)future).getUninterruptibly();
    }
    // should not get here though! but in case ...
    try {
      return Uninterruptibles.getUninterruptibly(future);
    } catch (ExecutionException e) {
      // extracted from DefaultResultSet.extractCauseFromExecutionException()
      // --------------------------------------------------------------------
      // We could just rethrow e.getCause(). However, the cause of the ExecutionException has likely been
      // created on the I/O thread receiving the response. Which means that the stacktrace associated
      // with said cause will make no mention of the current thread. This is painful for say, finding
      // out which execute() statement actually raised the exception. So instead, we re-create the
      // exception.
      if (e.getCause() instanceof DriverException) {
        throw ((DriverException)e.getCause()).copy();
      }
      throw new DriverInternalError("Unexpected exception thrown", e.getCause());
    }
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.GenericStatement#executeAsync()
   */
  @Override
  @SuppressWarnings("unchecked")
  public F executeAsync() {
    final ResultSetFuture rawFuture = executeAsyncRaw();

    if (ObjectSet.class == resultClass) {
      return (F)bridge.newObjectSetFuture(context, rawFuture);
    }
    if (Void.class == resultClass) {
      return (F)bridge.newVoidFuture(rawFuture);
    }
    if (ResultSet.class == resultClass) {
      return (F)rawFuture;
    }
    throw new IllegalStateException(
      "unsupported result class: " + resultClass.getName()
    );
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.GenericStatement#executeRaw()
   */
  @Override
  public ResultSet executeRaw() {
    return executeAsyncRaw().getUninterruptibly();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.GenericStatement#executeAsyncRaw()
   */
  @Override
  public ResultSetFuture executeAsyncRaw() {
    final String query = getQueryString();

    try {
      if (StringUtils.isEmpty(query)) { // nothing to query
        return new EmptyResultSetFuture(mgr);
      }
      if (logger.isDebugEnabled()) {
        if (isTracing()) {
          if ((query.length() < 2048)
              || !((this instanceof Batch) || (this instanceof Sequence))) {
            logger.log(Level.DEBUG, "CQL -> %s", query);
          } else {
            if (this instanceof Batch) {
              logger.log(Level.DEBUG, "CQL -> %s ... APPLY BATCH", query.substring(0, 2024));
            } else {
              logger.log(Level.DEBUG, "CQL -> %s ... APPLY SEQUENCE", query.substring(0, 2024));
            }
          }
        }
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
      return mgr.getSession().executeAsync(raw);
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
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return StringUtils.defaultString(getQueryString());
  }
}

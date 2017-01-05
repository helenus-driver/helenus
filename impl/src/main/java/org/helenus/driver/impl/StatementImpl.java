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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import java.nio.ByteBuffer;

import org.apache.commons.lang3.StringUtils;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.EmptyResultSetFuture;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.DriverInternalError;
import com.datastax.driver.core.policies.RetryPolicy;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;

import org.helenus.driver.Batch;
import org.helenus.driver.GenericStatement;
import org.helenus.driver.Group;
import org.helenus.driver.ObjectSet;
import org.helenus.driver.ObjectSetFuture;
import org.helenus.driver.Sequence;
import org.helenus.driver.StatementBridge;
import org.helenus.driver.StatementPreprocessingException;
import org.helenus.driver.VoidFuture;
import org.helenus.driver.info.ClassInfo;

/**
 * The <code>StatementImpl</code> abstract class extends the functionality of
 * Cassandra's {@link com.datastax.driver.core.Statement} class to provide support
 * for POJOs.
 * <p>
 * An executable statement.
 *
 * @copyright 2015-2016 The Helenus Driver Project Authors
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
   * Holds the column name prefix used for the special column key created for
   * case insensitive keys.
   * <p>
   * <i>Note:</i> Multi-clustering key columns also marked as case insensitive
   * will not be using this prefix and instead will continue using the
   * {@link #MK_PREFIX} prefix.
   *
   * @author paouelle
   */
  public final static String CI_PREFIX = "ci_";

  /**
   * Holds the column name prefix used for the special column that holds the
   * collection of elements for a user-defined type that extends {@link List},
   * {@link Set}, or {@link Map}.
   *
   * @author paouelle
   */
  public final static String UDT_C_PREFIX = "c_";

  /**
   * Extracts the cause from a given execution exception and throws back a
   * corresponding driver exception.
   *
   * @author paouelle
   *
   * @param  e the execution exception to handle
   * @return nothing
   * @throws DriverException a driver exception based on the cause of the
   *         provided execution exception
   */
  static DriverException propagateCause(ExecutionException e)
    throws DriverException {
    // extracted from com.datastax.driver.core.DriverThrowables.propagateCause()
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
   * Holds a flag indicating if this statement is enabled or not.
   *
   * @author paouelle
   */
  private volatile boolean enabled = true;

  /**
   * Holds the serial consistency level for this statement.
   *
   * @author paouelle
   */
  private volatile ConsistencyLevel serialConsistency;

  /**
   * Trace prefix to prepend to the statement when traced or <code>null</code>
   * if tracing is disabled for this statement.
   *
   * @author paouelle
   */
  private volatile String tracePrefix = null;

  /**
   * Error trace prefix to prepend to the statement when traced or <code>null</code>
   * if error tracing is disabled for this statement.
   *
   * @author paouelle
   */
  private volatile String errorTracePrefix = null;

  /**
   * Holds the query fetch size for this statement.
   *
   * @author paouelle
   */
  private volatile int fetchSize;

  /**
   * Holds the default timestamp for this statement.
   *
   * @author paouelle
   */
  private volatile long defaultTimestamp = Long.MIN_VALUE;

  /**
   * Holds the read timeout for this statement.
   *
   * @author paouelle
   */
  private volatile int readTimeoutMillis = Integer.MIN_VALUE;

  /**
   * Holds the retry policy for this statement.
   *
   * @author paouelle
   */
  private volatile RetryPolicy retryPolicy;

  /**
   * Holds the paging state for this statement.
   *
   * @author paouelle
   */
  private volatile ByteBuffer pagingState;

  /**
   * Holds the idempotent flag for this statement.
   *
   * @author paouelle
   */
  protected volatile Boolean idempotent; // TODO

  /**
   * Holds a dirty bit for the cached query string.
   *
   * @author paouelle
   */
  private volatile boolean dirty;

  /**
   * Holds the number of simple statements included as part of this statement.
   * <p>
   * <i>Note:</i> Even for inserts, this value might be bigger than one if the
   * POJO defines multiple tables. Set to <code>-1</code> if not computed yet.
   *
   * @author paouelle
   */
  protected volatile int simpleSize = -1;

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
   * Holds user-data associated with this statement.
   *
   * @author paouelle
   */
  private volatile Object data = null;

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
    this.enabled = statement.enabled;
    this.consistency = statement.consistency;
    this.serialConsistency = statement.serialConsistency;
    this.tracePrefix = statement.tracePrefix;
    this.errorTracePrefix = statement.errorTracePrefix;
    this.fetchSize = statement.fetchSize;
    this.defaultTimestamp = statement.defaultTimestamp;
    this.readTimeoutMillis = statement.readTimeoutMillis;
    this.retryPolicy = statement.retryPolicy;
    this.pagingState = statement.pagingState;
    this.idempotent = statement.idempotent;
    this.simpleSize = statement.simpleSize;
    this.dirty = statement.dirty;
    this.cache = statement.cache;
    this.isCounterOp = statement.isCounterOp;
    this.data = statement.data;
  }

  /**
   * Instantiates a new <code>StatementImpl</code> object.
   *
   * @author paouelle
   *
   * @param statement the non-<code>null</code> statement being copied or
   *        wrapped
   * @param context the non-<code>null</code> class info context for the POJO
   *        associated with this cloned statement
   */
  protected StatementImpl(
    StatementImpl<R, F, T> statement, ClassInfoImpl<T>.Context context
  ) {
    this.mgr = statement.mgr;
    this.bridge = statement.bridge;
    this.resultClass = statement.resultClass;
    this.pojoClass = statement.pojoClass;
    this.context = context;
    this.pojoContext = null;
    this.keyspace = statement.keyspace;
    this.enabled = statement.enabled;
    this.consistency = statement.consistency;
    this.serialConsistency = statement.serialConsistency;
    this.tracePrefix = statement.tracePrefix;
    this.errorTracePrefix = statement.errorTracePrefix;
    this.fetchSize = statement.fetchSize;
    this.defaultTimestamp = statement.defaultTimestamp;
    this.readTimeoutMillis = statement.readTimeoutMillis;
    this.retryPolicy = statement.retryPolicy;
    this.pagingState = statement.pagingState;
    this.idempotent = statement.idempotent;
    this.simpleSize = statement.simpleSize;
    this.dirty = statement.dirty;
    this.cache = statement.cache;
    this.isCounterOp = statement.isCounterOp;
    this.data = statement.data;
  }

  /**
   * Log to the debug level the execution of the specified simple statement.
   *
   * @author paouelle
   *
   * @param query the query of the statement being executed
   */
  private void debugExecution(String query) {
    if (logger.isDebugEnabled()
        && (isTracing() || mgr.areAllStatementsTracesEnabled())) {
      final String prefix = StringUtils.defaultString(tracePrefix);

      if (mgr.isFullTracesEnabled() || (query.length() < 2048)) {
        logger.log(Level.DEBUG, "%sCQL -> %s", prefix, query);
      } else if (this instanceof Batch) {
        logger.log(
          Level.DEBUG,
          "%sCQL -> %s ... APPLY BATCH",
          prefix,
          query.substring(0, 2032)
        );
      } else if (this instanceof Sequence) {
        logger.log(
          Level.DEBUG,
          "%sCQL -> %s ... APPLY SEQUENCE",
          prefix,
          query.substring(0, 2029)
        );
      } else if (this instanceof Group) {
        logger.log(
          Level.DEBUG,
          "%sCQL -> %s ... APPLY GROUP",
          prefix,
          query.substring(0, 2032)
        );
      } else {
        logger.log(
          Level.DEBUG,
          "%sCQL -> %s ...",
          prefix,
          query.substring(0, 2044)
        );
      }
    }
  }

  /**
   * Log the execution exception.
   *
   * @author paouelle
   *
   * @param query the query string of the statement that failed
   * @param e the exception
   */
  private void errorExecution(String query, Throwable e) {
    // try as an error log first
    if (logger.isErrorEnabled()
        && (isErrorTracing() || mgr.areAllStatementsTracesEnabled())) {
      final String prefix = StringUtils.defaultString(errorTracePrefix);

      if (mgr.isFullTracesEnabled() || (query.length() < 2048)) {
        logger.log(
          Level.ERROR, "%sCQL ERROR -> %s", prefix, query
         );
      } else if (StatementImpl.this instanceof Batch) {
        logger.log(
          Level.ERROR,
          "%sCQL ERROR -> %s ... APPLY BATCH",
          prefix,
          query.substring(0, 2032)
        );
      } else if (StatementImpl.this instanceof Sequence) {
        logger.log(
          Level.ERROR,
          "%sCQL ERROR -> %s ... APPLY SEQUENCE",
          prefix,
          query.substring(0, 2029)
        );
      } else if (StatementImpl.this instanceof Group) {
        logger.log(
          Level.ERROR,
          "%sCQL ERROR -> %s ... APPLY GROUP",
          prefix,
          query.substring(0, 2032)
        );
      } else {
        logger.log(
          Level.ERROR,
          "%sCQL ERROR -> %s ...",
          prefix,
          query.substring(0, 2044)
        );
      }
      logger.log(Level.ERROR, "%sCQL ERROR -> ", errorTracePrefix, e);
    } else if (logger.isDebugEnabled()
               && (isTracing() || mgr.areAllStatementsTracesEnabled())) { // fallback to tracing
      final String prefix = StringUtils.defaultString(tracePrefix);

      if (mgr.isFullTracesEnabled() || (query.length() < 2048)) {
        logger.log(
          Level.DEBUG, "%sCQL ERROR -> %s", prefix, query
        );
      } else if (StatementImpl.this instanceof Batch) {
        logger.log(
          Level.DEBUG,
          "%sCQL ERROR -> %s ... APPLY BATCH",
          prefix,
          query.substring(0, 2032)
        );
      } else if (StatementImpl.this instanceof Sequence) {
        logger.log(
          Level.DEBUG,
          "%sCQL ERROR -> %s ... APPLY SEQUENCE",
          prefix,
          query.substring(0, 2029)
        );
      } else if (StatementImpl.this instanceof Group) {
        logger.log(
          Level.DEBUG,
          "%sCQL ERROR -> %s ... APPLY GROUP",
          prefix,
          query.substring(0, 2032)
        );
      } else {
        logger.log(
          Level.DEBUG,
          "%sCQL ERROR -> %s ...",
          prefix,
          query.substring(0, 2044)
        );
      }
      logger.log(Level.DEBUG, "%sCQL ERROR -> ", prefix, e);
    }
  }

  /**
   * Initializes the specified statement with the same settings as this statement.
   *
   * @author paouelle
   *
   * @param <S> the type of statement to initialize
   *
   * @param  s the non-<code>null</code> statement to initialize
   * @return <code>s</code>
   */
  protected <S extends StatementImpl<?, ?, ?>> S init(S s) {
    if (isEnabled()) {
      s.enable();
    } else {
      s.disable();
    }
    if (getConsistencyLevel() != null) {
      s.setConsistencyLevel(getConsistencyLevel());
    }
    if (getSerialConsistencyLevel() != null) {
      s.setSerialConsistencyLevel(getSerialConsistencyLevel());
    }
    s.enableTracing(tracePrefix);
    s.enableErrorTracing(errorTracePrefix);
    if (getRetryPolicy() != null) {
     s.setRetryPolicy(getRetryPolicy());
    }
    s.setFetchSize(getFetchSize());
    if (defaultTimestamp != Long.MIN_VALUE) {
      s.setDefaultTimestamp(defaultTimestamp);
    }
    if (readTimeoutMillis > 0) {
      s.setReadTimeoutMillis(readTimeoutMillis);
    }
    if (idempotent != null) {
      s.setIdempotent(idempotent);
    }
    s.setUserData(data);
    return s;
  }

  /**
   * Initializes the specified Cassandra statement with the same settings as
   * this statement.
   *
   * @author paouelle
   *
   * @param <S> the type of statement to initialize
   *
   * @param  s the non-<code>null</code> statement to initialize
   * @return <code>s</code>
   */
  protected <S extends Statement> S init(S s) {
    if (getConsistencyLevel() != null) {
      s.setConsistencyLevel(getConsistencyLevel());
    }
    if (getSerialConsistencyLevel() != null) {
      s.setSerialConsistencyLevel(getSerialConsistencyLevel());
    }
    if (s.isTracing()) {
      s.enableTracing();
    } else {
      s.disableTracing();
    }
    if (getRetryPolicy() != null) {
     s.setRetryPolicy(getRetryPolicy());
    }
    s.setFetchSize(getFetchSize());
    if (defaultTimestamp != Long.MIN_VALUE) {
      s.setDefaultTimestamp(defaultTimestamp);
    }
    if (readTimeoutMillis > 0) {
      s.setReadTimeoutMillis(readTimeoutMillis);
    }
    if (idempotent != null) {
      s.setIdempotent(idempotent);
    }
    return s;
  }

  /**
   * Gets all underlying statements or this statement if none contained within.
   * <p>
   * <i>Note:</i> Any statements that contains other statements should be
   * flattened out based on its type. For example, a batch will flattened out
   * all included batches recursively. A sequence will do the same with contained
   * sequences but not with contained groups or batches. A group will do the
   * same with contained groups but not with contained sequences or batches.
   *
   * @author paouelle
   *
   * @return a non-<code>null</code> list of all underlying statements from this
   *         statement
   */
  protected List<StatementImpl<?, ?, ?>> buildStatements()  {
    if (!isEnabled()) {
      return Collections.emptyList();
    }
    // by default return a list with just ourselves
    return Collections.singletonList(this);
  }

  /**
   * Builds the query strings (one per underlying statement) to be collected if
   * the statement represents some a batch, sequence, or a group statement.
   *
   * @author paouelle
   *
   * @return the string builders used to build the query strings to be collected
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
    if (!enabled) {
      return null;
    }
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
   * Clears the keyspace so it be recomputed later.
   *
   * @author paouelle
   */
  protected void clearKeyspace() {
    this.keyspace = null;
  }

  /**
   * Sets the dirty bit for the cached query string and clear the number of
   * statements.
   *
   * @author paouelle
   */
  protected void setDirty() {
    this.dirty = true;
    this.cache = null;
    this.simpleSize = -1; // clear the simple size as well
  }

  /**
   * Sets the dirty bit for the cached query string and clear the number of
   * statements potentially recursively for all contained statement.
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
   * Gets the number of simple statements in this statement.
   *
   * @author paouelle
   *
   * @return the number of simple statements in this statement
   */
  protected abstract int simpleSize();

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
   * Executes this statement asynchronously.
   *
   * This method does not block. It returns as soon as the statement has been
   * passed to the underlying network stack. In particular, returning from
   * this method does not guarantee that the statement is valid or has even been
   * submitted to a live node. Any exception pertaining to the failure of the
   * statement will be thrown when accessing the {@link ResultSetFuture}.
   *
   * Note that for queries that doesn't return a result (INSERT, UPDATE and
   * DELETE), you will need to access the result future (that is call one of
   * its get method to make sure the statement's execution was successful.
   *
   * @author paouelle
   *
   * @return a future on the result of the statement's execution
   * @throws IllegalArgumentException if the statement is associated with a POJO
   *         and the keyspace has not yet been computed and cannot be
   *         computed with the provided keyspace keys yet
   * @throws StatementPreprocessingException if the statement cannot be preprocessed
   *         for execution
   */
  @SuppressWarnings("unchecked")
  protected F executeAsync0() {
    final ResultSetFuture rawFuture = executeAsyncRaw0();

    if (ObjectSet.class == resultClass) {
      return (F)new ObjectSetFutureImpl<>(context, rawFuture);
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
   * Executes the provided statement asynchronously and returned a raw result set.
   * A raw result set is in the raw form as returned by Cassandra's driver. This
   * can be useful when doing SELECT statement that queries for COUNT().
   *
   * This method does not block. It returns as soon as the statement has been
   * passed to the underlying network stack. In particular, returning from
   * this method does not guarantee that the statement is valid or has even been
   * submitted to a live node. Any exception pertaining to the failure of the
   * statement will be thrown when accessing the {@link ResultSetFuture}.
   *
   * Note that for queries that doesn't return a result (INSERT, UPDATE and
   * DELETE), you will need to access the ResultSetFuture (that is call one of
   * its get method) to make sure the statement was successful.
   *
   * @author paouelle
   *
   * @return a future on the raw result of the statement.
   * @throws IllegalArgumentException if the statement is associated with a POJO
   *         and the keyspace has not yet been computed and cannot be
   *         computed with the provided keyspace keys yet
   * @throws StatementPreprocessingException if the statement cannot be preprocessed
   *         for execution
   */
  protected ResultSetFuture executeAsyncRaw0() {
    if (!enabled) {
      return new EmptyResultSetFuture(mgr);
    }
    final String query = getQueryString();

    try {
      if (StringUtils.isEmpty(query)) { // nothing to query
        return new EmptyResultSetFuture(mgr);
      }
      final SimpleStatement raw = init(new SimpleStatement(query));

      debugExecution(query);
      final ResultSetFuture f = mgr.sent(this, mgr.getSession().executeAsync(raw));

      return new ResultSetFuture() {
        @Override
        public void addListener(Runnable listener, Executor executor) {
          f.addListener(listener, executor);
        }
        @Override
        public boolean isCancelled() {
          return f.isCancelled();
        }
        @Override
        public boolean isDone() {
          return f.isDone();
        }
        @SuppressWarnings("synthetic-access")
        @Override
        public ResultSet get() throws InterruptedException, ExecutionException {
          try {
            return f.get();
          } catch (InterruptedException e) {
            throw e;
          } catch (Error|Exception e) {
            errorExecution(query, e);
            throw e; // re-throw
          }
        }
        @SuppressWarnings("synthetic-access")
        @Override
        public ResultSet get(long timeout, TimeUnit unit)
          throws InterruptedException, ExecutionException, TimeoutException {
          try {
            return f.get(timeout, unit);
          } catch (InterruptedException|TimeoutException e) {
            throw e;
          } catch (Error|Exception e) {
            errorExecution(query, e);
            throw e; // re-throw
          }
        }
        @SuppressWarnings("synthetic-access")
        @Override
        public ResultSet getUninterruptibly() {
          try {
            return f.getUninterruptibly();
          } catch (Error|Exception e) {
            errorExecution(query, e);
            throw e; // re-throw
          }
        }
        @SuppressWarnings("synthetic-access")
        @Override
        public ResultSet getUninterruptibly(long timeout, TimeUnit unit)
          throws TimeoutException {
          try {
            return f.getUninterruptibly(timeout, unit);
          } catch (TimeoutException e) {
            throw e;
          } catch (Error|Exception e) {
            errorExecution(query, e);
            throw e; // re-throw
          }
        }
        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
          return f.cancel(mayInterruptIfRunning);
        }
      };
    } finally {
      // let's recursively clear the query string that gets cache to reduce
      // the memory impact chance are now that it got executed, it won't be
      // needed anymore
      setDirty(true);
    }
  }

  /**
   * Gets the class of POJO for this statement or <code>null</code> if this
   * statement is not associated with a POJO class.
   *
   * @author paouelle
   *
   * @return the class for the POJO if associated with one
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
   * Gets the POJO class information associated with this statement.
   *
   * @author paouelle
   *
   * @return the POJO class info associated with this statement
   */
  public ClassInfoImpl<T> getClassInfoImpl() {
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
   * Gets the POJO object associated with this statement or <code>null</code>
   * if this statement is not associated with a POJO object.
   *
   * @author paouelle
   *
   * @return the POJO object if associated with one
   */
  public T getObject() {
    return (pojoContext != null) ? pojoContext.getObject() : null;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.GenericStatement#getKeyspace()
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
   * @see org.helenus.driver.GenericStatement#enable()
   */
  @Override
  public GenericStatement<R, F> enable() {
    if (!enabled) {
      setDirty();
      this.enabled = true;
    }
    return this;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.GenericStatement#disable()
   */
  @Override
  public GenericStatement<R, F> disable() {
    if (enabled) {
      setDirty();
      this.enabled = false;
    }
    return this;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.GenericStatement#isEnabled()
   */
  @Override
  public boolean isEnabled() {
    return enabled;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.GenericStatement#setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel)
   */
  @Override
  public GenericStatement<R, F> setConsistencyLevel(ConsistencyLevel consistency) {
    if (consistency != this.consistency) {
      setDirty();
      this.consistency = consistency;
    }
    return this;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.GenericStatement#getConsistencyLevel()
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
   * @see org.helenus.driver.GenericStatement#setSerialConsistencyLevel(com.datastax.driver.core.ConsistencyLevel)
   */
  @Override
  public GenericStatement<R, F> setSerialConsistencyLevel(
    ConsistencyLevel serialConsistency
  ) {
    org.apache.commons.lang3.Validate.isTrue(
      serialConsistency.isSerial(),
      "invalid serial consistency level: %s",
      serialConsistency
    );
    if (serialConsistency != this.serialConsistency) {
      setDirty();
      this.serialConsistency = serialConsistency;
    }
    return this;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.GenericStatement#getSerialConsistencyLevel()
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
   * @see org.helenus.driver.GenericStatement#enableTracing()
   */
  @Override
  public GenericStatement<R, F> enableTracing() {
    if (!Objects.equals("", tracePrefix)) {
      setDirty();
      this.tracePrefix = "";
    }
    return this;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.GenericStatement#enableTracing(java.lang.String)
   */
  @Override
  public GenericStatement<R, F> enableTracing(String prefix) {
    if (!Objects.equals(prefix, tracePrefix)) {
      setDirty();
      this.tracePrefix = prefix;
    }
    return this;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.GenericStatement#disableTracing()
   */
  @Override
  public GenericStatement<R, F> disableTracing() {
    if (tracePrefix != null) {
      setDirty();
      this.tracePrefix = null;
    }
    return this;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.GenericStatement#isTracing()
   */
  @Override
  public boolean isTracing() {
    return (tracePrefix != null);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.GenericStatement#enableErrorTracing()
   */
  @Override
  public GenericStatement<R, F> enableErrorTracing() {
    if (!Objects.equals("", errorTracePrefix)) {
      setDirty();
      this.errorTracePrefix = "";
    }
    return this;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.GenericStatement#enableErrorTracing(java.lang.String)
   */
  @Override
  public GenericStatement<R, F> enableErrorTracing(String prefix) {
    if (!Objects.equals(prefix, errorTracePrefix)) {
      setDirty();
      this.errorTracePrefix = prefix;
    }
    this.errorTracePrefix = prefix;
    return this;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.GenericStatement#disableErrorTracing()
   */
  @Override
  public GenericStatement<R, F> disableErrorTracing() {
    if (errorTracePrefix != null) {
      setDirty();
      this.errorTracePrefix = null;
    }
    return this;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.GenericStatement#isErrorTracing()
   */
  @Override
  public boolean isErrorTracing() {
    return (errorTracePrefix != null);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.GenericStatement#setRetryPolicy(com.datastax.driver.core.policies.RetryPolicy)
   */
  @Override
  public GenericStatement<R, F> setRetryPolicy(RetryPolicy policy) {
    if (!Objects.equals(policy, retryPolicy)) {
      setDirty();
      this.retryPolicy = policy;
    }
    return this;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.GenericStatement#getRetryPolicy()
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
   * @see org.helenus.driver.GenericStatement#setFetchSize(int)
   */
  @Override
  public GenericStatement<R, F> setFetchSize(int fetchSize) {
    if (fetchSize != this.fetchSize) {
      setDirty();
      this.fetchSize = fetchSize;
    }
    return this;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.GenericStatement#getFetchSize()
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
   * @see org.helenus.driver.GenericStatement#setDefaultTimestamp(long)
   */
  @Override
  public GenericStatement<R, F> setDefaultTimestamp(long defaultTimestamp) {
    this.defaultTimestamp = defaultTimestamp;
    return this;
  }

  /**
   * {@inheritDoc}
   *
   * @author <a href="mailto:paouelle@enlightedinc.com">paouelle</a>
   *
   * @see org.helenus.driver.GenericStatement#getDefaultTimestamp()
   */
  @Override
  public long getDefaultTimestamp() {
    return defaultTimestamp;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.GenericStatement#setReadTimeoutMillis(int)
   */
  @Override
  public GenericStatement<R, F> setReadTimeoutMillis(int readTimeoutMillis) {
    org.apache.commons.lang3.Validate.isTrue(readTimeoutMillis >= 0, "read timeout must be >= 0");
    this.readTimeoutMillis = readTimeoutMillis;
    return this;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.GenericStatement#getReadTimeoutMillis()
   */
  @Override
  public int getReadTimeoutMillis() {
    return readTimeoutMillis;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.GenericStatement#setIdempotent(boolean)
   */
  @Override
  public GenericStatement<R, F> setIdempotent(boolean idempotent) {
    this.idempotent = idempotent;
    return this;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.GenericStatement#isIdempotent()
   */
  @Override
  public Boolean isIdempotent() {
    return idempotent;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.GenericStatement#getUserData()
   */
  @Override
  @SuppressWarnings("unchecked")
  public <U> U getUserData() {
    return (U)data;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.GenericStatement#setUserData(java.lang.Object)
   */
  @Override
  public <U> void setUserData(U data) {
    this.data = data;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.GenericStatement#getQueryString()
   */
  @Override
  public String getQueryString() {
    if (!enabled) {
      return null;
    }
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
   * @see org.helenus.driver.GenericStatement#execute()
   */
  @SuppressWarnings("unchecked")
  @Override
  public R execute() {
    final ListenableFuture<R> future = executeAsync();

    try {
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
      return Uninterruptibles.getUninterruptibly(future);
    } catch (ExecutionException e) {
      throw StatementImpl.propagateCause(e);
    }
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.GenericStatement#executeAsync()
   */
  @Override
  public F executeAsync() {
    mgr.executing(this);
    return executeAsync0();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.GenericStatement#executeRaw()
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
   * @see org.helenus.driver.GenericStatement#executeAsyncRaw()
   */
  @Override
  public ResultSetFuture executeAsyncRaw() {
    mgr.executing(this);
    return executeAsyncRaw0();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.lang.Object#toString()
   */
  @Override
  public final String toString() {
    return StringUtils.defaultString(getQueryString());
  }
}

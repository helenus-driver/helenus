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
package com.github.helenus.driver;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.datastax.driver.core.exceptions.QueryValidationException;
import com.datastax.driver.core.policies.RetryPolicy;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * The <code>GenericStatement</code> interface is used to extend the functionality of
 * Cassandra's {@link com.datastax.driver.core.Statement} class to provide support
 * for POJOs.
 * <p>
 * An executable statement.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 15, 2015 - paouelle - Creation
 *
 * @param <R> The type of result returned when executing this statement
 * @param <F> The type of future result returned when executing this statement
 *
 * @since 1.0
 */
public interface GenericStatement<R, F extends ListenableFuture<R>> {
  /**
   * Gets the keyspace this statement operates on.
   * <p>
   * Note that not all statements specify on which keyspace they operate on, and so
   * this method can always return <code>null</code>. Firstly, some queries do not
   * operate inside a keyspace: keyspace creation, {@code USE} queries, user
   * creation, etc. Secondly, even statements that operate within a keyspace do not
   * have to specify said keyspace directly, in which case the currently logged
   * in keyspace (the one set through a {@code USE} query (or through the use of
   * {@link Cluster#connect(String)})). Lastly, this keyspace information is only
   * a hint for token-aware routing (since replica placement depend on the
   * replication strategy in use which is a per-keyspace property) and having
   * this method return <code>null</code> (or even a bogus keyspace name) will
   * never cause the statement to fail.
   *
   * @author paouelle
   *
   * @return the keyspace this statement operate on if relevant or
   *         <code>null</code> if none defined yet
   * @throws IllegalArgumentException if the statement is associated with a POJO
   *         and the keyspace has not yet been computed and cannot be
   *         computed with the provided suffixes yet
   */
  public String getKeyspace();

  /**
   * Sets the consistency level for the statement.
   *
   * @author paouelle
   *
   * @param  consistency the consistency level to set.
   * @return this {@code GenericStatement} object
   */
  public GenericStatement<R, F> setConsistencyLevel(ConsistencyLevel consistency);

  /**
   * Gets the consistency level for this statement.
   *
   * @author paouelle
   *
   * @return the consistency level for this statement, or <code>null</code> if
   *         no consistency level has been specified (through
   *         {@code setConsistencyLevel}). In the latter case, the default
   *         consistency level will be used.
   */
  public ConsistencyLevel getConsistencyLevel();

  /**
   * Sets the serial consistency level for the statement.
   * <p>
   * The serial consistency level is only used by conditional updates (so
   * INSERT, UPDATE and DELETE with an IF condition). For those, the serial
   * consistency level defines the consistency level of the serial phase (or
   * "paxos" phase) while the normal consistency level defines the consistency
   * for the "learn" phase, i.e. what type of reads will be guaranteed to see
   * the update right away. For instance, if a conditional write has a regular
   * consistency of QUORUM (and is successful), then a QUORUM read is guaranteed
   * to see that write. But if the regular consistency of that write is ANY,
   * then only a read with a consistency of SERIAL is guaranteed to see it (even
   * a read with consistency ALL is not guaranteed to be enough).
   * <p>
   * The serial consistency can only be one of {@code ConsistencyLevel.SERIAL}
   * or {@code ConsistencyLevel.LOCAL_SERIAL}. While
   * {@code ConsistencyLevel.SERIAL} guarantees full linearizability (with other
   * SERIAL updates), {@code ConsistencyLevel.LOCAL_SERIAL} only guarantees it
   * in the local datacenter.
   * <p>
   * The serial consistency level is ignored for any statement that is not a
   * conditional update (serial reads should use the regular consistency level
   * for instance).
   *
   * @author paouelle
   *
   * @param  serialConsistency the serial consistency level to set.
   * @return this {@code GenericStatement} object
   * @throws IllegalArgumentException if {@code serialConsistency} is not one of
   *         {@code ConsistencyLevel.SERIAL} or
   *         {@code ConsistencyLevel.LOCAL_SERIAL}.
   */
  public GenericStatement<R, F> setSerialConsistencyLevel(ConsistencyLevel serialConsistency);

  /**
   * Gets the serial consistency level for this statement.
   * <p>
   * See {@link #setSerialConsistencyLevel} for more detail on the serial
   * consistency level.
   *
   * @author paouelle
   *
   * @return the consistency level for this statement, or {@code null} if no serial
   *         consistency level has been specified (through
   *         {@code setSerialConsistencyLevel}). In the latter case, the default
   *         serial consistency level will be used.
   */
  public ConsistencyLevel getSerialConsistencyLevel();

  /**
   * Enables tracing for this statement.
   * <p>
   * By default (that is unless you call this method), tracing is not enabled.
   *
   * @author paouelle
   *
   * @return this {@code GenericStatement} object
   */
  public GenericStatement<R, F> enableTracing();

  /**
   * Disables tracing for this statement.
   *
   * @author paouelle
   *
   * @return this for chaining
   */
  public GenericStatement<R, F> disableTracing();

  /**
   * Checks whether tracing is enabled for this statement or not.
   *
   * @author paouelle
   *
   * @return <code>true</code> if this statement has tracing enabled, <code>false</code>
   *         otherwise.
   */
  public boolean isTracing();

  /**
   * Sets the retry policy to use for this statement.
   * <p>
   * The default retry policy, if this method is not called, is the one returned
   * by {@link com.datastax.driver.core.policies.Policies#getRetryPolicy} in the
   * cluster configuration. This method is thus only useful in case you want to
   * punctually override the default policy for this request.
   *
   * @author paouelle
   *
   * @param  policy the retry policy to use for this statement.
   * @return this {@code GenericStatement} object
   */
  public GenericStatement<R, F> setRetryPolicy(RetryPolicy policy);

  /**
   * Gets the retry policy sets for this statement, if any.
   *
   * @author paouelle
   *
   * @return the retry policy sets specifically for this statement or <code>null</code>
   *         if no statement specific retry policy has been set through
   *         {@link #setRetryPolicy} (in which case the Cluster retry policy
   *         will apply if necessary).
   */
  public RetryPolicy getRetryPolicy();

  /**
   * Sets the statement fetch size.
   * <p>
   * The fetch size controls how much resulting POJOs or rows will be retrieved
   * simultaneously (the goal being to avoid loading too much results in memory
   * for queries yielding large results). Please note that while value as low as
   * 1 can be used, it is *highly* discouraged to use such a low value in
   * practice as it will yield very poor performance. If in doubt, leaving the
   * default is probably a good idea.
   * <p>
   * Also note that only {@code SELECT} queries only ever make use of that
   * setting.
   *
   * @author paouelle
   *
   * @param  fetchSize the fetch size to use. If {@code fetchSize &lte; 0}, the
   *         default fetch size will be used. To disable paging of the result
   *         set, use {@code fetchSize == Integer.MAX_VALUE}.
   * @return this {@code GenericStatement} object
   */
  public GenericStatement<R, F> setFetchSize(int fetchSize);

  /**
   * Gets the fetch size for this statement.
   *
   * @author paouelle
   *
   * @return the fetch size for this statement. If that value is less or equal to 0
   *         (the default unless {@link #setFetchSize} is used), the default
   *         fetch size will be used.
   */
  public int getFetchSize();

  /**
   * Gets the query string for this statement.
   *
   * @author paouelle
   *
   * @return a valid CQL query string or <code>null</code> if nothing to query
   * @throws IllegalArgumentException if the statement is associated with a POJO
   *         and the keyspace has not yet been computed and cannot be
   *         computed with the provided suffixes yet
   */
  public String getQueryString();

  /**
   * Executes this statement and return a result.
   *
   * This method blocks until at least some result has been received from the
   * database. However, for SELECT queries, it does not guarantee that the
   * result has been received in full. But it does guarantee that some
   * response has been received from the database, and in particular
   * guarantee that if the request is invalid, an exception will be thrown
   * by this method.
   *
   * @author paouelle
   *
   * @return the non-<code>null</code> result of the execution of the statement.
   *         That result will never be <code>null</code>
   * @throws IllegalArgumentException if the statement is associated with a POJO
   *         and the keyspace has not yet been computed and cannot be
   *         computed with the provided suffixes yet
   * @throws NoHostAvailableException if no host in the cluster can be
   *         contacted successfully to execute this statement.
   * @throws QueryExecutionException if the statement triggered an execution
   *         exception, i.e. an exception thrown by Cassandra when it cannot execute
   *         the statement with the requested consistency level successfully.
   * @throws UpdateNotAppliedException if the statement is a conditional update
   *         that failed to be applied
   * @throws ObjectExistException if the statement is a conditional insert
   *         that failed to be applied
   * @throws QueryValidationException if the statement is invalid (syntax error,
   *         unauthorized or any other validation problem).
   * @throws StatementPreprocessingException if the statement cannot be preprocessed
   *         for execution
   */
  public R execute();

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
   *         computed with the provided suffixes yet
   * @throws StatementPreprocessingException if the statement cannot be preprocessed
   *         for execution
   */
  public F executeAsync();

  /**
   * Executes this statement and returned a raw result set. A raw result set
   * is in the raw form as returned by Cassandra's driver. This can be useful
   * when doing SELECT statement that queries for COUNT().
   *
   * This method blocks until at least some result has been received from the
   * database. However, for SELECT queries, it does not guarantee that the
   * result has been received in full. But it does guarantee that some
   * response has been received from the database, and in particular
   * guarantee that if the request is invalid, an exception will be thrown
   * by this method.
   *
   * @author paouelle
   *
   * @return the non-<code>null</code> raw result of the statement. That result will
   *         never be null but can be empty (and will be for any non SELECT statement).
   * @throws IllegalArgumentException if the statement is associated with a POJO
   *         and the keyspace has not yet been computed and cannot be
   *         computed with the provided suffixes yet
   * @throws NoHostAvailableException if no host in the cluster can be
   *         contacted successfully to execute this statement.
   * @throws QueryExecutionException if the statement triggered an execution
   *         exception, i.e. an exception thrown by Cassandra when it cannot execute
   *         the statement with the requested consistency level successfully.
   * @throws QueryValidationException if the statement is invalid (syntax error,
   *         unauthorized or any other validation problem).
   * @throws StatementPreprocessingException if the statement cannot be preprocessed
   *         for execution
   */
  public ResultSet executeRaw();

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
   *         computed with the provided suffixes yet
   * @throws StatementPreprocessingException if the statement cannot be preprocessed
   *         for execution
   */
  public ResultSetFuture executeAsyncRaw();
}

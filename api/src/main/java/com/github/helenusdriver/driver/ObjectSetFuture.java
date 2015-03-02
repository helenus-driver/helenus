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
package com.github.helenusdriver.driver;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.datastax.driver.core.exceptions.QueryValidationException;
import com.google.common.util.concurrent.AbstractFuture;

/**
 * The <code>ObjectSetFuture</code> class extends Cassandra's
 * {@link com.datastax.driver.core.ResultSetFuture} in order to provide
 * support for POJOs.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 15, 2015 - paouelle - Creation
 *
 * @param <T> The type of POJO associated with the future object set
 *
 * @since 1.0
 */
public class ObjectSetFuture<T> extends AbstractFuture<ObjectSet<T>> {
  /**
   * Holds the statement context associated with this object set.
   *
   * @author paouelle
   */
  private final StatementManager.Context<T> context;

  /**
   * Holds the raw result set future.
   *
   * @author paouelle
   */
  private final ResultSetFuture future;

  /**
   * Instantiates a new <code>ObjectSetFuture</code> object.
   *
   * @author paouelle
   *
   * @param context the non-<code>null</code> statement context associated with
   *        this future object set
   * @param future the non-<code>null</code> result set future
   */
  ObjectSetFuture(StatementManager.Context<T> context, ResultSetFuture future) {
    this.context = context;
    this.future = future;
  }

  /**
   * Post-process the result set. The returned {@link ObjectSet} will be based
   * on what is left off in the result set after post-processing.
   *
   * @author paouelle
   *
   * @param  result the non-<code>null</code> result set to be post processed
   */
  protected void postProcess(ResultSet result) {}

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @throws NoHostAvailableException if no host in the cluster can be contacted
   *         successfully to execute this query.
   * @throws QueryExecutionException if the query triggered an execution
   *         exception, that is an exception thrown by Cassandra when it
   *         cannot execute the query with the requested consistency level
   *         successfully.
   * @throws ObjectNotFoundException if the statement is a select and the
   *         keyspace specified doesn't exist
   * @throws QueryValidationException if the query is invalid (syntax error,
   *         unauthorized or any other validation problem).
   *
   * @see com.google.common.util.concurrent.AbstractFuture#get(long, java.util.concurrent.TimeUnit)
   */
  @Override
  public ObjectSet<T> get(long timeout, TimeUnit unit)
    throws InterruptedException, TimeoutException, ExecutionException {
    try {
      final ResultSet result = future.get(timeout, unit);

      postProcess(result);
      return new ObjectSet<>(context, result);
    } catch (InvalidQueryException e) {
      ObjectNotFoundException.handleKeyspaceNotFound(context.getObjectClass(), e);
      throw e;
    }
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @throws NoHostAvailableException if no host in the cluster can be contacted
   *         successfully to execute this query.
   * @throws QueryExecutionException if the query triggered an execution
   *         exception, that is an exception thrown by Cassandra when it
   *         cannot execute the query with the requested consistency level
   *         successfully.
   * @throws ObjectNotFoundException if the statement is a select and the
   *         keyspace specified doesn't exist
   * @throws QueryValidationException if the query is invalid (syntax error,
   *         unauthorized or any other validation problem).
   *
   * @see com.google.common.util.concurrent.AbstractFuture#get()
   */
  @Override
  public ObjectSet<T> get() throws InterruptedException, ExecutionException {
    try {
      final ResultSet result = future.get();

      postProcess(result);
      return new ObjectSet<>(context, result);
    } catch (InvalidQueryException e) {
      ObjectNotFoundException.handleKeyspaceNotFound(context.getObjectClass(), e);
      throw e;
    }
  }

  /**
   * Waits for the query to return and return its object set.
   *
   * This method is usually more convenient than {@link #get} because it:
   * <ul>
   * <li>Waits for the result uninterruptibly, and so doesn't throw
   * {@link InterruptedException}.</li>
   * <li>Returns meaningful exceptions, instead of having to deal with
   * ExecutionException.</li>
   * </ul>
   * As such, it is the preferred way to get the future result.
   *
   * @author paouelle
   *
   * @return the non-<code>null</code> object set
   * @throws NoHostAvailableException if no host in the cluster can be contacted
   *         successfully to execute this query.
   * @throws QueryExecutionException if the query triggered an execution
   *         exception, that is an exception thrown by Cassandra when it
   *         cannot execute the query with the requested consistency level
   *         successfully.
   * @throws ObjectNotFoundException if the statement is a select and the
   *         keyspace specified doesn't exist
   * @throws QueryValidationException if the query is invalid (syntax error,
   *         unauthorized or any other validation problem).
   */
  public ObjectSet<T> getUninterruptibly() {
    try {
      final ResultSet result = future.getUninterruptibly();

      postProcess(result);
      return new ObjectSet<>(context, result);
    } catch (InvalidQueryException e) {
      ObjectNotFoundException.handleKeyspaceNotFound(context.getObjectClass(), e);
      throw e;
    }
  }

  /**
   * Waits for the provided time for the query to return and return its object
   * set if available.
   *
   * This method is usually more convenient than {@link #get} because it:
   * <ul>
   * <li>Waits for the result uninterruptibly, and so doesn't throw
   * {@link InterruptedException}.</li>
   * <li>Returns meaningful exceptions, instead of having to deal with
   * ExecutionException.</li>
   * </ul>
   * As such, it is the preferred way to get the future result.
   *
   * @author paouelle
   *
   * @param  timeout the timeout to wait for
   * @param  unit the units for the timeout
   * @return the non-<code>null</code> object set
   * @throws NoHostAvailableException if no host in the cluster can be contacted
   *         successfully to execute this query.
   * @throws QueryExecutionException if the query triggered an execution
   *         exception, that is an exception thrown by Cassandra when it
   *         cannot execute the query with the requested consistency level
   *         successfully.
   * @throws ObjectNotFoundException if the statement is a select and the
   *         keyspace specified doesn't exist
   * @throws QueryValidationException if the query if invalid (syntax error,
   *         unauthorized or any other validation problem).
   * @throws TimeoutException if the wait timed out (Note that this is different
   *         from a Cassandra timeout, which is a
   *         {@code QueryExecutionException}).
   */
  public ObjectSet<T> getUninterruptibly(long timeout, TimeUnit unit)
    throws TimeoutException {
    try {
      final ResultSet result = future.getUninterruptibly(timeout, unit);

      postProcess(result);
      return new ObjectSet<>(context, result);
    } catch (InvalidQueryException e) {
      ObjectNotFoundException.handleKeyspaceNotFound(context.getObjectClass(), e);
      throw e;
    }
  }

  /**
   * {@inheritDoc}
   *
   * Attempts to cancel the execution of the request corresponding to this
   * future. This attempt will fail if the request has already returned.
   * <p>
   * Please note that this only cancel the request driver side, but nothing
   * is done to interrupt the execution of the request Cassandra side (and that even
   * if {@code mayInterruptIfRunning} is true) since Cassandra does not
   * support such interruption.
   * <p>
   * This method can be used to ensure no more work is performed driver side
   * (which, while it doesn't include stopping a request already submitted
   * to a Cassandra node, may include not retrying another Cassandra host on
   * failure/timeout) if the object set is not going to be retried. Typically,
   * the code to wait for a request result for a maximum of 1 second could
   * look like:
   * <pre>
   *   final ObjectSetFuture&lt;T&gt; future = statement.executeAsync();
   *
   *   try {
   *       final ObjectSet&lt;T&gt; result = future.get(1, TimeUnit.SECONDS);
   *       ... process result ...
   *   } catch (TimeoutException e) {
   *       future.cancel(true); // Ensure any ressource used by this query driver
   *                            // side is released immediately
   *       ... handle timeout ...
   *   }
   * </pre>
   *
   * @param  mayInterruptIfRunning the value of this parameter is currently
   *         ignored.
   * @return <code>false</code> if the future could not be cancelled (it has already
   *         completed normally); <code>true</code> otherwise.
   *
   * @see com.google.common.util.concurrent.AbstractFuture#cancel(boolean)
   */
  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    return future.cancel(mayInterruptIfRunning);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.google.common.util.concurrent.AbstractFuture#isDone()
   */
  @Override
  public boolean isDone() {
    return future.isDone();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.google.common.util.concurrent.AbstractFuture#isCancelled()
   */
  @Override
  public boolean isCancelled() {
    return future.isCancelled();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.google.common.util.concurrent.AbstractFuture#addListener(Runnable, java.util.concurrent.Executor)
   */
  @Override
  public void addListener(Runnable listener, Executor exec) {
    future.addListener(listener, exec);
  }
}

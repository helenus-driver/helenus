/*
 * Copyright (C) 2015-2017 The Helenus Driver Project Authors.
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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.datastax.driver.core.exceptions.QueryValidationException;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;

import org.helenus.driver.ObjectNotFoundException;
import org.helenus.driver.ObjectSet;
import org.helenus.driver.StatementManager;

/**
 * The <code>ListenableFutureImpl</code> class extends Cassandra's
 * {@link com.datastax.driver.core.ResultSetFuture} in order to provide
 * support for POJOs.
 *
 * @copyright 2015-2017 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 4, 2017 - paouelle - abstracted out from ObjectSetFutureImpl
 *
 * @param <T> The type of POJO associated with the future object set
 *
 * @since 1.0
 */
public class ListenableFutureImpl<T> extends AbstractFuture<ObjectSet<T>> {
  /**
   * Holds the statement context associated with this object set.
   *
   * @author paouelle
   */
  protected final StatementManager.Context<T> context;

  /**
   * Holds the raw result set future.
   *
   * @author paouelle
   */
  private final ListenableFuture<ResultSet> future;

  /**
   * Instantiates a new <code>ListenableFutureImpl</code> object.
   *
   * @author paouelle
   *
   * @param context the non-<code>null</code> statement context associated with
   *        this future object set
   * @param future the non-<code>null</code> result set future
   */
  ListenableFutureImpl(
    StatementManager.Context<T> context, ListenableFuture<ResultSet> future
  ) {
    this.context = context;
    this.future = future;
  }

  /**
   * Post-process the result set. The returned {@link ObjectSetImpl} will be based
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
      return new ObjectSetImpl<>(context, result);
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
      return new ObjectSetImpl<>(context, result);
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

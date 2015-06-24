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
package org.helenus.driver;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.datastax.driver.core.exceptions.QueryValidationException;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * The <code>ObjectSetFuture</code> class extends on Cassandra's
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
public interface ObjectSetFuture<T> extends ListenableFuture<ObjectSet<T>> {
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
   * @see java.util.concurrent.Future#get(long, java.util.concurrent.TimeUnit)
   */
  @Override
  public ObjectSet<T> get(long timeout, TimeUnit unit)
    throws InterruptedException, TimeoutException, ExecutionException;

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
   * @see java.util.concurrent.Future#get()
   */
  @Override
  public ObjectSet<T> get() throws InterruptedException, ExecutionException;

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
  public ObjectSet<T> getUninterruptibly();

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
    throws TimeoutException;

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
   * @see java.util.concurrent.Future#cancel(boolean)
   */
  @Override
  public boolean cancel(boolean mayInterruptIfRunning);
}

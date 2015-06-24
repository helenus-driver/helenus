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
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.datastax.driver.core.exceptions.QueryValidationException;
import com.google.common.util.concurrent.AbstractFuture;

/**
 * The <code>VoidFuture</code> class extends Cassandra's
 * {@link com.datastax.driver.core.ResultSetFuture} in order to provide
 * support for {@link BatchableStatement} which don't return anything.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 15, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
public class VoidFuture extends AbstractFuture<Void> {
  /**
   * The <code>PostProcessor</code> interface defines a post-processor for s
   * given future result set.
   *
   * @copyright 2015-2015 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 15, 2015 - paouelle - Creation
   *
   * @since 1.0
   */
  public interface PostProcessor {
    /**
     * Post-process the result set.
     *
     * @author paouelle
     *
     * @param result the non-<code>null</code> result set to be post processed
     */
    public void postProcess(ResultSet result);
  }

  /**
   * Holds the raw result set future.
   *
   * @author paouelle
   */
  private final ResultSetFuture future;

  /**
   * Holds the optional post processor.
   *
   * @author paouelle
   */
  private final PostProcessor processor;

  /**
   * Instantiates a new <code>VoidFuture</code> object.
   *
   * @author paouelle
   *
   * @param future the non-<code>null</code> result set future
   */
  VoidFuture(ResultSetFuture future) {
    this(future, null);
  }

  /**
   * Instantiates a new <code>VoidFuture</code> object.
   *
   * @author paouelle
   *
   * @param future the non-<code>null</code> result set future
   * @param processor the optional post processor
   */
  VoidFuture(ResultSetFuture future, PostProcessor processor) {
    this.future = future;
    this.processor = processor;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.google.common.util.concurrent.AbstractFuture#get(long, java.util.concurrent.TimeUnit)
   */
  @Override
  public Void get(long timeout, TimeUnit unit)
    throws InterruptedException, TimeoutException, ExecutionException {
    final ResultSet result = future.get(timeout, unit);

    if (processor != null) {
      processor.postProcess(result);
    }
    return null;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.google.common.util.concurrent.AbstractFuture#get()
   */
  @Override
  public Void get() throws InterruptedException, ExecutionException {
    final ResultSet result = future.get();

    if (processor != null) {
      processor.postProcess(result);
    }
    return null;
  }

  /**
   * Waits for the query to return .
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
   * @return void
   * @throws NoHostAvailableException if no host in the cluster can be contacted
   *         successfully to execute this query.
   * @throws QueryExecutionException if the query triggered an execution
   *         exception, that is an exception thrown by Cassandra when it
   *         cannot execute the query with the requested consistency level
   *         successfully.
   * @throws UpdateNotAppliedException if the query is a conditional update
   *         that failed to be applied
   * @throws ObjectExistException if the statement is a conditional insert
   *         that failed to be applied
   * @throws QueryValidationException if the query is invalid (syntax error,
   *         unauthorized or any other validation problem).
   */
  public Void getUninterruptibly() {
    final ResultSet result = future.getUninterruptibly();

    if (processor != null) {
      processor.postProcess(result);
    }
    return null;
  }

  /**
   * Waits for the provided time for the query to return.
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
   * @return void
   * @throws NoHostAvailableException if no host in the cluster can be contacted
   *         successfully to execute this query.
   * @throws QueryExecutionException if the query triggered an execution
   *         exception, that is an exception thrown by Cassandra when it
   *         cannot execute the query with the requested consistency level
   *         successfully.
   * @throws UpdateNotAppliedException if the query is a conditional update
   *         that failed to be applied
   * @throws ObjectExistException if the statement is a conditional insert
   *         that failed to be applied
   * @throws QueryValidationException if the query if invalid (syntax error,
   *         unauthorized or any other validation problem).
   * @throws TimeoutException if the wait timed out (Note that this is different
   *         from a Cassandra timeout, which is a
   *         {@code QueryExecutionException}).
   */
  public Void getUninterruptibly(long timeout, TimeUnit unit)
    throws TimeoutException {
    final ResultSet result = future.getUninterruptibly(timeout, unit);

    if (processor != null) {
      processor.postProcess(result);
    }
    return null;
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
   *   final VoidFuture future = statement.executeAsync();
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

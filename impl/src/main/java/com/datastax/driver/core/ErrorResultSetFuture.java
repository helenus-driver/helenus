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
package com.datastax.driver.core;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.DriverInternalError;
import com.google.common.util.concurrent.ExecutionList;

import org.helenus.driver.StatementManager;

/**
 * The <code>ErrorResultSetFuture</code> class extends Cassandra
 * {@link ResultSetFuture} in order to provide an error result set when an
 * error occurred processing a intermediate query.
 *
 * @copyright 2015-2016 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 26, 2016 - paouelle - Creation
 *
 * @since 1.0
 */
public class ErrorResultSetFuture extends DefaultResultSetFuture {
  /**
   * Finalized executor list used to dispatch to added listeners right away.
   *
   * @author paouelle
   */
  private static final ExecutionList execution = new ExecutionList();

  static {
    // make sure the state of the execution list is executed
    // such that any listeners added will simply result in a direct
    // execution of the listener
    ErrorResultSetFuture.execution.execute();
  }

  /**
   * Holds the error that occurred.
   *
   * @author paouelle
   */
  private final Throwable error;

  /**
   * Instantiates a new <code>ErrorResultSetFuture</code> object.
   *
   * @author paouelle
   *
   * @param  mgr the statement manager
   * @param  error the error that occurred
   * @throws NullPointerException if <code>mgr</code> or <code>error</code> is
   *         <code>null</code>
   */
  public ErrorResultSetFuture(StatementManager mgr, Throwable error) {
    super(
      null,
      mgr.getCluster().getConfiguration().getProtocolOptions().getProtocolVersionEnum(),
      null
    );
    org.apache.commons.lang3.Validate.notNull(mgr, "invalid null mgr"); // will never be reached!
    org.apache.commons.lang3.Validate.notNull(error, "invalid null error");
    this.error = error;
  }

  /**
   * Propagates the original error properly wrapped.
   *
   * @author paouelle
   *
   * @return nothing as an exception is always thrown
   * @throws Error if an error occurred
   * @throws DriverException if a driver exception occurred
   * @throws DriverInternalError if any other error occurred
   */
  RuntimeException propagateError() {
    if (error instanceof Error) {
      throw ((Error)error);
    }
    // We could just rethrow error. However, the cause of the ExecutionException has likely been
    // created on the I/O thread receiving the response. Which means that the stacktrace associated
    // with said cause will make no mention of the current thread. This is painful for say, finding
    // out which execute() statement actually raised the exception. So instead, we re-create the
    // exception.
    if (error instanceof DriverException) {
      throw ((DriverException)error).copy();
    }
    throw new DriverInternalError("Unexpected exception thrown", error);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.datastax.driver.core.ResultSetFuture#getUninterruptibly()
   */
  @Override
  public ResultSet getUninterruptibly() {
    throw propagateError();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.datastax.driver.core.ResultSetFuture#getUninterruptibly(long, java.util.concurrent.TimeUnit)
   */
  @Override
  public ResultSet getUninterruptibly(long timeout, TimeUnit unit)
    throws TimeoutException {
    throw propagateError();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.datastax.driver.core.ResultSetFuture#cancel(boolean)
   */
  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    return false; // nothing to cancel; already failed
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.google.common.util.concurrent.AbstractFuture#get(long, java.util.concurrent.TimeUnit)
   */
  @Override
  public ResultSet get(long timeout, TimeUnit unit)
    throws InterruptedException, TimeoutException, ExecutionException {
    throw propagateError();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.google.common.util.concurrent.AbstractFuture#get()
   */
  @Override
  public ResultSet get() throws InterruptedException, ExecutionException {
    throw propagateError();
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
    return true; // nothing to do so already done
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
    return false; // nothing to do so assume never cancelled but done
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.google.common.util.concurrent.AbstractFuture#addListener(java.lang.Runnable, java.util.concurrent.Executor)
   */
  @Override
  public void addListener(Runnable listener, Executor exec) {
    // since such an error result is always done by design, we simulate what is
    // normally done by an execution list which is to call the listener right
    ErrorResultSetFuture.execution.add(listener, exec);
  }
}

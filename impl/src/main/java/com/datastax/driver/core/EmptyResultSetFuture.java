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

import com.google.common.util.concurrent.ExecutionList;

import com.github.helenus.driver.StatementManager;

/**
 * The <code>EmptyResultSetFuture</code> class extends Cassandra
 * {@link ResultSetFuture} in order to provide an empty result set when no
 * query needed to be sent.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 19, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
public class EmptyResultSetFuture extends DefaultResultSetFuture {
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
    EmptyResultSetFuture.execution.execute();
  }

  /**
   * Holds a generic empty result set.
   *
   * @author paouelle
   */
  private final ResultSet empty;

  /**
   * Instantiates a new <code>EmptyResultSetFuture</code> object.
   *
   * @author paouelle
   *
   * @param  mgr the statement manager
   * @throws NullPointerException if <code>mgr</code> is <code>null</code>
   */
  public EmptyResultSetFuture(StatementManager mgr) {
    super(
      null,
      mgr.getCluster().getConfiguration().getProtocolOptions().getProtocolVersion(),
      null
    );
    org.apache.commons.lang3.Validate.notNull(mgr, "invalid null mgr"); // will never be reached!
    this.empty = ArrayBackedResultSet.fromMessage(
      new Responses.Result(Responses.Result.Kind.VOID) {}, // VOID to force an empty result
      null,
      mgr.getCluster().getConfiguration().getProtocolOptions().getProtocolVersion(),
      null,
      null
    );
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
    return empty;
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
    return empty;
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
    return true; // nothing to cancel
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
    return empty;
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
    return empty;
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
    return true; // nothing to do so assume always cancelled
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
    // since such an empty result is always done by design, we simulate what is
    // normally done by an execution list which is to call the listener right
    EmptyResultSetFuture.execution.add(listener, exec);
  }
}

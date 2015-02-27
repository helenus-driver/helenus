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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.github.helenusdriver.driver.StatementManager;
import com.google.common.util.concurrent.ExecutionList;

/**
 * The <code>LastResultSetFuture</code> class defines a result set which is
 * designed to execute multiple statements in a sequence and return only the
 * result set from the last statement.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 19, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
public class LastResultSetFuture extends DefaultResultSetFuture {
  /**
   * Holds a direct executor used for future listeners
   * registered internally.
   *
   * @author paouelle
   */
  private static final Executor DIRECT = new Executor() {
    @Override
    public void execute(Runnable r) {
      r.run();
    }
  };

  /**
   * Holds the statement manager.
   *
   * @author paouelle
   */
  private final StatementManager mgr;

  /**
   * Holds the statements to execute.
   *
   * @author paouelle
   */
  private final Iterator<SimpleStatement> statements;

  /**
   * The execution list to hold our executors.
   *
   * @author paouelle
   */
  private final ExecutionList executionList = new ExecutionList();

  /**
   * Holds the future result of the currently executing statement.
   *
   * @author paouelle
   */
  private ResultSetFuture future = null;

  /**
   * Holds the result listener used to detect when a statement is done
   * executing.
   *
   * @author paouelle
   */
  private final Runnable listener = new Runnable() {
    @SuppressWarnings("synthetic-access")
    @Override
    public void run() {
      boolean execute = false;;

      try {
        synchronized (LastResultSetFuture.this.statements) {
          // notified that the current future is done (success, error, or cancelled)
          // we need to execute out own listeners only if there was an error or cancellation
          // or if this was the last statement to execute
          if (LastResultSetFuture.this.future.isCancelled()) {
            // already cancelled so bail out
            // leaving the future intact for our own clients
            execute = true; // notify out listeners
            return;
          }
          if (!LastResultSetFuture.this.future.isDone()) {
            // not done yet so bail out
            // leaving the future intact for our own clients
            return;
          }
          try {
            // test the current future result, that is the only way to detect if
            // it completed successfully or if it failed with an exception
            LastResultSetFuture.this.future.get();
          } catch (Error|Exception e) {
            // ignore and return leaving the future intact for own own clients
            execute = true; // notify out listeners
            return;
          }
          // move on the the next if any
          if (LastResultSetFuture.this.statements.hasNext()) {
            LastResultSetFuture.this.future = mgr.getSession().executeAsync(
              LastResultSetFuture.this.statements.next()
            );
            LastResultSetFuture.this.future.addListener(
              LastResultSetFuture.this.listener,
              LastResultSetFuture.DIRECT
            );
          } else { // that was the last statement in the sequence
            // leave the last future intact for our own clients
            execute = true; // notify out listeners
          }
        }
      } finally {
        if (execute) { // do outside of lock
          executionList.execute();
        }
      }
    }
  };

  /**
   * Instantiates a new <code>LastResultSetFuture</code> object.
   *
   * @author paouelle
   *
   * @param  statements the list of statements to execute in the specified order
   * @param  mgr the statement manager
   * @throws NullPointerException if <code>mgr</code>, <code>statements</code>
   *         or any of the statements are <code>null</code>
   */
  public LastResultSetFuture(
    List<SimpleStatement> statements, StatementManager mgr
  ) {
    super(
      null,
      mgr.getCluster().getConfiguration().getProtocolOptions().getProtocolVersionEnum(),
      null
    );
    org.apache.commons.lang3.Validate.notNull(mgr, "invalid null mgr"); // will never be reached!
    org.apache.commons.lang3.Validate.notNull(statements, "invalid null statements");
    this.mgr = mgr;
    final List<SimpleStatement> ss = new ArrayList<>(statements.size());

    for (final SimpleStatement s: statements) {
      org.apache.commons.lang3.Validate.notNull(s, "invalid null statement");
      ss.add(s);
    }
    this.statements = ss.iterator();
    // execute the first one to get things going
    if (this.statements.hasNext()) {
      this.future = mgr.getSession().executeAsync(this.statements.next());
      this.future.addListener(listener, LastResultSetFuture.DIRECT);
    }
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
    synchronized (statements) {
      return future.isDone() && !statements.hasNext();
    }
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
    synchronized (statements) {
      return future.isCancelled();
    }
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
    synchronized (statements) {
      return future.cancel(mayInterruptIfRunning);
    }
  }

  /**
   * {@inheritDoc}
   * <p>
   * <i>Note:</i> This method will finish executing all remaining statements
   * until one generates an error and return the result set from that last one
   * or until there is no more time left.
   *
   * @author paouelle
   *
   * @see com.google.common.util.concurrent.AbstractFuture#get(long, java.util.concurrent.TimeUnit)
   */
  @Override
  public ResultSet get(long timeout, TimeUnit unit)
    throws InterruptedException, TimeoutException, ExecutionException {
    final long end = System.nanoTime() + unit.toNanos(timeout);
    ResultSetFuture future;

    while (true) {
      synchronized (statements) {
        future = this.future;
        // note that our listener above will actually be executing the next
        // statements automatically
        if (!statements.hasNext()) {
          break;
        }
      }
      // note that our listener above will actually be executing the next
      // statements automatically
      // --- Future treats negative timeouts just like zero.
      future.get(end - System.nanoTime(), TimeUnit.NANOSECONDS);
    }
    return future.get(end - System.nanoTime(), TimeUnit.NANOSECONDS);
  }

  /**
   * {@inheritDoc}
   * <p>
   * <i>Note:</i> This method will finish executing all remaining statements
   * until one generates an error and return the result set from that last one.
   *
   * @author paouelle
   *
   * @see com.google.common.util.concurrent.AbstractFuture#get()
   */
  @Override
  public ResultSet get() throws InterruptedException, ExecutionException {
    ResultSetFuture future;

    while (true) {
      synchronized (statements) {
        future = this.future;
        // note that our listener above will actually be executing the next
        // statements automatically
        if (!statements.hasNext()) {
          break;
        }
      }
      future.get();
    }
    return future.get();
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
    executionList.add(listener, exec);
  }
}

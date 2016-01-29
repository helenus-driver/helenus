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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.util.concurrent.ExecutionList;

import org.helenus.driver.impl.GroupStatementImpl;
import org.helenus.driver.impl.SequenceStatementImpl;
import org.helenus.driver.impl.StatementImpl;
import org.helenus.driver.impl.StatementManagerImpl;
import org.helenus.util.stream.Collectors;

/**
 * The <code>LastResultParallelSetFuture</code> class defines a result set
 * which is designed to execute multiple statements in parallel and return
 * only the result set from the last statement.
 *
 * @copyright 2015-2016 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 26, 2016 - paouelle - Creation
 *
 * @since 1.0
 */
public class LastResultParallelSetFuture extends DefaultResultSetFuture {
  /**
   * Holds the statements to execute. Each entries in the iterator is a list of
   * statements to execute in parallel.
   *
   * @author paouelle
   */
  private final Iterator<List<StatementImpl<?, ?, ?>>> statements;

  /**
   * The execution list to hold our executors.
   *
   * @author paouelle
   */
  private final ExecutionList executionList = new ExecutionList();

  /**
   * Holds the statement manager.
   *
   * @author paouelle
   */
  private final StatementManagerImpl mgr;

  /**
   * Holds the listenable futures with the corresponding executing statements.
   *
   * @author paouelle
   */
  private Map<ResultSetFuture, StatementImpl<?, ?, ?>> futures = null;

  /**
   * Holds the first listenable future that reported it was cancelled or failed.
   *
   * @author paouelle
   */
  private ResultSetFuture error = null;

  /**
   * Holds the last listenable future that reported a successful result.
   *
   * @author paouelle
   */
  private ResultSetFuture success = null;

  /**
   * Holds a flag indicating the result set was cancelled.
   *
   * @author paouelle
   */
  private boolean cancelled = false;

  /**
   * Holds the result listener used to detect when a statement is done
   * executing.
   *
   * @author paouelle
   */
  private class Listener implements Runnable {
    /**
     * Holds the future associated with this listener.
     *
     * @author paouelle
     */
    private final ResultSetFuture future;

    /**
     * Instantiates a new <code>Listener</code> object.
     *
     * @author paouelle
     *
     * @param future the non-<code>null</code> future associated with this
     *        listener
     */
    public Listener(ResultSetFuture future) {
      this.future = future;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see java.lang.Runnable#run()
     */
    @SuppressWarnings("synthetic-access")
    @Override
    public void run() {
      boolean execute = false;

      try {
        synchronized (statements) {
          // notified that a current future is done (success, error, or cancelled)
          // start by removing the future from our map
          final StatementImpl<?, ?, ?> statement = futures.remove(future);

          if (error != null) {
            // ignore this result since we already failed or were cancelled
            statements.notifyAll(); // wake up anything in case
            return;
          }
          // we need to execute our own listeners only if there was an error or cancellation
          // or if this was the last statement to execute
          if (future.isCancelled()) {
            // already cancelled so bail out
            // setting the error future for our own clients
            LastResultParallelSetFuture.this.error = future;
            LastResultParallelSetFuture.this.success = null;
            execute = true; // notify our listeners
            statements.notifyAll(); // wake up anything in case
            return;
          }
          if (!future.isDone()) { // this should never happen!!!
            // not done yet so bail out
            // leaving the error & success future intact for our own clients
            futures.put(future, statement); // put it back!!!
            return;
          }
          try {
            // test the current future result, that is the only way to detect if
            // it completed successfully or if it failed with an exception
            future.get();
          } catch (ThreadDeath|OutOfMemoryError|StackOverflowError|AssertionError e) {
            // ignore and return leaving the future intact for our own clients
            LastResultParallelSetFuture.this.error = future;
            LastResultParallelSetFuture.this.success = null;
            execute = true; // notify our listeners
            statements.notifyAll(); // wake up anything in case
            throw e;
          } catch (Error|Exception e) {
            // ignore and return leaving the future intact for our own clients
            // to detect this error
            LastResultParallelSetFuture.this.error = future;
            LastResultParallelSetFuture.this.success = null;
            execute = true; // notify our listeners
            statements.notifyAll(); // wake up anything in case
          }
          LastResultParallelSetFuture.this.success = future;
          // move on to the next if we are done with this set
          if (futures.isEmpty()) {
            if (statements.hasNext()) {
              try {
                LastResultParallelSetFuture.this.futures = statements.next().stream()
                  .collect(Collectors.toIdentityMap(
                    s -> {
                      final ResultSetFuture f = s.executeAsyncRaw();

                      f.addListener(new Listener(f), mgr.getPoolExecutor());
                      return f;
                    }, s -> s
                  ));
              } catch (ThreadDeath|OutOfMemoryError|StackOverflowError|AssertionError e) {
                // hum! we need to propagate this one into an error result future
                LastResultParallelSetFuture.this.error = new ErrorResultSetFuture(mgr, e);
                LastResultParallelSetFuture.this.success = null;
                execute = true; // notify our listeners
                statements.notifyAll(); // wake up anything in case
                throw e;
              } catch (Error|Exception e) {
                // hum! we need to propagate this one into an error result future
                LastResultParallelSetFuture.this.error = new ErrorResultSetFuture(mgr, e);
                LastResultParallelSetFuture.this.success = null;
                execute = true; // notify our listeners
                statements.notifyAll(); // wake up anything in case
              }
            } else { // that was the last set of statements in the group
              // leave the success future intact for our own clients
              execute = true; // notify our listeners
              statements.notifyAll(); // wake up anything in case
            }
          } // else - still waiting for results from this current set
        }
      } finally {
        if (execute) { // do outside of lock
          executionList.execute();
        }
      }
    }
  };

  /**
   * Instantiates a new <code>LastResultSequentialSetFuture</code> object.
   *
   * @author paouelle
   *
   * @param  group the group statement to execute the statements for
   * @param  statements the list of statements to execute
   * @param  mgr the statement manager
   * @throws NullPointerException if <code>mgr</code>, <code>group</code>,
   *         <code>statements</code>, or any of the statements are
   *         <code>null</code>
   */
  public LastResultParallelSetFuture(
    GroupStatementImpl<?, ?, ?> group,
    List<StatementImpl<?, ?, ?>> statements,
    StatementManagerImpl mgr
  ) {
    super(
      null,
      mgr.getCluster().getConfiguration().getProtocolOptions().getProtocolVersionEnum(),
      null
    );
    org.apache.commons.lang3.Validate.notNull(statements, "invalid null statements");
    this.mgr = mgr;
    // start splitting the list in multiple lists where each lists either ends
    // with a sequence statement or reaches the parallel factor for the group
    // statement
    final List<List<StatementImpl<?, ?, ?>>> slist = new LinkedList<>();
    final int factor = group.getParallelFactor();
    List<StatementImpl<?, ?, ?>> cslist = new ArrayList<>(factor);

    for (final StatementImpl<?, ?, ?> s: statements) {
      org.apache.commons.lang3.Validate.notNull(s, "invalid null statement");
      cslist.add(s);
      if ((cslist.size() == factor) || (s instanceof SequenceStatementImpl)) {
        slist.add(cslist);
        cslist = new ArrayList<>(factor);
      }
    }
    if (!cslist.isEmpty()) {
      slist.add(cslist);
    }
    this.statements = slist.iterator();
    // execute the first list to get things going
    synchronized (this.statements) { // sync to prevent handling listener's callbacks until we are done
      this.futures = this.statements.next().stream()
        .collect(Collectors.toIdentityMap(
          s -> {
            final ResultSetFuture f = s.executeAsyncRaw();

            f.addListener(new Listener(f), mgr.getPoolExecutor());
            return f;
          }, s -> s
        ));
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
      return (error != null) || ((success != null) && !statements.hasNext());
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
      return cancelled || ((error != null) && error.isCancelled());
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
    boolean execute = false;

    try {
      synchronized (statements) {
        if (cancelled) {
          return false;
        }
        execute = true;
        this.cancelled = true;
        statements.notifyAll(); // wake up anything in case
        boolean did = false; // until proven otherwise

        try {
          for (final ResultSetFuture f: futures.keySet()) {
            if (f.cancel(mayInterruptIfRunning)) {
              did = true;
            }
          }
          // if we cancelled one or if we had more to execute then we did cancel
          return did || statements.hasNext();
        } finally {
          while (statements.hasNext()) { // empty out the iterator of statements
            statements.next();
          }
        }
      }
    } finally {
      if (execute) { // do outside of lock
        executionList.execute();
      }
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

    synchronized (statements) {
      while (true) {
        if (cancelled) {
          throw new CancellationException();
        }
        if (error != null) {
          future = error;
          break;
        }
        // note that our listener above will actually be executing the next
        // statements automatically
        if (futures.isEmpty() && !statements.hasNext()) {
          future = success;
          break;
        }
        // wait for something to happen!!!
        // note that our listener above will actually be executing the next
        // statements automatically
        final long duration = end - System.nanoTime();

        if (duration <= 0L) {
          throw new TimeoutException("timeout waiting for last result");
        }
        TimeUnit.NANOSECONDS.timedWait(statements, duration);
      }
    }
    // --- Future treats negative timeouts just like zero.
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

    synchronized (statements) {
      while (true) {
        if (cancelled) {
          throw new CancellationException();
        }
        if (error != null) {
          future = error;
          break;
        }
        if (futures.isEmpty() && !statements.hasNext()) {
          future = success;
          break;
        }
        // wait for something to happen!!!
        // note that our listener above will actually be executing the next
        // statements automatically
        statements.wait();
      }
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

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
import java.util.BitSet;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import com.google.common.util.concurrent.ExecutionList;
import com.google.common.util.concurrent.ListenableFuture;

import org.helenus.driver.impl.StatementManagerImpl;

/**
 * The <code>CompoundResultSetFuture</code> class defines a result set which is
 * designed to return a compound results of multiple result set futures which
 * are created from a sequence of select statements and requires to be combined
 * together.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Mar 8, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
public class CompoundResultSetFuture extends DefaultResultSetFuture {
  /**
   * The execution list to hold our executors.
   *
   * @author paouelle
   */
  private final ExecutionList executionList = new ExecutionList();

  /**
   * Holds the future result sets.
   *
   * @author paouelle
   */
  private final List<ResultSetFuture> futures;

  /**
   * Holds flags for each futures indicating if the listener was called.
   *
   * @author paouelle
   */
  private final BitSet called;

  /**
   * Instantiates a new <code>CompoundResultSetFuture</code> object.
   *
   * @author paouelle
   *
   * @param  futures the list of result set futures to compound together
   * @param  mgr the statement manager
   * @throws NullPointerException if <code>mgr</code>, <code>statements</code>
   *         or any of the result set futures are <code>null</code>
   */
  public CompoundResultSetFuture(
    List<ResultSetFuture> futures, StatementManagerImpl mgr
  ) {
    super(
      null,
      mgr.getCluster().getConfiguration().getProtocolOptions().getProtocolVersionEnum(),
      null
    );
    org.apache.commons.lang3.Validate.notNull(mgr, "invalid null mgr"); // will never be reached!
    org.apache.commons.lang3.Validate.notNull(futures, "invalid null result set futures");
    final List<ResultSetFuture> rsets = new ArrayList<>(futures.size());

    this.called = new BitSet(futures.size());
    for (int i = 0; i < futures.size(); i++) {
      final ResultSetFuture rset = futures.get(i);

      org.apache.commons.lang3.Validate.notNull(rset, "invalid null result set future");
      rsets.add(rset);
      called.set(i);
    }
    this.futures = rsets;
    for (int i = 0; i < rsets.size(); i++) {
      final ResultSetFuture rset = rsets.get(i);
      final int index = i;

      rset.addListener(new Runnable() {
        @SuppressWarnings("synthetic-access")
        @Override
        public void run() {
          boolean execute = false;

          try {
            synchronized (called) {
              called.clear(index);
              if (called.isEmpty()) {
                execute = true; // notify out listeners
              }
            }
          } finally {
            if (execute) { // do outside of lock
              executionList.execute();
            }
          }
        }
      }, mgr.getDirectExecutor());
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
    synchronized (futures) {
      for (final ResultSetFuture future: futures) {
        if (!future.isDone()) {
          return false;
        }
      }
    }
    return true;
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
    synchronized (futures) {
      for (final ResultSetFuture future: futures) {
        if (!future.isCancelled()) {
          return false;
        }
      }
    }
    return true;
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
    boolean cancelled = true; // until proven otherwise

    synchronized (futures) {
      for (final ResultSetFuture future: futures) {
        if (!future.cancel(mayInterruptIfRunning)) {
          cancelled = false;
        }
      }
    }
    return cancelled;
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
    final long end = System.nanoTime() + unit.toNanos(timeout);
    final List<ResultSet> results = new ArrayList<>(futures.size());

    synchronized (futures) {
      for (final ResultSetFuture future: futures) {
        final ResultSet result = future.get(end - System.nanoTime(), TimeUnit.NANOSECONDS);

        results.add(result);
      }
    }
    return new CompoundResultSet(results);
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
    final List<ResultSet> results = new ArrayList<>(futures.size());

    synchronized (futures) {
      for (final ResultSetFuture future: futures) {
        results.add(future.get());
      }
    }
    return new CompoundResultSet(results);
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

  /**
   * The <code>CompoundResultSet</code> class is used to compound together
   * multiple result sets.
   *
   * @copyright 2015-2015 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Mar 8, 2015 - paouelle - Creation
   *
   * @since 1.0
   */
  private static class CompoundResultSet implements ResultSet {
    /**
     * Holds the result sets being compounded.
     *
     * @author paouelle
     */
    private final List<ResultSet> results;

    /**
     * Holds the current result set from which to retrieve results.
     *
     * @author paouelle
     */
    private int i = 0;

    /**
     * Instantiates a new <code>CompoundResultSet</code> object.
     *
     * @author paouelle
     *
     * @param results the non-<code>null</code> and non-empty list of result
     *        sets to compound together
     */
    CompoundResultSet(List<ResultSet> results) {
      this.results = results;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.datastax.driver.core.ResultSet#getColumnDefinitions()
     */
    @Override
    public ColumnDefinitions getColumnDefinitions() {
      return results.get(0).getColumnDefinitions();
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.datastax.driver.core.ResultSet#isExhausted()
     */
    @Override
    public boolean isExhausted() {
      if (i >= results.size()) {
        return true;
      }
      ResultSet current = results.get(i);

      while (current.isExhausted()) {
        if (++i >= results.size()) {
          return true;
        }
        current = results.get(i);
      }
      return false;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.datastax.driver.core.ResultSet#one()
     */
    @Override
    public Row one() {
      if (isExhausted()) {
        return null;
      }
      return results.get(i).one();
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.datastax.driver.core.ResultSet#all()
     */
    @Override
    public List<Row> all() {
      if (isExhausted()) {
        return Collections.emptyList();
      }
      final List<Row> rows = new ArrayList<>(results.get(i).all());

      while (!isExhausted()) {
        rows.addAll(results.get(i).all());
      }
      return rows;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.datastax.driver.core.ResultSet#iterator()
     */
    @Override
    public Iterator<Row> iterator() {
      return new Iterator<Row>() {
        @Override
        public boolean hasNext() {
          return !isExhausted();
        }
        @SuppressWarnings("synthetic-access")
        @Override
        public Row next() {
          if (isExhausted()) {
            throw new NoSuchElementException();
          }
          return results.get(i).one();
        }
      };
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.datastax.driver.core.ResultSet#getAvailableWithoutFetching()
     */
    @Override
    public int getAvailableWithoutFetching() {
      if (isExhausted()) {
        return 0;
      }
      int num = 0;
      int j = i;

      while (true) {
        final ResultSet current = results.get(j);

        num += current.getAvailableWithoutFetching();
        if (!current.isFullyFetched() || (++j >= results.size())) {
          break;
        }
      }
      return num;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.datastax.driver.core.ResultSet#isFullyFetched()
     */
    @Override
    public boolean isFullyFetched() {
      if (!isExhausted()) {
        int j = i;

        do {
          if (!results.get(j).isFullyFetched()) {
            return false;
          }
        } while (++j < results.size());
      }
      return true;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.datastax.driver.core.ResultSet#fetchMoreResults()
     */
    @Override
    public ListenableFuture<Void> fetchMoreResults() {
      if (!isExhausted()) {
        int j = i;

        do {
          final ResultSet current = results.get(j);

          if (!current.isFullyFetched()) {
            return current.fetchMoreResults();
          }
        } while (++j < results.size());
      }
      // all done so just call the first result set one to be able
      // to return a listenable future
      return results.get(0).fetchMoreResults();
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.datastax.driver.core.ResultSet#getExecutionInfo()
     */
    @Override
    public ExecutionInfo getExecutionInfo() {
      if (isExhausted()) { // return the one from the last result set
        return results.get(results.size() - 1).getExecutionInfo();
      }
      // otherwise return the one for the current result set
      return results.get(i).getExecutionInfo();
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.datastax.driver.core.ResultSet#getAllExecutionInfo()
     */
    @Override
    public List<ExecutionInfo> getAllExecutionInfo() {
      return results.stream()
        .flatMap(r -> r.getAllExecutionInfo().stream())
        .collect(Collectors.toList());
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.datastax.driver.core.ResultSet#wasApplied()
     */
    @Override
    public boolean wasApplied() {
      return true;
    }
  }
}

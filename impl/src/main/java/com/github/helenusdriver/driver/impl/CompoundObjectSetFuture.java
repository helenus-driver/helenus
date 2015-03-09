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
package com.github.helenusdriver.driver.impl;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ExecutionInfo;
import com.github.helenusdriver.driver.ObjectSet;
import com.github.helenusdriver.driver.ObjectSetFuture;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ExecutionList;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * The <code>CompoundObjectSetFuture</code> class defines an object set which is
 * designed to return a compound results of multiple object set futures which
 * are created from a sequence of select statements and requires to be combined
 * together.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Mar 9, 2015 - paouelle - Creation
 *
 * @param <T> The type of POJO associated with the future object set
 *
 * @since 1.0
 */
public class CompoundObjectSetFuture<T>
  extends AbstractFuture<ObjectSet<T>>
  implements ObjectSetFuture<T> {
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
   * The execution list to hold our executors.
   *
   * @author paouelle
   */
  private final ExecutionList executionList = new ExecutionList();

  /**
   * Holds the future object sets.
   *
   * @author paouelle
   */
  private final List<ObjectSetFuture<T>> futures;

  /**
   * Holds flags for each futures indicating if the listener was called.
   *
   * @author paouelle
   */
  private final BitSet called;

  /**
   * Instantiates a new <code>CompoundObjectSetFuture</code> object.
   *
   * @author paouelle
   *
   * @param  futures the list of object set futures to compound together
   * @throws NullPointerException if <code>statements</code> or any of the
   *         object set futures are <code>null</code>
   */
  public CompoundObjectSetFuture(List<ObjectSetFuture<T>> futures) {
    org.apache.commons.lang3.Validate.notNull(futures, "invalid null result set futures");
    final List<ObjectSetFuture<T>> osets = new ArrayList<>(futures.size());

    this.called = new BitSet(futures.size());
    for (int i = 0; i < futures.size(); i++) {
      final ObjectSetFuture<T> oset = futures.get(i);

      org.apache.commons.lang3.Validate.notNull(oset, "invalid null object set future");
      osets.add(oset);
      called.set(i);
    }
    this.futures = osets;
    for (int i = 0; i < osets.size(); i++) {
      final ObjectSetFuture<T> oset = osets.get(i);
      final int index = i;

      oset.addListener(new Runnable() {
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
      }, DIRECT);
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
      for (final ObjectSetFuture<T> future: futures) {
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
      for (final ObjectSetFuture<T> future: futures) {
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
   * @see com.google.common.util.concurrent.AbstractFuture#cancel(boolean)
   */
  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    boolean cancelled = true; // until proven otherwise

    synchronized (futures) {
      for (final ObjectSetFuture<T> future: futures) {
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
  public ObjectSet<T> get(long timeout, TimeUnit unit)
    throws InterruptedException, TimeoutException, ExecutionException {
    final long end = System.nanoTime() + unit.toNanos(timeout);
    final List<ObjectSet<T>> results = new ArrayList<>(futures.size());

    synchronized (futures) {
      for (final ObjectSetFuture<T> future: futures) {
        final ObjectSet<T> result = future.get(end - System.nanoTime(), TimeUnit.NANOSECONDS);

        results.add(result);
      }
    }
    return new CompoundObjectSet<>(results);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.google.common.util.concurrent.AbstractFuture#get()
   */
  @Override
  public ObjectSet<T> get() throws InterruptedException, ExecutionException {
    final List<ObjectSet<T>> results = new ArrayList<>(futures.size());

    synchronized (futures) {
      for (final ObjectSetFuture<T> future: futures) {
        results.add(future.get());
      }
    }
    return new CompoundObjectSet<>(results);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.ObjectSetFuture#getUninterruptibly()
   */
  @Override
  public ObjectSet<T> getUninterruptibly() {
    final List<ObjectSet<T>> results = new ArrayList<>(futures.size());

    synchronized (futures) {
      for (final ObjectSetFuture<T> future: futures) {
        results.add(future.getUninterruptibly());
      }
    }
    return new CompoundObjectSet<>(results);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.ObjectSetFuture#getUninterruptibly(long, java.util.concurrent.TimeUnit)
   */
  @Override
  public ObjectSet<T> getUninterruptibly(long timeout, TimeUnit unit)
    throws TimeoutException {
    final long end = System.nanoTime() + unit.toNanos(timeout);
    final List<ObjectSet<T>> results = new ArrayList<>(futures.size());

    synchronized (futures) {
      for (final ObjectSetFuture<T> future: futures) {
        final ObjectSet<T> result = future.getUninterruptibly(
          end - System.nanoTime(), TimeUnit.NANOSECONDS
        );

        results.add(result);
      }
    }
    return new CompoundObjectSet<>(results);
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
   * The <code>CompoundObjectSet</code> class is used to compound together
   * multiple object sets.
   *
   * @copyright Enlighted Inc. Confidential
   *            Copyright (c) 2015-2015 by Enlighted, Inc.
   *            All Rights Reserved.
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Mar 9, 2015
   *            - paouelle
   *            - Creation
   *
   * @param <T> The type of POJO associated with the future object set
   *
   * @since 1.0
   */
  private static class CompoundObjectSet<T> implements ObjectSet<T> {
    /**
     * Holds the object sets being compounded.
     *
     * @author paouelle
     */
    private final List<ObjectSet<T>> objects;

    /**
     * Holds the current object set from which to retrieve objects.
     *
     * @author paouelle
     */
    private int i = 0;

    /**
     * Instantiates a new <code>CompoundObjectSet</code> object.
     *
     * @author paouelle
     *
     * @param objects the non-<code>null</code> and non-empty list of object
     *        sets to compound together
     */
    CompoundObjectSet(List<ObjectSet<T>> objects) {
      this.objects = objects;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.github.helenusdriver.driver.ObjectSet#getColumnDefinitions()
     */
    @Override
    public ColumnDefinitions getColumnDefinitions() {
      return objects.get(0).getColumnDefinitions();
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.github.helenusdriver.driver.ObjectSet#isExhausted()
     */
    @Override
    public boolean isExhausted() {
      if (i >= objects.size()) {
        return true;
      }
      ObjectSet<T> current = objects.get(i);

      while (current.isExhausted()) {
        if (++i >= objects.size()) {
          return true;
        }
        current = objects.get(i);
      }
      return false;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.github.helenusdriver.driver.ObjectSet#one()
     */
    @Override
    public T one() {
      if (isExhausted()) {
        return null;
      }
      return objects.get(i).one();
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.github.helenusdriver.driver.ObjectSet#oneRequired()
     */
    @Override
    public T oneRequired() {
      if (isExhausted()) {
        return null;
      }
      return objects.get(i).oneRequired();
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.github.helenusdriver.driver.ObjectSet#stream()
     */
    @Override
    public Stream<T> stream() {
      if (isExhausted()) {
        return Stream.empty();
      }
      return StreamSupport.stream(Spliterators.spliteratorUnknownSize(
        iterator(),
        Spliterator.ORDERED | Spliterator.IMMUTABLE | Spliterator.NONNULL
      ), false);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.github.helenusdriver.driver.ObjectSet#all()
     */
    @Override
    public List<T> all() {
      if (isExhausted()) {
        return Collections.emptyList();
      }
      final List<T> objs = new ArrayList<>(objects.get(i).all());

      while (!isExhausted()) {
        objs.addAll(objects.get(i).all());
      }
      return objs;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.github.helenusdriver.driver.ObjectSet#iterator()
     */
    @Override
    public Iterator<T> iterator() {
      return new Iterator<T>() {
        @Override
        public boolean hasNext() {
          return !isExhausted();
        }
        @SuppressWarnings("synthetic-access")
        @Override
        public T next() {
          if (isExhausted()) {
            throw new NoSuchElementException();
          }
          return objects.get(i).one();
        }
      };
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.github.helenusdriver.driver.ObjectSet#getAvailableWithoutFetching()
     */
    @Override
    public int getAvailableWithoutFetching() {
      if (isExhausted()) {
        return 0;
      }
      int num = 0;
      int j = i;

      while (true) {
        final ObjectSet<T> current = objects.get(j);

        num += current.getAvailableWithoutFetching();
        if (!current.isFullyFetched() || (++j >= objects.size())) {
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
     * @see com.github.helenusdriver.driver.ObjectSet#isFullyFetched()
     */
    @Override
    public boolean isFullyFetched() {
      if (!isExhausted()) {
        int j = i;

        do {
          if (!objects.get(j).isFullyFetched()) {
            return false;
          }
        } while (++j < objects.size());
      }
      return true;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.github.helenusdriver.driver.ObjectSet#fetchMoreObjects()
     */
    @Override
    public ListenableFuture<Void> fetchMoreObjects() {
      if (!isExhausted()) {
        int j = i;

        do {
          final ObjectSet<T> current = objects.get(j);

          if (!current.isFullyFetched()) {
            return current.fetchMoreObjects();
          }
        } while (++j < objects.size());
      }
      // all done so just call the first result set one to be able
      // to return a listenable future
      return objects.get(0).fetchMoreObjects();
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.github.helenusdriver.driver.ObjectSet#getExecutionInfo()
     */
    @Override
    public ExecutionInfo getExecutionInfo() {
      if (isExhausted()) { // return the one from the last result set
        return objects.get(objects.size() - 1).getExecutionInfo();
      }
      // otherwise return the one for the current result set
      return objects.get(i).getExecutionInfo();
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.github.helenusdriver.driver.ObjectSet#getAllExecutionInfo()
     */
    @Override
    public List<ExecutionInfo> getAllExecutionInfo() {
      return objects.stream()
        .flatMap(r -> r.getAllExecutionInfo().stream())
        .collect(Collectors.toList());
    }
  }
}

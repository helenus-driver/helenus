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
package org.helenus.driver.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.util.concurrent.ListenableFuture;

import org.helenus.driver.ObjectNotFoundException;
import org.helenus.driver.ObjectSet;
import org.helenus.driver.StatementManager;
import org.helenus.driver.TooManyMatchesFoundException;

/**
 * The <code>ObjectSetImpl</code> class extends on Cassandra's
 * {@link com.datastax.driver.core.ResultSet} class in order to provide support
 * for POJOs.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 15, 2015 - paouelle - Creation
 *
 * @param <T> The type of POJO associated with this object set.
 *
 * @since 1.0
 */
public class ObjectSetImpl<T> implements ObjectSet<T> {
  /**
   * Holds the statement context associated with this object set.
   *
   * @author paouelle
   */
  private final StatementManager.Context<T> context;

  /**
   * Holds the raw future set.
   *
   * @author paouelle
   */
  private final com.datastax.driver.core.ResultSet result;

  /**
   * Holds the filter to apply.
   *
   * @author paouelle
   */
  private volatile Predicate<? super T> filter = t -> true;

  /**
   * Holds the next object to be returned.
   *
   * @author paouelle
   */
  private volatile T next = null;

  /**
   * Instantiates a new <code>ObjectSetImpl</code> object.
   *
   * @author paouelle
   *
   * @param context the non-<code>null</code> statement context associated with
   *        this object set
   * @param result the non-<code>null</code> result set
   */
  ObjectSetImpl(StatementManager.Context<T> context, ResultSet result) {
    this.context = context;
    this.result = result;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.ObjectSet#getColumnDefinitions()
   */
  @Override
  public ColumnDefinitions getColumnDefinitions() {
    return result.getColumnDefinitions();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.ObjectSet#wasApplied()
     */
  @Override
  public boolean wasApplied() {
    return result.wasApplied();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.ObjectSet#isExhausted()
   */
  @Override
  public boolean isExhausted() {
    // we cannot rely only on the result set as it possible that the next result
    // get's further filtered by the context (e.g. it is an object of a different
    // type than requested)
    while ((next == null) && !result.isExhausted()) {
      final T n = context.getObject(result.one());

      if (filter.test(n)) {
        this.next = n;
      }
    }
    return next == null;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.ObjectSet#one()
   */
  @Override
  public T one() {
    if (isExhausted()) {
      return null;
    }
    final T next = this.next;

    this.next = null;
    return next;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.ObjectSet#oneRequired()
   */
  @Override
  public T oneRequired() {
    final T next = one();

    if (next == null) {
      throw new ObjectNotFoundException(
        context.getObjectClass(),
        "one object was required; none found"
      );
    }
    return next;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.ObjectSet#onlyOneRequired()
   */
  @Override
  public T onlyOneRequired() {
    final T next = oneRequired();

    if (one() != null) {
      throw new TooManyMatchesFoundException(
        context.getObjectClass(),
        "only one object was required, more than one found"
      );
    }
    return next;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.ObjectSet#filter(java.util.function.Predicate)
   */
  @Override
  public ObjectSet<T> filter(Predicate<? super T> filter) {
    org.apache.commons.lang3.Validate.notNull(filter, "invalid null filter");
    this.filter = filter;
    return this;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.ObjectSet#stream()
   */
  @Override
  public Stream<T> stream() {
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
   * @see org.helenus.driver.ObjectSet#all()
   */
  @Override
  public List<T> all() {
    final List<Row> rows = result.all();
    final List<T> ts = new ArrayList<>(rows.size() + 1);
    final T next = this.next;

    if (next != null) { // don't leave this one behind
      this.next = null;
      ts.add(next);
    }
    for (final Row row: rows) {
      final T obj = context.getObject(row);

      if ((obj != null) && filter.test(obj)) {
        ts.add(obj);
      }
    }
    return ts;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.ObjectSet#iterator()
   */
  @Override
  public Iterator<T> iterator() {
    final Iterator<Row> i = result.iterator();

    return new Iterator<T>() {
      private T next = null;

      @Override
      @SuppressWarnings("synthetic-access")
      public boolean hasNext() {
        final T next = ObjectSetImpl.this.next;

        if (next != null) { // don't leave this one behind
          ObjectSetImpl.this.next = null;
          this.next = next;
        } else {
          while ((this.next == null) && i.hasNext()) { // skip over all invalid type result
            final T n = context.getObject(i.next());

            if (filter.test(n)) {
              this.next = n;
            }
          }
        }
        return this.next != null;
      }
      @Override
      public T next() {
        if (!hasNext()) {
          throw new NoSuchElementException("ObjectSet Iterator");
        }
        final T next = this.next;

        this.next = null;
        return next;
      }
    };
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.ObjectSet#getAvailableWithoutFetching()
   */
  @Override
  public int getAvailableWithoutFetching() {
    return result.getAvailableWithoutFetching() + ((next != null) ? 1 : 0);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.ObjectSet#isFullyFetched()
   */
  @Override
  public boolean isFullyFetched() {
    return (next == null) && result.isFullyFetched();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.ObjectSet#fetchMoreObjects()
   */
  @Override
  public ListenableFuture<ObjectSet<T>> fetchMoreObjects() {
    return new ListenableFutureImpl<>(context, result.fetchMoreResults());
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.ObjectSet#getExecutionInfo()
   */
  @Override
  public ExecutionInfo getExecutionInfo() {
    return result.getExecutionInfo();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.ObjectSet#getAllExecutionInfo()
   */
  @Override
  public List<ExecutionInfo> getAllExecutionInfo() {
    return result.getAllExecutionInfo();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return result.toString();
  }
}

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
package com.github.helenus.driver;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * The <code>ObjectSet</code> class extends Cassandra's
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
public class ObjectSet<T> implements Iterable<T> {
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
   * Instantiates a new <code>ObjectSet</code> object.
   *
   * @author paouelle
   *
   * @param context the non-<code>null</code> statement context associated with
   *        this object set
   * @param result the non-<code>null</code> result set
   */
  ObjectSet(StatementManager.Context<T> context, ResultSet result) {
    this.context = context;
    this.result = result;
  }

  /**
   * Gets the columns returned in this object set.
   *
   * @author paouelle
   *
   * @return the columns returned in this object set.
   */
  public ColumnDefinitions getColumnDefinitions() {
    return result.getColumnDefinitions();
  }

  /**
   * Gets whether this object set has more POJOs.
   *
   * @author paouelle
   *
   * @return whether this object set has more POJOs.
   */
  public boolean isExhausted() {
    return result.isExhausted();
  }

  /**
   * Gets the the next POJO from this object set.
   *
   * @author paouelle
   *
   * @return the next POJO in this object set or <code>null</code> if this
   *         object set is exhausted
   * @throws ObjectConversionException if unable to convert to a POJO
   */
  public T one() {
    return context.getObject(result.one());
  }

  /**
   * Gets a stream of all the remaining POJOs in this object set.
   * <p>
   * The returned stream will throw {@link ObjectConversionException} when
   * walked and we were unable to convert to a POJO.
   *
   * @author paouelle
   *
   * @return a non-<code>null</code> stream containing the remaining POJOs of
   *         this object set
   */
  public Stream<T> stream() {
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(
      iterator(),
      Spliterator.ORDERED | Spliterator.IMMUTABLE | Spliterator.NONNULL
    ), false);
  }

  /**
   * Gets a list of all the remaining POJOs in this object set.
   *
   * @author paouelle
   *
   * @return a non-<code>null</code> list containing the remaining POJOs of
   *         this object set. The returned list is empty if and only the result
   *         set is exhausted
   * @throws ObjectConversionException if unable to convert to POJOs
   */
  public List<T> all() {
    final List<Row> rows = result.all();
    final List<T> ts = new ArrayList<>(rows.size());

    for (final Row row: rows) {
      final T obj = context.getObject(row);

      if (obj != null) {
        ts.add(obj);
      }
    }
    return ts;
  }

  /**
   * {@inheritDoc}
   * <p>
   * Gets an iterator over the POJOs contained in this object set.
   *
   * The {@link Iterator#next} method is equivalent to calling {@link #one}. So
   * this iterator will consume POJOs from this object set and after a full
   * iteration, the object set will be empty.
   *
   * The returned iterator does not support the {@link Iterator#remove} method
   * and will throw {@link ObjectConversionException} when calling
   * {@link Iterator#hasNext} or {@link Iterator#next} and we were unable to
   * convert to a POJO.
   *
   * @author paouelle
   *
   * @see java.lang.Iterable#iterator()
   */
  @Override
  public Iterator<T> iterator() {
    final Iterator<Row> i = result.iterator();

    return new Iterator<T>() {
      private T next = null;

      @Override
      @SuppressWarnings("synthetic-access")
      public boolean hasNext() {
        while ((next == null) && i.hasNext()) { // skip over all invalid type result
          this.next = context.getObject(i.next());
        }
        return next != null;
      }
      @Override
      public T next() {
        if (!hasNext()) {
          throw new NoSuchElementException("ObjectSet Iterator");
        }
        final T obj = next;

        this.next = null;
        return obj;
      }
    };
  }

  /**
   * Gets the number of POJOs that can be retrieved from this object set without
   * blocking to fetch.
   *
   * @author paouelle
   *
   * @return the number of POJOs readily available in this object set. If
   *         {@link #isFullyFetched()}, this is the total number of POJOs
   *         remaining in this object set (after which the object set will be
   *         exhausted)
   */
  public int getAvailableWithoutFetching() {
    return result.getAvailableWithoutFetching();
  }

  /**
   * Checks whether all POJOs from this object set has been fetched from the
   * database.
   * <p>
   * Note that if {@code isFullyFetched()}, then
   * {@link #getAvailableWithoutFetching} will return how many POJOs remains in
   * the object set before exhaustion. But please note that
   * {@code !isFullyFetched()} never guarantees that the object set is not
   * exhausted (you should call {@code isExhausted()} to make sure of it).
   *
   * @author paouelle
   *
   * @return whether all POJOs have been fetched
   */
  public boolean isFullyFetched() {
    return result.isFullyFetched();
  }

  /**
   * Force the fetching the next page of POJOs for this object set, if any.
   * <p>
   * This method is entirely optional. It will be called automatically while the
   * object set is consumed (through {@link #one}, {@link #all} or iteration)
   * when needed (i.e. when {@code getAvailableWithoutFetching() == 0} and
   * {@code isFullyFetched() == false}).
   * <p>
   * You can however call this method manually to force the fetching of the next
   * page of POJOs. This can allow to prefetch POJOs before they are strictly
   * needed. For instance, if you want to prefetch the next page of POJOs as
   * soon as there is less than 100 POJOs readily available in this object set,
   * you can do:
   *
   * <pre>
   *   final ObjectSet<T> os = statement.execute(...);
   *   final Iterator&lt;T&gt; i = os.iterator();
   *
   *   while (i.hasNext()) {
   *       if (os.getAvailableWithoutFetching() == 100 && !os.isFullyFetched())
   *           os.fetchMoreObjects();
   *       final T t = i.next()
   *
   *       ... process the POJO ...
   *   }
   * </pre>
   *
   * This method is not blocking, so in the example above, the call to
   * {@code fetchMoreObjects} will not block the processing of the 100 currently
   * available POJOs (but {@code i.hasNext()} will block once those POJOs have
   * been processed until the fetch query returns, if it hasn't yet).
   * <p>
   * Only one page of POJOs (for a given object set) can be fetched at any
   * given time. If this method is called twice and the query triggered by the
   * first call has not returned yet when the second one is performed, then the
   * 2nd call will simply return a future on the currently in progress query.
   *
   * @author paouelle
   *
   * @return a future on the completion of fetching the next page of POJOs. If
   *         the object set is already fully retrieved (
   *         {@code isFullyFetched() == true}), then the returned future will
   *         return immediately but not particular error will be thrown (you
   *         should thus call {@code isFullyFetched() to know if calling this
   *          method can be of any use}).
   */
  public ListenableFuture<Void> fetchMoreObjects() {
    return result.fetchMoreResults();
  }

  /**
   * Gets information on the execution of the last query made for this object
   * set.
   * <p>
   * Note that in most cases, an object set is fetched with only one query, but
   * large object sets can be paged and thus be retrieved by multiple queries.
   * If that is the case, that method return that {@code ExecutionInfo} for the
   * last query performed. To retrieve the informations for all queries, use
   * {@link #getAllExecutionInfo}.
   * <p>
   * The returned object includes basic information such as the queried hosts,
   * but also the Cassandra query trace if tracing was enabled for the query.
   *
   * @author paouelle
   *
   * @return the execution info for the last query made for this object set
   */
  public ExecutionInfo getExecutionInfo() {
    return result.getExecutionInfo();
  }

  /**
   * Gets the execution informations for all queries made to retrieve this
   * object set.
   * <p>
   * Unless the object set is large enough to get paged underneath, the returned
   * list will be a singleton. If paging has been used however, the returned list
   * contains the {@code ExecutionInfo} for all the queries done to obtain this
   * object set (at the time of the call) in the order those queries were made.
   *
   * @author paouelle
   *
   * @return a list of the execution info for all the queries made for this
   *         object set
   */
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

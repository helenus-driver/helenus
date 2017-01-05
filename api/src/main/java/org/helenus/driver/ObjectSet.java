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

import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.PagingIterable;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * The <code>ObjectSet</code> class extends on Cassandra's
 * {@link com.datastax.driver.core.ResultSet} class in order to provide support
 * for POJOs.
 *
 * @copyright 2015-2017 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 15, 2015 - paouelle - Creation
 *
 * @param <T> The type of POJO associated with this object set.
 *
 * @since 1.0
 */
public interface ObjectSet<T> extends PagingIterable<ObjectSet<T>, T> {
  /**
   * Gets the columns returned in this object set.
   *
   * @author paouelle
   *
   * @return the columns returned in this object set.
   */
  public ColumnDefinitions getColumnDefinitions();

  /**
   * If the query that produced this object set was a conditional update,
   * return whether it was successfully applied.
   * <p>
   * This is equivalent to calling:
   * <p>
   * <pre>
   * os.one().getBool("[applied]");
   * </pre>
   * <p>
   * For consistency, this method always returns <code>true</code> for
   * non-conditional queries (although there is no reason to call the method
   * in that case). This is also the case for conditional DDL statements
   * ({@code CREATE KEYSPACE... IF NOT EXISTS}, {@code CREATE TABLE... IF NOT EXISTS}),
   * for which Cassandra doesn't return an {@code [applied]} column.
   * <p>
   * Note that, for versions of Cassandra strictly lower than 2.0.9 and 2.1.0-rc2,
   * a server-side bug (CASSANDRA-7337) causes this method to always return
   * <code>true</code> for batches containing conditional queries.
   *
   * @author paouelle
   *
   * @return if the query was a conditional update, whether it was applied.
   *         <code>true</code> for other types of queries
   *
   * @see <a href="https://issues.apache.org/jira/browse/CASSANDRA-7337">CASSANDRA-7337</a>
   */
  public boolean wasApplied();

  /**
   * Gets the the next POJO from this object set.
   *
   * @author paouelle
   *
   * @return the next POJO in this object set or <code>null</code> if this
   *         object set is exhausted
   * @throws ObjectConversionException if unable to convert to a POJO
   *
   * @see com.datastax.driver.core.PagingIterable#one()
   */
  @Override
  public T one();

  /**
   * Gets the the next POJO from this object set.
   *
   * @author paouelle
   *
   * @return the next POJO in this object set
   * @throws ObjectConversionException if unable to convert to a POJO
   * @throws ObjectNotFoundException if this object set is exhausted
   */
  public T oneRequired();

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
  public Stream<T> stream();

  /**
   * Gets a list of all the remaining POJOs in this object set.
   *
   * @author paouelle
   *
   * @return a non-<code>null</code> list containing the remaining POJOs of
   *         this object set. The returned list is empty if and only the result
   *         set is exhausted
   * @throws ObjectConversionException if unable to convert to POJOs
   *
   * @see com.datastax.driver.core.PagingIterable#all()
   */
  @Override
  public List<T> all();

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
   * @see com.datastax.driver.core.PagingIterable#iterator()
   */
  @Override
  public Iterator<T> iterator();

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
   *   final ObjectSet&lt;T&gt; os = statement.execute(...);
   *   final Iterator&lt;T&gt; i = os.iterator();
   *
   *   while (i.hasNext()) {
   *       if (os.getAvailableWithoutFetching() == 100 &amp; !os.isFullyFetched())
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
   *         method can be of any use}).
   */
  public ListenableFuture<ObjectSet<T>> fetchMoreObjects();

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.datastax.driver.core.PagingIterable#fetchMoreResults()
   */
  @Override
  public default ListenableFuture<ObjectSet<T>> fetchMoreResults() {
    return fetchMoreObjects();
  }
}

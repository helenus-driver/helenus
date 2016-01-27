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

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.google.common.util.concurrent.ListenableFuture;

import org.helenus.util.function.ERunnable;

/**
 * The <code>Group</code> interface defines support for a statement that can
 * execute a set of statements in parallel returning the result from the first
 * one that fails. Basically, a group will sends multiple requests to Cassandra
 * at the same time up to either its parallel factor or until it reaches a
 * sequence statement and then wait for all results before continuing to the
 * next set of statements to be sent in parallel.
 * <p>
 * The group's algorithm will allow the distributed nature of Cassandra to work
 * for you and distribute the writes to the optimal destination which leads to
 * not only fastest inserts/updates (assuming different partitions are being
 * updated), but it’ll cause the least load on the cluster.
 *
 * @copyright 2015-2016 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 20, 2016 - paouelle - Creation
 *
 * @since 1.0
 */
public interface Group
  extends ParentStatement, SequenceableStatement<Void, VoidFuture> {
  /**
   * {@inheritDoc}
   * <p>
   * Gets the keyspace of the first statement in this group.
   *
   * @author paouelle
   *
   * @return the keyspace from the first statement in this group or
   *         <code>null</code> if the group is empty
   *
   * @see org.helenus.driver.GenericStatement#getKeyspace()
   */
  @Override
  public String getKeyspace();

  /**
   * {@inheritDoc}
   * <p>
   * <i>Note:</i> This might not actually be a valid query string if there are
   * more than one statement being grouped. It will then be a representation
   * of the query strings for each statement similar to a "BATCH" statement.
   *
   * @author paouelle
   *
   * @see org.helenus.driver.GenericStatement#getQueryString()
   */
  @Override
  public String getQueryString();

  /**
   * {@inheritDoc}
   * <p>
   * <i>Note:</i> This method will execute all statements simultaneously in
   * subsets of statements one subset after the other until one statement
   * generates an error or all of them have succeeded.
   *
   * @author paouelle
   *
   * @see org.helenus.driver.GenericStatement#execute()
   */
  @Override
  public Void execute();

  /**
   * {@inheritDoc}
   * <p>
   * <i>Note:</i> This method will asynchronously execute all statements
   * simultaneously in subsets of statements one subset after the other until
   * one statement generates an error or all of them have succeeded.
   *
   * @author paouelle
   *
   * @see org.helenus.driver.GenericStatement#executeAsync()
   */
  @Override
  public VoidFuture executeAsync();

  /**
   * {@inheritDoc}
   * <p>
   * <i>Note:</i> This method will execute all statements simultaneously in
   * subsets of statements one subset after the other until one statement
   * generates an error or all of them have succeeded and return the result set
   * from the last statement executed.
   *
   * @author paouelle
   *
   * @see org.helenus.driver.GenericStatement#executeRaw()
   */
  @Override
  public ResultSet executeRaw();

  /**
   * {@inheritDoc}
   * <p>
   * <i>Note:</i> This method will asynchronously execute all statements
   * simultaneously in subsets of statements one subset after the other until
   * one statement generates an error or all of them have succeeded.
   *
   * @author paouelle
   *
   * @see org.helenus.driver.GenericStatement#executeAsyncRaw()
   */
  @Override
  public ResultSetFuture executeAsyncRaw();

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.ParentStatement#add(org.helenus.driver.BatchableStatement)
   */
  @Override
  public <R, F extends ListenableFuture<R>> Group add(BatchableStatement<R, F> statement);

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.ParentStatement#add(com.datastax.driver.core.RegularStatement)
   */
  @Override
  public Group add(com.datastax.driver.core.RegularStatement statement);

  /**
   * Adds a new statement to this group.
   *
   * @author paouelle
   *
   * @param <R> The type of result returned when executing the statement to record
   * @param <F> The type of future result returned when executing the statement
   *            to record
   *
   * @param  statement the new statement to add
   * @return this group
   * @throws NullPointerException if <code>statement</code> is <code>null</code>
   * @throws ObjectValidationException if the statement's POJO is validated via
   *         an associated recorder and that validation fails
   */
  public <R, F extends ListenableFuture<R>> Group add(
    GroupableStatement<R, F> statement
  );

  /**
   * Gets the number of simultaneous statements to send to the Cassandra cluster
   * at the same time before starting to wait for all responses before proceeding
   * to the next set (assuming a sequence is not reached before that).
   * <p>
   * By default, the parallel factor will be based on the number of nodes in the
   * Cassandra cluster multiplied by 32 (default number of threads in the write
   * pool of a Cassandra node).
   *
   * @author paouelle
   *
   * @return the parallel factor to use when sending simultaneous statements
   */
  public int getParallelFactor();

  /**
   * Sets the number of simultaneous statements to send to the Cassandra cluster
   * at the same time before starting to wait for all responses before proceeding
   * to the next set (assuming a sequence is not reached before that).
   * <p>
   * By default, the parallel factor will be based on the number of nodes in the
   * Cassandra cluster multiplied by 32 (default number of threads in the write
   * pool of a Cassandra node).
   *
   * @author paouelle
   *
   * @param  factor the new parallel factor to use when sending simultaneous
   *         statements
   * @return this group
   */
  public Group setParallelFactor(int factor);

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.ParentStatement#addErrorHandler(org.helenus.util.function.ERunnable)
   */
  @Override
  public Group addErrorHandler(ERunnable<?> run);
}

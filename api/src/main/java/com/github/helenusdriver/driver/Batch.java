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
package com.github.helenusdriver.driver;

import com.github.helenusdriver.util.function.ERunnable;
import com.google.common.util.concurrent.AbstractFuture;

/**
 * The <code>Batch</code> interface extends the functionality of Cassandra's
 * {@link com.datastax.driver.core.querybuilder.Batch} class to provide
 * support for POJOs.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 15, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
public interface Batch
  extends Statement<Void>, BatchableStatement<Void, VoidFuture>, Group<Batch> {
  /**
   * Holds the max size of the batch after which we should be committing it to
   * Cassandra while processing the CSV file.
   * <p>
   * <i>Note:</i> With regard to a large number of records in a batch mutation
   * there are some potential issues. Each row becomes a task in the write
   * thread pool on each Cassandra replica. If a single client sends 1,000 rows
   * in a mutation it will take time for the (default) 32 threads in the write
   * pool to work through the mutations. While they are doing this other
   * clients/requests will appear to be starved/stalled. There are also issues
   * with the max message size in thrift and cql over thrift.
   * <p>
   * As a rule of thumb don't go over a few hundred if you have a high number of
   * concurrent writers.
   *
   * @author paouelle
   */
  public final static int RECOMMENDED_MAX = 100;

  /**
   * {@inheritDoc}
   * <p>
   * Gets the keyspace of the first statement in this batch.
   *
   * @author paouelle
   *
   * @return the keyspace from the first statement in this batch or
   *         <code>null</code> if the batch is empty
   *
   * @see com.github.helenusdriver.driver.GenericStatement#getKeyspace()
   */
  @Override
  public String getKeyspace();

  /**
   * Checks the this batch has reached the recommended size for a batch in
   * system with a high number of concurrent writers.
   *
   * @author paouelle
   *
   * @return <code>true</code> if the recommended size has been reached or
   *         exceeded for this batch; <code>false</code> otherwise
   */
  public boolean hasReachedRecommendedSize();

  /**
   * Adds a new options for this BATCH statement.
   *
   * @author paouelle
   *
   * @param  using the option to add
   * @return the options of this BATCH statement
   */
  public Options using(Using using);

  /**
   * Duplicates this batch statement.
   * <p>
   * <i>Note:</i> The registered recorder is not copied over to the duplicated
   * batch.
   *
   * @author paouelle
   *
   * @return a new batch statement which is a duplicate of this one
   */
  public Batch duplicate();

  /**
   * Duplicates this batch statement.
   *
   * @author paouelle
   *
   * @param  recorder the recorder to register with the duplicated batch
   * @return a new batch statement which is a duplicate of this one
   * @throws NullPointerException if <code>recorder</code> is <code>null</code>
   */
  public Batch duplicate(Recorder recorder);

  /**
   * The <code>Options</code> interface defines the options of a BATCH statement.
   *
   * @copyright 2015-2015 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 15, 2015 - paouelle - Creation
   *
   * @since 1.0
   */
  public interface Options
    extends GenericStatement<Void, VoidFuture>, BatchableStatement<Void, VoidFuture> {
    /**
     * Adds the provided option.
     * <p>
     * <i>Note:</i> The primary key and mandatory columns are always added to the
     * BATCH statement.
     *
     * @author paouelle
     *
     * @param  using a BATCH option
     * @return this {@code Options} object
     */
    public Options and(Using using);

    /**
     * Adds a new statement to the BATCH statement these options are part of.
     *
     * @author paouelle
     *
     * @param <R> The type of result returned when executing the statement to batch
     * @param <F> The type of future result returned when executing the statement
     *            to batch
     *
     * @param  statement the new statement to add
     * @return this batch
     * @throws NullPointerException if <code>statement</code> is <code>null</code>
     * @throws IllegalArgumentException if counter and non-counter operations
     *         are mixed or if the statement represents a "select" statement or a
     *         "batch" statement  or if the statement is not of a supported class
     */
    public <R, F extends AbstractFuture<R>> Batch add(BatchableStatement<R, F> statement);

    /**
     * Adds a new raw Cassandra statement to the BATCH statement these options
     * are part of.
     *
     * @author paouelle
     *
     * @param  statement the new statement to add
     * @return this batch
     * @throws NullPointerException if <code>statement</code> is <code>null</code>
     * @throws IllegalArgumentException if counter and non-counter operations
     *         are mixed or if the statement represents a "select" statement or a
     *         "batch" statement
     */
    public Batch add(com.datastax.driver.core.RegularStatement statement);

    /**
     * Registers an error handler with this batch. Error handlers are simply
     * attached to the batch and must be specifically executed via the
     * {@link #runErrorHandlers} method by the user when an error occurs either
     * from the execution of the batch of before executing the batch to make sure
     * that allocated resources can be properly released if no longer required.
     *
     * @author paouelle
     *
     * @param  run the error handler to register
     * @return this batch
     * @throws NullPointerException if <code>run</code> is <code>null</code>
     */
    public Batch addErrorHandler(ERunnable<?> run);
  }
}

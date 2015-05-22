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

import java.util.Optional;

import com.github.helenusdriver.util.function.ERunnable;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * The <code>Group</code> interface extends the functionality of
 * the {@link GenericStatement} interface for statements that are capable of
 * grouping other statements together such as a {@link Batch} or a {@link Sequence}.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - May 21, 2015 - paouelle - Creation
 *
 * @param <S> the type of statement that is
 *
 * @since 1.0
 */
public interface Group<S> extends GenericStatement<Void, VoidFuture> {
  /**
   * Gets the recorder registered with this group or with a parent
   * if any.
   * <p>
   * <i>Note:</i> A parent is another group to which this one was
   * added.
   *
   * @author paouelle
   *
   * @return the recorder registered with this group (or a parent)
   *         or empty if none was registered at the time of creation
   */
  public Optional<Recorder> getRecorder();

  /**
   * Checks if the sequence has no statements added.
   *
   * @author paouelle
   *
   * @return <code>true</code> if no statements were added to the sequence;
   *         <code>false</code> otherwise
   */
  public boolean isEmpty();

  /**
   * Gets the number of statements that are part of this sequence.
   *
   * @author paouelle
   *
   * @return the number of statements in this sequence
   */
  public int size();

  /**
   * Removes all statements already added to this sequence.
   *
   * @author paouelle
   */
  public void clear();

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
   * @return the group
   * @throws NullPointerException if <code>statement</code> is <code>null</code>
   * @throws IllegalArgumentException if counter and non-counter operations
   *         are mixed or if the statement is not of a supported class
   */
  public <R, F extends ListenableFuture<R>> S add(BatchableStatement<R, F> statement);

  /**
   * Adds a new raw Cassandra statement to this group.
   *
   * @author paouelle
   *
   * @param  statement the new statement to add
   * @return this group
   * @throws NullPointerException if <code>statement</code> is <code>null</code>
   * @throws IllegalArgumentException if counter and non-counter operations
   *         are mixed or if the statement represents a "select" statement or a
   *         "batch" statement or if the statement is not of a supported class
   */
  public S add(com.datastax.driver.core.RegularStatement statement);

  /**
   * Registers an error handler with this group. Error handlers
   * are simply attached to the group and must be specifically
   * executed via the {@link #runErrorHandlers} method by the user when an error
   * occurs either from the execution of the group of before executing
   * the group to make sure that allocated resources can be properly
   * released if no longer required.
   *
   * @author paouelle
   *
   * @param  run the error handler to register
   * @return this group
   * @throws NullPointerException if <code>run</code> is <code>null</code>
   */
  public S addErrorHandler(ERunnable<?> run);

  /**
   * Runs all registered error handlers in sequence from the current thread. This
   * method will recursively runs all registered error handlers in sequences and
   * batches that have been added to this group.
   * <p>
   * <i>Note:</i> Errors thrown out of the error handlers will not be
   * propagated up.
   *
   * @author paouelle
   */
  public void runErrorHandlers();
}

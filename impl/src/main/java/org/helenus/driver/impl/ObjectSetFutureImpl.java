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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.exceptions.InvalidQueryException;

import org.helenus.driver.ObjectNotFoundException;
import org.helenus.driver.ObjectSet;
import org.helenus.driver.ObjectSetFuture;
import org.helenus.driver.StatementManager;

/**
 * The <code>ObjectSetFutureImpl</code> class extends Cassandra's
 * {@link com.datastax.driver.core.ResultSetFuture} in order to provide
 * support for POJOs.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 15, 2015 - paouelle - Creation
 *
 * @param <T> The type of POJO associated with the future object set
 *
 * @since 1.0
 */
public class ObjectSetFutureImpl<T>
  extends ListenableFutureImpl<T>
  implements ObjectSetFuture<T> {
  /**
   * Holds the raw result set future.
   *
   * @author paouelle
   */
  private final ResultSetFuture future;

  /**
   * Instantiates a new <code>ObjectSetFutureImpl</code> object.
   *
   * @author paouelle
   *
   * @param context the non-<code>null</code> statement context associated with
   *        this future object set
   * @param future the non-<code>null</code> result set future
   */
  ObjectSetFutureImpl(StatementManager.Context<T> context, ResultSetFuture future) {
    super(context, future);
    this.future = future;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.ObjectSetFuture#getUninterruptibly()
   */
  @Override
  public ObjectSet<T> getUninterruptibly() {
    try {
      final ResultSet result = future.getUninterruptibly();

      postProcess(result);
      return new ObjectSetImpl<>(context, result);
    } catch (InvalidQueryException e) {
      ObjectNotFoundException.handleKeyspaceNotFound(context.getObjectClass(), e);
      throw e;
    }
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.ObjectSetFuture#getUninterruptibly(long, java.util.concurrent.TimeUnit)
   */
  @Override
  public ObjectSet<T> getUninterruptibly(long timeout, TimeUnit unit)
    throws TimeoutException {
    try {
      final ResultSet result = future.getUninterruptibly(timeout, unit);

      postProcess(result);
      return new ObjectSetImpl<>(context, result);
    } catch (InvalidQueryException e) {
      ObjectNotFoundException.handleKeyspaceNotFound(context.getObjectClass(), e);
      throw e;
    }
  }
}

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

import com.datastax.driver.core.ResultSetFuture;


/**
 * The <code>StatementBridge</code> class provides access to protected statement
 * information and services. A statement bridge is returned when a statement
 * manager is installed using the {@link StatementManager#setManager}.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 15, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
public final class StatementBridge {
  /**
   * Instantiates a new <code>StatementBridge</code> object.
   *
   * @author paouelle
   */
  StatementBridge() {}

  /**
   * Instantiates a new <code>ObjectSetFuture</code> object.
   *
   * @author paouelle
   *
   * @param <T> The type of POJO
   *
   * @param  context the non-<code>null</code> statement context associated with
   *         the future object set
   * @param  future the non-<code>null</code> result set future
   * @return the corresponding new instance
   */
  public <T> ObjectSetFuture<T> newObjectSetFuture(
    StatementManager.Context<T> context, ResultSetFuture future
  ) {
    return new ObjectSetFuture<>(context, future);
  }

  /**
   * Instantiates a new <code>VoidFuture</code> object.
   *
   * @author paouelle
   *
   * @param  future the non-<code>null</code> result set future
   * @return the corresponding new instance
   */
  public VoidFuture newVoidFuture(ResultSetFuture future) {
    return new VoidFuture(future);
  }

  /**
   * Instantiates a new <code>VoidFuture</code> object.
   *
   * @author paouelle
   *
   * @param  future the non-<code>null</code> result set future
   * @param  processor the optional post processor
   * @return the corresponding new instance
   */
  public VoidFuture newVoidFuture(
    ResultSetFuture future, VoidFuture.PostProcessor processor
  ) {
    return new VoidFuture(future, processor);
  }
}

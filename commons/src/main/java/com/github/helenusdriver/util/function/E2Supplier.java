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
package com.github.helenusdriver.util.function;

/**
 * The <code>E2Supplier</code> interface expands on the {@link ESupplier}
 * interface provide the ability to throw two specified exceptions.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 15, 2015 - paouelle - Creation
 *
 * @param <T> the type of results supplied by this supplier
 * @param <E1> the type of the exception that can be thrown by the supplier
 * @param <E2> the type of the exception that can be thrown by the supplier
 *
 * @since 2.0
 */
@FunctionalInterface
public interface E2Supplier<T, E1 extends Throwable, E2 extends Throwable> {
  /**
   * Gets a result.
   *
   * @author paouelle
   *
   * @return a result
   * @throws E1 if an error occurs
   * @throws E2 if an error occurs
   */
  public T get() throws E1, E2;
}

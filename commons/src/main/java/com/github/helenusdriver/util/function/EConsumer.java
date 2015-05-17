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

import java.util.function.Consumer;

/**
 * The <code>EConsumer</code> interface expands on the {@link Consumer}
 * interface to provide the ability to throw back exceptions.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 15, 2015 - paouelle - Creation
 *
 * @param <T> the type of the first argument to the operation
 * @param <E> the type of exceptions that can be thrown by the operation
 *
 * @since 2.0
 */
@FunctionalInterface
public interface EConsumer<T, E extends Throwable> {
  /**
   * Performs this operation on the given argument.
   *
   * @author paouelle
   *
   * @param  t the input argument
   * @throws E if an error occurs
   */
  public void accept(T t) throws E;

  /**
   * Returns a composed {@code EConsumer} that performs, in sequence, this
   * operation followed by the {@code after} operation. If performing either
   * operation throws an exception, it is relayed to the caller of the
   * composed operation. If performing this operation throws an exception,
   * the {@code after} operation will not be performed.
   *
   * @author paouelle
   *
   * @param  after the operation to perform after this operation
   * @return a composed {@code EConsumer} that performs in sequence this
   *         operation followed by the {@code after} operation
   * @throws NullPointerException if {@code after} is <code>null</code>
   * @throws E if an error occurs
   */
  public default EConsumer<T, E> andThen(EConsumer<? super T, E> after) throws E {
    org.apache.commons.lang3.Validate.notNull(after, "invalid null after");
    return (a) -> {
      accept(a);
      after.accept(a);
    };
  }
}
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
package org.helenus.util.function;

import java.util.function.BiConsumer;

/**
 * The <code>EConsumer</code> interface expands on the {@link BiConsumer}
 * interface to provide the ability to throw back exceptions.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - May 28, 2015 - paouelle - Creation
 *
 * @param <T> the type of the first argument to the operation
 * @param <U> the type of the second argument to the operation
 * @param <E> the type of exceptions that can be thrown by the operation
 *
 * @since 2.0
 */
@FunctionalInterface
public interface EBiConsumer<T, U, E extends Throwable> {
  /**
   * Performs this operation on the given arguments.
   *
   * @author paouelle
   *
   * @param  t the first input argument
   * @param  u the second input argument
   * @throws E if an error occurs
   */
  public void accept(T t, U u) throws E;

  /**
   * Returns a composed {@code EBiConsumer} that performs, in sequence, this
   * operation followed by the {@code after} operation. If performing either
   * operation throws an exception, it is relayed to the caller of the
   * composed operation.  If performing this operation throws an exception,
   * the {@code after} operation will not be performed.
   *
   * @author paouelle
   *
   * @param  after the operation to perform after this operation
   * @return a composed {@code BiEConsumer} that performs in sequence this
   *         operation followed by the {@code after} operation
   * @throws NullPointerException if {@code after} is <code>null</code>
   */
  public default EBiConsumer<T, U, E> andThen(
    EBiConsumer<? super T, ? super U, E> after
  ) throws E {
    org.apache.commons.lang3.Validate.notNull(after, "invalid null after");
    return (l, r) -> {
      accept(l, r);
      after.accept(l, r);
    };
  }
}

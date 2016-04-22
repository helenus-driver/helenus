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

import java.util.function.Consumer;

/**
 * The <code>QuadConsumer</code> interface represents an operation that accepts
 * four input arguments and returns no result. This is the four-arity
 * specialization of {@link Consumer}. Unlike most other functional interfaces,
 * <code>QuadConsumer</code> is expected to operate via side-effects.
 * <p>
 * This is a <a href="package-summary.html">functional interface</a>
 * whose functional method is {@link #accept(Object, Object, Object, Object)}.
 *
 * @copyright 2015-2016 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Apr 22, 2015 - paouelle - Creation
 *
 * @param <T> the type of the first argument to the operation
 * @param <U> the type of the second argument to the operation
 * @param <V> the type of the third argument to the operation
 * @param <X> the type of the fourth argument to the operation
 *
 * @since 2.0
 */
@FunctionalInterface
public interface QuadConsumer<T, U, V, X> {
  /**
   * Performs this operation on the given arguments.
   *
   * @author paouelle
   *
   * @param t the first input argument
   * @param u the second input argument
   * @param v the third input argument
   * @param x the fourth input argument
   */
  public void accept(T t, U u, V v, X x);

  /**
   * Returns a composed {@code QuadConsumer} that performs, in sequence, this
   * operation followed by the {@code after} operation. If performing either
   * operation throws an exception, it is relayed to the caller of the
   * composed operation. If performing this operation throws an exception,
   * the {@code after} operation will not be performed.
   *
   * @author paouelle
   *
   * @param  after the operation to perform after this operation
   * @return a composed {@code QuadConsumer} that performs in sequence this
   *         operation followed by the {@code after} operation
   * @throws NullPointerException if {@code after} is <code>null</code>
   */
  public default QuadConsumer<T, U, V, X> andThen(
    QuadConsumer<? super T, ? super U, ? super V, ? super X> after
  ) {
    org.apache.commons.lang3.Validate.notNull(after, "invalid null after");
    return (l, m, r, f) -> {
      accept(l, m, r, f);
      after.accept(l, m, r, f);
    };
  }
}

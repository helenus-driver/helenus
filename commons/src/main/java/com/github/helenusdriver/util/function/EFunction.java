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

import java.util.function.Function;

/**
 * The <code>EFunction</code> interface expands on the {@link Function}
 * interface to provide the ability to throw exceptions.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 15, 2015 - paouelle - Creation
 *
 * @param <T> the type of the input to the function
 * @param <R> the type of the result of the function
 * @param <E> the type of the exception that can be thrown by the function
 *
 * @since 2.0
 */
@FunctionalInterface
public interface EFunction<T, R, E extends Throwable> {
  /**
   * Applies this function to the given argument.
   *
   * @author paouelle
   *
   * @param  t the function argument
   * @return the function result
   * @throws E if an error occurs
   */
  public R apply(T t) throws E;

  /**
   * Returns a composed function that first applies the {@code before}
   * function to its input, and then applies this function to the result.
   * If evaluation of either function throws an exception, it is relayed to
   * the caller of the composed function.
   *
   * @author paouelle
   *
   * @param <V> the type of input to the {@code before} function, and to the
   *        composed function
   *
   * @param  before the function to apply before this function is applied
   * @return a composed function that first applies the {@code before}
   *         function and then applies this function
   * @throws NullPointerException if before is <code>null</code>
   *
   * @see #andThen(EFunction)
   */
  public default <V> EFunction<V, R, E> compose(
    EFunction<? super V, ? extends T, E> before
  ) {
    org.apache.commons.lang3.Validate.notNull(before, "invalid null before");
    return (V v) -> apply(before.apply(v));
  }

  /**
   * Returns a composed function that first applies this function to
   * its input, and then applies the {@code after} function to the result.
   * If evaluation of either function throws an exception, it is relayed to
   * the caller of the composed function.
   *
   * @author paouelle
   *
   * @param <V> the type of output of the {@code after} function, and of the
   *        composed function
   *
   * @param  after the function to apply after this function is applied
   * @return a composed function that first applies this function and then
   *         applies the {@code after} function
   * @throws NullPointerException if after is <code>null</code>
   *
   * @see #compose(EFunction)
   */
  public default <V> EFunction<T, V, E> andThen(
    EFunction<? super R, ? extends V, E> after
  ) {
    org.apache.commons.lang3.Validate.notNull(after, "invalid null after");
    return (T t) -> after.apply(apply(t));
  }
}
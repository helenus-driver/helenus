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
package org.helenus.util.stream;

import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collector;

/**
 * The <code>Collectors</code> class extends on the {@link java.util.stream.Collectors}
 * class to provide additional collectors.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Aug 3, 2015 - paouelle - Creation
 *
 * @since 2.0
 */
public class Collectors {
  /**
   * Returns a merge function, suitable for use in
   * {@link Map#merge(Object, Object, BiFunction) Map.merge()} or
   * {@link java.util.stream.Collectors#toMap(Function, Function, BinaryOperator) toMap()},
   * which always throws {@link IllegalStateException}. This can be used to
   * enforce the assumption that the elements being collected are distinct.
   *
   * @author paouelle
   *
   * @param  <T> the type of input arguments to the merge function
   *
   * @return a merge function which always throw <code>IllegalStateException</code>
   */
  public static <T> BinaryOperator<T> throwingMerger() {
    return (u,v) -> {
      throw new IllegalStateException(String.format("Duplicate key %s", u));
    };
  }

  /**
   * Returns a {@link Collector} that accumulates elements into a
   * {@link LinkedHashMap} whose keys and values are the result of applying the
   * provided mapping functions to the input elements.
   * <p>
   * If the mapped keys contains duplicates (according to
   * {@link Object#equals(Object)}), an {@link IllegalStateException} is
   * thrown when the collection operation is performed. If the mapped keys
   * may have duplicates, use {@link #toLinkedMap(Function, Function, BinaryOperator)}
   * instead.
   *
   * @author paouelle
   *
   * @param <T> the type of the input elements
   * @param <K> the output type of the key mapping function
   * @param <U> the output type of the value mapping function
   *
   * @param  keyMapper a mapping function to produce keys
   * @param  valueMapper a mapping function to produce values
   * @return a {@link Collector} which collects elements into a {@link LinkedHashMap}
   *         whose keys and values are the result of applying mapping functions
   *         to the input elements
   *
   * @see #toLinkedMap(Function, Function, BinaryOperator)
   */
  public static <T, K, U> Collector<T, ?, Map<K, U>> toLinkedMap(
    Function<? super T, ? extends K> keyMapper,
    Function<? super T, ? extends U> valueMapper
  ) {
    return java.util.stream.Collectors.toMap(
      keyMapper,
      valueMapper,
      Collectors.throwingMerger(),
      LinkedHashMap::new
    );
  }

  /**
   * Returns a {@link Collector} that accumulates elements into a
   * {@link LinkedHashMap} whose keys and values are the result of applying the
   * provided mapping functions to the input elements.
   * <p>
   * If the mapped keys contains duplicates (according to {@link Object#equals(Object)}),
   * the value mapping function is applied to each equal element, and the
   * results are merged using the provided merging function.
   *
   * @author paouelle
   *
   * @param <T> the type of the input elements
   * @param <K> the output type of the key mapping function
   * @param <U> the output type of the value mapping function
   *
   * @param  keyMapper a mapping function to produce keys
   * @param  valueMapper a mapping function to produce values
   * @param  mergeFunction a merge function, used to resolve collisions between
   *         values associated with the same key, as supplied
   *         to {@link Map#merge(Object, Object, BiFunction)}
   * @return a {@link Collector} which collects elements into a {@link LinkedHashMap}
   *         whose keys are the result of applying a key mapping function to the
   *         input elements, and whose values are the result of applying a value
   *         mapping function to all input elements equal to the key and combining
   *         them using the merge function
   *
   * @see #toLinkedMap(Function, Function)
   */
  public static <T, K, U> Collector<T, ?, Map<K, U>> toLinkedMap(
    Function<? super T, ? extends K> keyMapper,
    Function<? super T, ? extends U> valueMapper,
    BinaryOperator<U> mergeFunction
  ) {
    return java.util.stream.Collectors.toMap(
      keyMapper,
      valueMapper,
      mergeFunction,
      LinkedHashMap::new
    );
  }

  /**
   * Returns a {@link Collector} that accumulates elements into a
   * {@link TreeMap} whose keys and values are the result of applying the
   * provided mapping functions to the input elements.
   * <p>
   * If the mapped keys contains duplicates (according to
   * {@link Object#equals(Object)}), an {@link IllegalStateException} is
   * thrown when the collection operation is performed. If the mapped keys
   * may have duplicates, use {@link #toTreeMap(Function, Function, BinaryOperator)}
   * instead.
   *
   * @author paouelle
   *
   * @param <T> the type of the input elements
   * @param <K> the output type of the key mapping function
   * @param <U> the output type of the value mapping function
   *
   * @param  keyMapper a mapping function to produce keys
   * @param  valueMapper a mapping function to produce values
   * @return a {@link Collector} which collects elements into a {@link TreeMap}
   *         whose keys and values are the result of applying mapping functions
   *         to the input elements
   *
   * @see #toTreeMap(Function, Function, BinaryOperator)
   */
  public static <T, K, U> Collector<T, ?, TreeMap<K, U>> toTreeMap(
    Function<? super T, ? extends K> keyMapper,
    Function<? super T, ? extends U> valueMapper
  ) {
    return java.util.stream.Collectors.toMap(
      keyMapper,
      valueMapper,
      Collectors.throwingMerger(),
      TreeMap::new
    );
  }

  /**
   * Returns a {@link Collector} that accumulates elements into a
   * {@link TreeMap} whose keys and values are the result of applying the
   * provided mapping functions to the input elements.
   * <p>
   * If the mapped keys contains duplicates (according to {@link Object#equals(Object)}),
   * the value mapping function is applied to each equal element, and the
   * results are merged using the provided merging function.
   *
   * @author paouelle
   *
   * @param <T> the type of the input elements
   * @param <K> the output type of the key mapping function
   * @param <U> the output type of the value mapping function
   *
   * @param  keyMapper a mapping function to produce keys
   * @param  valueMapper a mapping function to produce values
   * @param  mergeFunction a merge function, used to resolve collisions between
   *         values associated with the same key, as supplied
   *         to {@link Map#merge(Object, Object, BiFunction)}
   * @return a {@link Collector} which collects elements into a {@link TreeMap}
   *         whose keys are the result of applying a key mapping function to the
   *         input elements, and whose values are the result of applying a value
   *         mapping function to all input elements equal to the key and combining
   *         them using the merge function
   *
   * @see #toTreeMap(Function, Function)
   */
  public static <T, K, U> Collector<T, ?, TreeMap<K, U>> toTreeMap(
    Function<? super T, ? extends K> keyMapper,
    Function<? super T, ? extends U> valueMapper,
    BinaryOperator<U> mergeFunction
  ) {
    return java.util.stream.Collectors.toMap(
      keyMapper,
      valueMapper,
      mergeFunction,
      TreeMap::new
    );
  }

  /**
   * Returns a {@link Collector} that accumulates elements into a
   * {@link IdentityHashMap} whose keys and values are the result of applying the
   * provided mapping functions to the input elements.
   * <p>
   * If the mapped keys contains duplicates (according to the <code>==</code>
   * operator, an {@link IllegalStateException} is thrown when the collection
   * operation is performed. If the mapped keys may have duplicates, use
   * {@link #toIdentityMap(Function, Function, BinaryOperator)} instead.
   *
   * @author paouelle
   *
   * @param <T> the type of the input elements
   * @param <K> the output type of the key mapping function
   * @param <U> the output type of the value mapping function
   *
   * @param  keyMapper a mapping function to produce keys
   * @param  valueMapper a mapping function to produce values
   * @return a {@link Collector} which collects elements into a {@link IdentityHashMap}
   *         whose keys and values are the result of applying mapping functions
   *         to the input elements
   *
   * @see #toIdentityMap(Function, Function, BinaryOperator)
   */
  public static <T, K, U> Collector<T, ?, Map<K, U>> toIdentityMap(
    Function<? super T, ? extends K> keyMapper,
    Function<? super T, ? extends U> valueMapper
  ) {
    return java.util.stream.Collectors.toMap(
      keyMapper,
      valueMapper,
      Collectors.throwingMerger(),
      IdentityHashMap::new
    );
  }

  /**
   * Returns a {@link Collector} that accumulates elements into a
   * {@link IdentityHashMap} whose keys and values are the result of applying the
   * provided mapping functions to the input elements.
   * <p>
   * If the mapped keys contains duplicates (according to the <code>==</code>
   * operator), the value mapping function is applied to each equal element, and
   * the results are merged using the provided merging function.
   *
   * @author paouelle
   *
   * @param <T> the type of the input elements
   * @param <K> the output type of the key mapping function
   * @param <U> the output type of the value mapping function
   *
   * @param  keyMapper a mapping function to produce keys
   * @param  valueMapper a mapping function to produce values
   * @param  mergeFunction a merge function, used to resolve collisions between
   *         values associated with the same key, as supplied
   *         to {@link Map#merge(Object, Object, BiFunction)}
   * @return a {@link Collector} which collects elements into a {@link IdentityHashMap}
   *         whose keys are the result of applying a key mapping function to the
   *         input elements, and whose values are the result of applying a value
   *         mapping function to all input elements equal to the key and combining
   *         them using the merge function
   *
   * @see #toLinkedMap(Function, Function)
   */
  public static <T, K, U> Collector<T, ?, Map<K, U>> toIdentityMap(
    Function<? super T, ? extends K> keyMapper,
    Function<? super T, ? extends U> valueMapper,
    BinaryOperator<U> mergeFunction
  ) {
    return java.util.stream.Collectors.toMap(
      keyMapper,
      valueMapper,
      mergeFunction,
      IdentityHashMap::new
    );
  }
}

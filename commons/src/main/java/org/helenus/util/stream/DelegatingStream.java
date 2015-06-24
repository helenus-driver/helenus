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

import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;
import java.util.Spliterator;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.stream.Collector;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * The <code>DelegatingStream</code> class defines a {@link Stream} implementation
 * that delegate operations to another stream.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jun 8, 2015 - paouelle - Creation
 *
 * @param <T> The type of objects in the stream
 *
 * @since 2.0
 */
public class DelegatingStream<T> implements Stream<T> {
  /**
   * Holds the stream to delegate to.
   *
   * @author paouelle
   */
  protected final Stream<T> delegate;

  /**
   * Instantiates a new <code>DelegatingStream</code> object.
   *
   * @author paouelle
   *
   * @param  delegate the stream to delegate to
   * @throws NullPointerException if <code>delegate</code> is <code>null</code>
   */
  public DelegatingStream(Stream<T> delegate) {
    org.apache.commons.lang3.Validate.notNull(delegate, "invalid null delegate");
    this.delegate = delegate;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.stream.BaseStream#iterator()
   */
  @Override
  public Iterator<T> iterator() {
    return delegate.iterator();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.stream.BaseStream#spliterator()
   */
  @Override
  public Spliterator<T> spliterator() {
    return delegate.spliterator();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.stream.BaseStream#isParallel()
   */
  @Override
  public boolean isParallel() {
    return delegate.isParallel();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.stream.BaseStream#sequential()
   */
  @Override
  public Stream<T> sequential() {
    return delegate.sequential();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.stream.BaseStream#parallel()
   */
  @Override
  public Stream<T> parallel() {
    return delegate.parallel();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.stream.BaseStream#unordered()
   */
  @Override
  public Stream<T> unordered() {
    return delegate.unordered();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.stream.BaseStream#onClose(java.lang.Runnable)
   */
  @Override
  public Stream<T> onClose(Runnable closeHandler) {
    return delegate.onClose(closeHandler);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.stream.BaseStream#close()
   */
  @Override
  public void close() {
    delegate.close();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.stream.Stream#filter(java.util.function.Predicate)
   */
  @Override
  public Stream<T> filter(Predicate<? super T> predicate) {
    return delegate.filter(predicate);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.stream.Stream#map(java.util.function.Function)
   */
  @Override
  public <R> Stream<R> map(Function<? super T, ? extends R> mapper) {
    return delegate.map(mapper);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.stream.Stream#mapToInt(java.util.function.ToIntFunction)
   */
  @Override
  public IntStream mapToInt(ToIntFunction<? super T> mapper) {
    return delegate.mapToInt(mapper);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.stream.Stream#mapToLong(java.util.function.ToLongFunction)
   */
  @Override
  public LongStream mapToLong(ToLongFunction<? super T> mapper) {
    return delegate.mapToLong(mapper);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.stream.Stream#mapToDouble(java.util.function.ToDoubleFunction)
   */
  @Override
  public DoubleStream mapToDouble(ToDoubleFunction<? super T> mapper) {
    return delegate.mapToDouble(mapper);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.stream.Stream#flatMap(java.util.function.Function)
   */
  @Override
  public <R> Stream<R> flatMap(
    Function<? super T, ? extends Stream<? extends R>> mapper
  ) {
    return delegate.flatMap(mapper);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.stream.Stream#flatMapToInt(java.util.function.Function)
   */
  @Override
  public IntStream flatMapToInt(
    Function<? super T, ? extends IntStream> mapper
  ) {
    return delegate.flatMapToInt(mapper);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.stream.Stream#flatMapToLong(java.util.function.Function)
   */
  @Override
  public LongStream flatMapToLong(
    Function<? super T, ? extends LongStream> mapper
  ) {
    return delegate.flatMapToLong(mapper);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.stream.Stream#flatMapToDouble(java.util.function.Function)
   */
  @Override
  public DoubleStream flatMapToDouble(
    Function<? super T, ? extends DoubleStream> mapper
  ) {
    return delegate.flatMapToDouble(mapper);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.stream.Stream#distinct()
   */
  @Override
  public Stream<T> distinct() {
    return delegate.distinct();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.stream.Stream#sorted()
   */
  @Override
  public Stream<T> sorted() {
    return delegate.sorted();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.stream.Stream#sorted(java.util.Comparator)
   */
  @Override
  public Stream<T> sorted(Comparator<? super T> comparator) {
    return delegate.sorted(comparator);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.stream.Stream#peek(java.util.function.Consumer)
   */
  @Override
  public Stream<T> peek(Consumer<? super T> consumer) {
    return delegate.peek(consumer);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.stream.Stream#limit(long)
   */
  @Override
  public Stream<T> limit(long maxSize) {
    return delegate.limit(maxSize);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.stream.Stream#skip(long)
   */
  @Override
  public Stream<T> skip(long n) {
    return delegate.skip(n);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.stream.Stream#forEach(java.util.function.Consumer)
   */
  @Override
  public void forEach(Consumer<? super T> consumer) {
    delegate.forEach(consumer);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.stream.Stream#forEachOrdered(java.util.function.Consumer)
   */
  @Override
  public void forEachOrdered(Consumer<? super T> action) {
    delegate.forEachOrdered(action);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.stream.Stream#toArray()
   */
  @Override
  public Object[] toArray() {
    return delegate.toArray();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.stream.Stream#toArray(java.util.function.IntFunction)
   */
  @Override
  public <A> A[] toArray(IntFunction<A[]> generator) {
    return delegate.toArray(generator);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.stream.Stream#reduce(java.lang.Object, java.util.function.BinaryOperator)
   */
  @Override
  public T reduce(T identity, BinaryOperator<T> reducer) {
    return delegate.reduce(identity, reducer);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.stream.Stream#reduce(java.util.function.BinaryOperator)
   */
  @Override
  public Optional<T> reduce(BinaryOperator<T> combiner) {
    return delegate.reduce(combiner);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.stream.Stream#reduce(java.lang.Object, java.util.function.BiFunction, java.util.function.BinaryOperator)
   */
  @Override
  public <U> U reduce(
    U identity,
    BiFunction<U, ? super T, U> accumulator,
    BinaryOperator<U> combiner
  ) {
    return delegate.reduce(identity, accumulator, combiner);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.stream.Stream#collect(java.util.function.Supplier, java.util.function.BiConsumer, java.util.function.BiConsumer)
   */
  @Override
  public <R> R collect(Supplier<R> supplier,
    BiConsumer<R, ? super T> accumulator,
    BiConsumer<R, R> combiner
  ) {
    return delegate.collect(supplier, accumulator, combiner);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.stream.Stream#collect(java.util.stream.Collector)
   */
  @Override
  public <R, A> R collect(Collector<? super T, A, R> collector) {
    return delegate.collect(collector);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.stream.Stream#min(java.util.Comparator)
   */
  @Override
  public Optional<T> min(Comparator<? super T> comparator) {
    return delegate.min(comparator);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.stream.Stream#max(java.util.Comparator)
   */
  @Override
  public Optional<T> max(Comparator<? super T> comparator) {
    return delegate.max(comparator);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.stream.Stream#count()
   */
  @Override
  public long count() {
    return delegate.count();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.stream.Stream#anyMatch(java.util.function.Predicate)
   */
  @Override
  public boolean anyMatch(Predicate<? super T> predicate) {
    return delegate.anyMatch(predicate);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.stream.Stream#allMatch(java.util.function.Predicate)
   */
  @Override
  public boolean allMatch(Predicate<? super T> predicate) {
    return delegate.allMatch(predicate);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.stream.Stream#noneMatch(java.util.function.Predicate)
   */
  @Override
  public boolean noneMatch(Predicate<? super T> predicate) {
    return delegate.noneMatch(predicate);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.stream.Stream#findFirst()
   */
  @Override
  public Optional<T> findFirst() {
    return delegate.findFirst();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.stream.Stream#findAny()
   */
  @Override
  public Optional<T> findAny() {
    return delegate.findAny();
  }
}
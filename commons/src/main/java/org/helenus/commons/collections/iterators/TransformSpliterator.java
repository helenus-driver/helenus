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
package org.helenus.commons.collections.iterators;

import java.util.Comparator;
import java.util.Spliterator;
import java.util.function.Consumer;

import org.apache.commons.collections4.Transformer;
import org.apache.commons.collections4.comparators.TransformingComparator;

/**
 * The <code>TransformSpliterator</code> class provides a useful extension to
 * the <code>Spliterator</code> interface in cases where a transforming function
 * needs to be applied to the split-iterator before returning any elements.
 * <p>
 * The implementation provided here creates a split iterator which wraps around
 * the given split iterator. The original split iterator is iterated as this
 * split iterator is being accessed.
 *
 * @copyright 2015-2016 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Sep 9, 2016 - paouelle - Creation
 *
 * @param <I> the type of element being iterated on input
 * @param <O> the type of element returned on output
 *
 * @since 1.0
 */
public class TransformSpliterator<I, O> implements Spliterator<O> {
  /**
   * Holds the iterator to be transformed.
   *
   * @author paouelle
   */
  private Spliterator<? extends I> i;

  /**
   * Holds the transformer.
   *
   * @author paouelle
   */
  private Transformer<? super I, ? extends O> transformer;

  /**
   * Holds the optional reverse transformer.
   *
   * @author paouelle
   */
  private Transformer<? super O, ? extends I> reverseTransformer;

  /**
   * Constructs a transforming split iterator using an input split iterator.
   *
   * @author paouelle
   *
   * @param i a split iterator to be transformed
   */
  public TransformSpliterator(Spliterator<? extends I> i) {
    this.i = i;
  }

  /**
   * Constructs a transforming split iterator using an input split iterator and the
   * specified transformer.
   *
   * @author paouelle
   *
   * @param i a split iterator to be transformed
   * @param transformer a transformer object
   */
  public TransformSpliterator(
    Spliterator<? extends I> i, Transformer<? super I, ? extends O> transformer
  ) {
    this.i = i;
    this.transformer = transformer;
  }

  /**
   * Constructs a transforming split iterator using an input split iterator and the
   * specified transformer.
   *
   * @author paouelle
   *
   * @param i a list iterator to be transformed
   * @param transformer a transformer object
   * @param reverseTransformer a reverse transformer object
   */
  public TransformSpliterator(
    Spliterator<? extends I> i,
    Transformer<? super I, ? extends O> transformer,
    Transformer<? super O, ? extends I> reverseTransformer
  ) {
    this.i = i;
    this.transformer = transformer;
    this.reverseTransformer = reverseTransformer;
  }

  /**
   * Transforms the given object using the transformer.
   *
   * @author paouelle
   *
   * @param  source the object to transform
   * @return the transformed object
   */
  protected O transform(I source) {
    return transformer.transform(source);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.Spliterator#tryAdvance(java.util.function.Consumer)
   */
  @Override
  public boolean tryAdvance(Consumer<? super O> action) {
    return i.tryAdvance(ie -> action.accept(transform((ie))));
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.Spliterator#forEachRemaining(java.util.function.Consumer)
   */
  @Override
  public void forEachRemaining(Consumer<? super O> action) {
    i.forEachRemaining(ie -> action.accept(transform((ie))));
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.Spliterator#trySplit()
   */
  @Override
  public Spliterator<O> trySplit() {
    return new TransformSpliterator<>(i.trySplit(), transformer);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.Spliterator#estimateSize()
   */
  @Override
  public long estimateSize() {
    return i.estimateSize();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.Spliterator#getExactSizeIfKnown()
   */
  @Override
  public long getExactSizeIfKnown() {
    return i.getExactSizeIfKnown();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.Spliterator#characteristics()
   */
  @Override
  public int characteristics() {
    return i.characteristics();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.Spliterator#hasCharacteristics(int)
   */
  @Override
  public boolean hasCharacteristics(int characteristics) {
    return i.hasCharacteristics(characteristics);
  }

  /**
   * {@inheritDoc}
   * <p>
   * <i>Note:</i> If the underlying split iterator is <code>SORTED</code> in a
   * non-natural order, this method will be unable to return a transformed
   * comparator as it would require
   * @author paouelle
   *
   * @see java.util.Spliterator#getComparator()
   */
  @Override
  public Comparator<? super O> getComparator() {
    @SuppressWarnings("unchecked")
    final Comparator<I> c = (Comparator<I>)i.getComparator();

    if (c == null) {
      return null;
    }
    // we need to return this comparator but since it is on internal objects
    // we first have to transform it using the reverse transformer
    // if none were provided then we won't be able to do much :-(
    if (reverseTransformer != null) {
      return new TransformingComparator<>(reverseTransformer, c);
    }
    throw new IllegalStateException("unable to transform comparator");
  }

  /**
   * Gets the split iterator this split iterator is using.
   *
   * @author paouelle
   *
   * @return the split iterator
   */
  public Spliterator<? extends I> getSpliterator() {
    return i;
  }

  /**
   * Sets the split iterator for this split iterator to use.
   * If iteration has started, this effectively resets the split iterator.
   *
   * @author paouelle
   *
   * @param iterator the split iterator to use
   */
  public void setSpliterator(Spliterator<? extends I> iterator) {
    this.i = iterator;
  }

  /**
   * Gets the transformer this split iterator is using.
   *
   * @author paouelle
   *
   * @return the transformer
   */
  public Transformer<? super I, ? extends O> getTransformer() {
    return transformer;
  }

  /**
   * Sets the transformer this split iterator will use.
   *
   * @author paouelle
   *
   * @param transformer the transformer to use
   */
  public void setTransformer(Transformer<? super I, ? extends O> transformer) {
    this.transformer = transformer;
  }

  /**
   * Gets the optional reverse transformer this split iterator needs if the source
   * is <code>SORTED</code> with a non-natural order.
   * <p>
   *
   * @author paouelle
   *
   * @return the reverse transformer to use in comparisons
   */
  public Transformer<? super O, ? extends I> getReverseTransformer() {
    return reverseTransformer;
  }

  /**
   * Sets the reverse transformer this split iterator needs if the source
   * is <code>SORTED</code> with a non-natural order.
   *
   * @author paouelle
   *
   * @param reverseTransformer the reverse transformer to use in comparisons
   */
  public void setReverseTransformer(
    Transformer<? super O, ? extends I> reverseTransformer
  ) {
    this.reverseTransformer = reverseTransformer;
  }
}


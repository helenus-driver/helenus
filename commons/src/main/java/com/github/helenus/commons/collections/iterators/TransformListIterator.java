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
package com.github.helenus.commons.collections.iterators;

import java.util.ListIterator;

import org.apache.commons.collections4.Transformer;
import org.apache.commons.collections4.iterators.TransformIterator;

/**
 * The <code>TransformListIterator</code> class provides a useful extension to
 * the <code>ListIterator</code> interface in cases where a transforming function
 * needs to be applied to the iterator before returning any elements.
 * <p>
 * The implementation provided here creates a list iterator which wraps around
 * the given list iterator. The original list iterator is iterated as this
 * list iterator is being accessed.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 15, 2015 - paouelle - Creation
 * @version 2 - Jan 15, 2015 - paouelle - Creation
 *            - Renamed and remodeled after {@link TransformIterator}
 *
 * @param <I> the type of element being iterated on input
 * @param <O> the type of element returned on output
 *
 * @since 1.0
 */
public class TransformListIterator<I, O> implements ListIterator<O> {
  /**
   * Holds the iterator to be transformed.
   *
   * @author The Helenus Driver Project Authors
   */
  private ListIterator<? extends I> i;

  /**
   * Holds the transformer.
   *
   * @author paouelle
   */
  private Transformer<? super I, ? extends O> transformer;

  /**
   * Constructs a transforming list iterator using an input list iterator.
   *
   * @author The Helenus Driver Project Authors
   *
   * @param i a list iterator to be transformed
   */
  public TransformListIterator(ListIterator<? extends I> i) {
    this.i = i;
  }

  /**
   * Constructs a transforming list iterator using an input list iterator and the
   * specified transformer.
   *
   * @author The Helenus Driver Project Authors
   *
   * @param i a list iterator to be transformed
   * @param transformer a transformer object
   */
  public TransformListIterator(
    ListIterator<? extends I> i, Transformer<? super I, ? extends O> transformer
  ) {
    this.i = i;
    this.transformer = transformer;
  }

  /**
   * Transforms the given object using the transformer.
   *
   * @author The Helenus Driver Project Authors
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
   * @see java.util.ListIterator#hasNext()
   */
  @Override
  public boolean hasNext() {
    return i.hasNext();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.ListIterator#next()
   */
  @Override
  public O next() {
    return transform(i.next());
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.ListIterator#remove()
   */
  @Override
  public void remove() {
    i.remove();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.ListIterator#hasPrevious()
   */
  @Override
  public boolean hasPrevious() {
    return i.hasPrevious();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.ListIterator#previous()
   */
  @Override
  public O previous() {
    return transform(i.previous());
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.ListIterator#nextIndex()
   */
  @Override
  public int nextIndex() {
    return i.nextIndex();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.ListIterator#previousIndex()
   */
  @Override
  public int previousIndex() {
    return i.previousIndex();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.ListIterator#set(java.lang.Object)
   */
  @Override
  public void set(O o) {
    throw new UnsupportedOperationException("FilterListIterator");
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.ListIterator#add(java.lang.Object)
   */
  @Override
  public void add(O o) {
    throw new UnsupportedOperationException("FilterListIterator");
  }

  /**
   * Gets the iterator this iterator is using.
   *
   * @author paouelle
   *
   * @return the iterator
   */
  public ListIterator<? extends I> getIterator() {
    return i;
  }

  /**
   * Sets the iterator for this iterator to use.
   * If iteration has started, this effectively resets the iterator.
   *
   * @author paouelle
   *
   * @param iterator the iterator to use
   */
  public void setIterator(ListIterator<? extends I> iterator) {
    this.i = iterator;
  }

  /**
   * Gets the transformer this iterator is using.
   *
   * @author paouelle
   *
   * @return the transformer
   */
  public Transformer<? super I, ? extends O> getTransformer() {
    return transformer;
  }

  /**
   * Sets the transformer this the iterator to use.
   *
   * @author paouelle
   *
   * @param transformer  the transformer to use
   */
  public void setTransformer(Transformer<? super I, ? extends O> transformer) {
    this.transformer = transformer;
  }
}

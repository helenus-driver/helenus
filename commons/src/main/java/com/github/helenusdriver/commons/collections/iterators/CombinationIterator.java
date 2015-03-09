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
package com.github.helenusdriver.commons.collections.iterators;

import java.lang.reflect.Array;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.commons.collections4.ResettableIterator;

/**
 * The <code>CombinationIterator</code> class defines an iterator capable of
 * combining iterating all possible combinations of multiple collections. At
 * each step, it returns a list of elements where each element correspond to the
 * iterated element of the collection specified at the same index
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 15, 2015 - paouelle - Creation
 *
 * @param <T> the type of elements being combined and iterated over
 *
 * @since 1.0
 */
public class CombinationIterator<T>
  implements Iterator<List<T>>, ResettableIterator<List<T>> {
  /**
   * Holds the class of the elements being iterated.
   *
   * @author paouelle
   */
  private final Class<T> clazz;

  /**
   * Holds the collection of elements being iterated.
   *
   * @author paouelle
   */
  private final Collection<T>[] items;

  /**
   * Holds the number of collections being iterated.
   *
   * @author paouelle
   */
  private final int size;

  /**
   * Flag indicating if a next element has been generated and is ready to be
   * returned.
   *
   * @author paouelle
   */
  private volatile boolean hasNext;

  /**
   * Flag indicating we are done iterating all combinations.
   *
   * @author paouelle
   */
  private volatile boolean finished;

  /**
   * Holds the list of iterators for all collections being iterated.
   *
   * @author paouelle
   */
  private volatile Iterator<T>[] iterators;

  /**
   * Holds the current set of elements generated from an iteration step.
   *
   * @author paouelle
   */
  private volatile T[] current;

  /**
   * Instantiates a new <code>CombinationIterator</code> object.
   *
   * @author paouelle
   *
   * @param  clazz the class of the elements being iterated
   * @param  items the collections of elements being iterated, the order will
   *         correspond to the order of the elements returned at each step
   * @throws NullPointerException if <code>clazz</code> or <code>items</code> is
   *          <code>null</code>
   * @throws IllegalArgumentException if no collections of items is provided
   */
  @SafeVarargs
  public CombinationIterator(Class<T> clazz, Collection<T>... items) {
    org.apache.commons.lang3.Validate.notNull(clazz, "invalid null class");
    org.apache.commons.lang3.Validate.notNull(items, "invalid null items");
    org.apache.commons.lang3.Validate.isTrue(
      items.length > 0, "no collection of items provided"
    );
    this.clazz = clazz;
    this.items = items;
    this.size = items.length;
    this.hasNext = false;
    this.finished = false;
    this.iterators = null;
    this.current = null;
  }

  /**
   * Instantiates a new <code>CombinationIterator</code> object.
   *
   * @author paouelle
   *
   * @param  clazz the class of the elements being iterated
   * @param  items the collections of elements being iterated, the order will
   *         correspond to the order of the elements returned at each step
   * @throws NullPointerException if <code>clazz</code> or <code>items</code> is
   *          <code>null</code>
   * @throws IllegalArgumentException if no collections of items is provided
   */
  public CombinationIterator(Class<T> clazz, Collection<Collection<T>> items) {
    org.apache.commons.lang3.Validate.notNull(clazz, "invalid null class");
    org.apache.commons.lang3.Validate.notNull(items, "invalid null items");
    org.apache.commons.lang3.Validate.isTrue(
      !items.isEmpty(), "no collection of items provided"
    );
    this.clazz = clazz;
    this.items = items.toArray(new Collection[items.size()]);
    this.size = items.size();
    this.hasNext = false;
    this.finished = false;
    this.iterators = null;
    this.current = null;
  }

  /**
   * Gets the number of combinations in this iterator.
   *
   * @author paouelle
   *
   * @return the number of combinations in this iterator
   */
  public int size() {
    int size = items[0].size();

    for (int i = 1; i < items.length; i++) {
      i *= items[i].size();
    }
    return size;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.Iterator#hasNext()
   */
  @SuppressWarnings({"cast", "unchecked"})
  @Override
  public boolean hasNext() {
    if (hasNext) {
      return true;
    }
    if (finished) {
      return false;
    }
    if (iterators == null) { // starting up
      this.iterators = (Iterator<T>[])new Iterator[size];
      this.current = (T[])Array.newInstance(clazz, size);
      for (int j = 0; j < size; j++) {
        final Iterator<T> i = items[j].iterator();

        iterators[j] = i;
        if (!i.hasNext()) { // we can't even start with a value
          this.finished = true;
          this.current = null;
          this.iterators = null;
          return false;
        }
        current[j] = i.next();
      }
      this.hasNext = true;
      return true;
    }
    if (current == null) {
      this.current = (T[])Array.newInstance(clazz, size);
    }
    // iterate the list in reverse
    for (int j = size - 1; j >= 0; j--) {
      Iterator<T> i = iterators[j];

      if (i.hasNext()) {
        current[j] = i.next();
        this.hasNext = true;
        return true;
      }
      // if we get here then that level is done iterating so reset its iterator
      // and move the previous one forward
      if (i instanceof ResettableIterator) {
        ((ResettableIterator<T>)i).reset();
      } else { // create a new one
        i = items[j].iterator();
      }
      if (!i.hasNext()) { // we can't even restart with a value
        break;
      }
      current[j] = i.next();
      iterators[j] = i;
    }
    // if we get here then we are done
    this.finished = true;
    this.current = null;
    this.iterators = null;
    return false;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.Iterator#next()
   */
  @Override
  public List<T> next() {
    if (!hasNext()) {
      throw new NoSuchElementException("CombinationIterator");
    }
    this.hasNext = false;
    return Arrays.asList(current);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.Iterator#remove()
   */
  @Override
  public void remove() {
    throw new UnsupportedOperationException("CombinationIterator");
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see ResettableIterator#reset()
   */
  @Override
  public void reset() {
    this.hasNext = false;
    this.finished = false;
    this.iterators = null;
    this.current = null;
  }
}

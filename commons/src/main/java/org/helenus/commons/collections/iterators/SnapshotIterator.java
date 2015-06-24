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

import java.io.Serializable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.commons.collections4.ResettableIterator;

/**
 * The <code>SnapshotIterator</code> class provides support for creating a
 * snapshot from another iterator; preserving the order of returned elements.
 * <p>
 * <i>Note:</i> An instance of this class can be properly serialized even if
 * the iterator object passed to its constructor is not serializable as long
 * as its elements are serializable. If the elements are of type
 * {@link java.util.Map.Entry}, then they will be cloned and made serializable
 * as long as the associated keys and values are serializable.
 * <p>
 * The implementation provided here takes a snapshot of all elements returned
 * by the given iterator by iterating them and then releasing the iterator.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 15, 2015 - paouelle - Creation
 *
 * @param <T> the type of elements iterated by the iterator
 *
 * @since 1.0
 */
public class SnapshotIterator<T>
  implements ResettableIterator<T>, Serializable {
  /**
   * Holds the serialVersionUID.
   *
   * @author paouelle
   */
  private static final long serialVersionUID = 5564723343035529469L;

  /**
   * Holds the elements to iterate.
   *
   * @author paouelle
   *
   * @serial the list of elements being iterated
   */
  private final List<T> list;

  /**
   * Holds the current index for the iterator.
   *
   * @author paouelle
   *
   * @serial the current index for the iterator
   */
  private int current = 0;

  /**
   * Instantiates a new <code>SnapshotIterator</code> object.
   *
   * @author paouelle
   *
   * @param  i the iterator to create a snapshot for
   * @throws NullPointerException if <code>i</code> is <code>null</code>
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public SnapshotIterator(Iterator<T> i) {
    org.apache.commons.lang3.Validate.notNull(i, "invalid null iterator");
    if (!i.hasNext()) {
      this.list = Collections.emptyList();
      return;
    }
    this.list = new ArrayList<>();
    while (i.hasNext()) {
      T e = i.next();

      if (e instanceof Map.Entry<?,?>) {
        // make sure we clone these entries to avoid someone modifying them
        // behind our back and also to make them serializable!!!!
        e = (T)new SnapshotEntry((Map.Entry<?,?>)e);
      }
      list.add(e);
    }
  }

  /**
   * Instantiates a new <code>SnapshotIterator</code> object.
   *
   * @author paouelle
   *
   * @param  i the iterable object to snapshot it's iterator
   * @throws NullPointerException if <code>i</code> is <code>null</code>
   */
  public SnapshotIterator(Iterable<T> i) {
    this(i.iterator());
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
    this.current = 0;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.Iterator#hasNext()
   */
  @Override
  public boolean hasNext() {
    return (current < list.size());
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.Iterator#next()
   */
  @Override
  public T next() {
    if (!hasNext()) {
      throw new NoSuchElementException("Snapshot Iterator");
    }
    return list.get(current++);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @throws UnsupportedOperationException always thrown
   *
   * @see java.util.Iterator#remove()
   */
  @Override
  public void remove() {
    throw new UnsupportedOperationException("Snapshot Iterator");
  }
}

/**
 * The <code>SnapshotEntry</code> class provides a snapshot
 * implementation of the {@link java.util.Map.Entry} interface.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 15, 2015 - paouelle - Creation
 *
 * @param <K> the key type
 * @param <V> the value type
 *
 * @since 1.0
 */
final class SnapshotEntry<K, V> implements Map.Entry<K, V>, Serializable {
  /**
   * Holds the serialVersionUID.
   *
   * @author paouelle
   */
  private static final long serialVersionUID = -5090863113841689882L;

  /**
   * Holds the entry hash code.
   *
   * @author paouelle
   *
   * @serial the hash code for the entry
   */
  private final int hash;

  /**
   * Holds the associated key.
   *
   * @author paouelle
   *
   * @serial the key
   */
  private final K key;

  /**
   * Holds the associated value.
   *
   * @author paouelle
   *
   * @serial the value
   */
  private final V value;

  /**
   * Instantiates a new <code>SnapshotEntry</code> object.
   *
   * @author paouelle
   *
   * @param  e the entry to be cloned
   * @throws NullPointerException if <code>e</code> is <code>null</code>
   */
  SnapshotEntry(Map.Entry<K, V> e) {
    org.apache.commons.lang3.Validate.notNull(e, "invalid null entry");
    this.hash = e.hashCode();
    this.key = e.getKey();
    this.value = e.getValue();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.Map.Entry#getKey()
   */
  @Override
  public K getKey() {
    return key;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.Map.Entry#getValue()
   */
  @Override
  public V getValue() {
    return value;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @throws UnsupportedOperationException always thrown
   *
   * @see java.util.Map.Entry#setValue(java.lang.Object)
   */
  @Override
  public V setValue(V value) {
    throw new UnsupportedOperationException("Snapshot Entry");
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object o) {
    if (!(o instanceof Map.Entry<?,?>)) {
      return false;
    }
    final Map.Entry<?,?> e = (Map.Entry<?,?>)o;

    return (((key == null) ? (e.getKey() == null) : key.equals(e.getKey()))
            && ((value == null) ? (e.getValue() == null) : value.equals(e.getValue())));
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    return hash;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return key + "=" + value;
  }
}

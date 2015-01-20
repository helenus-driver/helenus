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
package com.github.helenus.driver.impl;

import java.util.AbstractList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections4.iterators.TransformIterator;

import com.github.helenus.commons.collections.iterators.TransformListIterator;
import com.github.helenus.persistence.Persisted;
import com.github.helenus.persistence.Persister;

/**
 * The <code>PersistedList</code> class provides a {@link List} implementation
 * suitable to hold persisted values.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 19, 2015 - paouelle - Creation
 *
 * @param <T> the decoded type of the elements
 * @param <PT> the persisted type from which we are decoding and to which we are
 *             encoding each elements
 *
 * @since 1.0
 */
public class PersistedList<T, PT>
  extends AbstractList<T> implements PersistedObject<T, PT> {
  /**
   * Holds the persisted annotation for this list.
   *
   * @author paouelle
   */
  private final Persisted persisted;

  /**
   * Holds the persister to use when encoding/decoding values.
   *
   * @author paouelle
   */
  private final Persister<T, PT> persister;

  /**
   * Holds the field name holding this encoded list.
   *
   * @author paouelle
   */
  private final String fname;

  /**
   * Holds the underlying list.
   *
   * @author paouelle
   */
  private final List<PersistedValue<T, PT>> list;

  /**
   * Instantiates a new <code>PersistedList</code> object.
   *
   * @author paouelle
   *
   * @param  persisted the non-<code>null</code> persisted annotation
   * @param  persister the non-<code>null</code> persister
   * @param  fname the non-<code>null</code> field name
   * @param  list the non-<code>null</code> encoded/decoded list
   * @param  encoded <code>true</code> if the list contains encoded values;
   *         <code>false</code> if it contains decoded values (this will force
   *         all values to be encoded)
   * @throws IllegalArgumentException if unable to encode/decode the values properly
   * @throws ClassCastException if any values cannot be encoded to the expected type
   */
  @SuppressWarnings("unchecked")
  PersistedList(
    Persisted persisted,
    Persister<T, PT> persister,
    String fname,
    List<?> list,
    boolean encoded
  ) {
    this.persisted = persisted;
    this.persister = persister;
    this.fname = fname;
    if (encoded) {
      this.list = ((List<PT>)list).stream()
          .map(
            pt -> new PersistedValue<>(persisted, persister, fname)
                  .setEncodedValue(pt)
          )
          .collect(Collectors.toList());
    } else {
      this.list = ((List<T>)list).stream()
          .map(
            t -> {
              final PersistedValue<T, PT> pval = new PersistedValue<>(
                persisted, persister, fname
              ).setDecodedValue(t);

              pval.getEncodedValue(); // force it to be encoded
              return pval;
            }
          )
          .collect(Collectors.toList());
    }
  }

  /**
   * Instantiates a new <code>PersistedList</code> object.
   *
   * @author paouelle
   *
   * @param  plist the main list we are creating a sublist for
   * @param  fromIndex low endpoint (inclusive) of the subList
   * @param  toIndex high endpoint (exclusive) of the subList
   * @throws IndexOutOfBoundsException if an endpoint index value is out of range
   *         {@code (fromIndex < 0 || toIndex > size)}
   * @throws IllegalArgumentException if the endpoint indices are out of order
   *         {@code (fromIndex > toIndex)}
   */
  private PersistedList(PersistedList<T, PT> plist, int fromIndex, int toIndex) {
    this.persisted = plist.persisted;
    this.persister = plist.persister;
    this.fname = plist.fname;
    this.list = plist.list.subList(fromIndex, toIndex);
  }

  /**
   * Gets the persisted list.
   *
   * @author paouelle
   *
   * @return the non-<code>null</code> persisted list
   */
  public List<PersistedValue<T, PT>> getPersistedList() {
    return list;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.List#size()
   */
  @Override
  public int size() {
    return list.size();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.List#isEmpty()
   */
  @Override
  public boolean isEmpty() {
    return list.isEmpty();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.List#iterator()
   */
  @Override
  public Iterator<T> iterator() {
    return new TransformIterator<PersistedValue<T, PT>, T>(list.iterator()) {
      @Override
      protected T transform(PersistedValue<T, PT> pv) {
        return pv.getDecodedValue();
      }
    };
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.List#listIterator(int)
   */
  @Override
  public ListIterator<T> listIterator(int index) {
    return new TransformListIterator<PersistedValue<T, PT>, T>(list.listIterator()) {
      @Override
      protected T transform(PersistedValue<T, PT> pv) {
        return pv.getDecodedValue();
      }
    };
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.Collection#stream()
   */
  @Override
  public Stream<T> stream() {
    return list.stream().map(pv -> pv.getDecodedValue());
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.Collection#parallelStream()
   */
  @Override
  public Stream<T> parallelStream() {
    return list.parallelStream().map(pv -> pv.getDecodedValue());
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.List#add(java.lang.Object)
   */
  @Override
  public boolean add(T e) {
    list.add(new PersistedValue<>(persisted, persister, fname).setDecodedValue(e));
    return true;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.List#add(int, java.lang.Object)
   */
  @Override
  public void add(int index, T e) {
    list.add(index, new PersistedValue<>(persisted, persister, fname).setDecodedValue(e));
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.List#clear()
   */
  @Override
  public void clear() {
    list.clear();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.List#get(int)
   */
  @Override
  public T get(int index) {
    return list.get(index).getDecodedValue();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.List#set(int, java.lang.Object)
   */
  @Override
  public T set(int index, T e) {
    return list.set(
      index, new PersistedValue<>(persisted, persister, fname).setDecodedValue(e)
    ).getDecodedValue();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.List#remove(int)
   */
  @Override
  public T remove(int index) {
    return list.remove(index).getDecodedValue();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.List#subList(int, int)
   */
  @Override
  public List<T> subList(int fromIndex, int toIndex) {
    return new PersistedList<>(this, fromIndex, toIndex);
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
    return list.toString();
  }
}

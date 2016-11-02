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
package org.helenus.driver.impl;

import java.util.AbstractSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.Spliterator;
import java.util.TreeSet;
import java.util.stream.Stream;

import org.apache.commons.collections4.iterators.TransformIterator;

import org.helenus.commons.collections.iterators.TransformSpliterator;
import org.helenus.driver.persistence.Persisted;
import org.helenus.driver.persistence.Persister;

/**
 * The <code>PersistedSet</code> class provides a {@link Set} implementation
 * suitable to hold persisted values.
 *
 * @copyright 2015-2016 The Helenus Driver Project Authors
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
public class PersistedSet<T, PT>
  extends AbstractSet<T> implements PersistedObject<T, PT> {
  /**
   * Creates a new empty set that resembles the type of the provided set.
   *
   * @author paouelle
   *
   * @param <NV> the new set value type
   *
   * @param  set the non-<code>null</code> set to create a resembling one from
   * @return a new map that resembles the provided one (i.e. {@link TreeSet},
   *         {@link LinkedHashSet}, or {@link HashSet}
   */
  private static <NV> Set<NV> newSet(Set<?> set) {
    if (set instanceof SortedSet) {
      return new TreeSet<>();
    } else if (set instanceof LinkedHashSet) {
      return new LinkedHashSet<>(set.size() * 3 / 2);
    }
    return new HashSet<>(set.size() * 3 / 2);
  }

  /**
   * Holds the persisted annotation for this set.
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
   * Holds the field name holding this encoded set.
   *
   * @author paouelle
   */
  private final String fname;

  /**
   * Holds the underlying set.
   *
   * @author paouelle
   */
  private final Set<PersistedValue<T, PT>> set;

  /**
   * Instantiates a new <code>PersistedMap</code> object.
   *
   * @author paouelle
   *
   * @param pset the persisted set from which we are creating another view
   * @param set the set view we are creating
   */
  PersistedSet(
    PersistedSet<T, PT> pset, Set<PersistedValue<T, PT>> set
  ) {
    this.persisted = pset.persisted;
    this.persister = pset.persister;
    this.fname = pset.fname;
    this.set = set;
  }

  /**
   * Instantiates a new <code>PersistedSet</code> object.
   *
   * @author paouelle
   *
   * @param  persisted the non-<code>null</code> persisted annotation
   * @param  persister the non-<code>null</code> persister
   * @param  fname the non-<code>null</code> field name
   * @param  pset the underlying persisted set
   * @param  set the non-<code>null</code> encoded/decoded set
   * @param  encoded <code>true</code> if the set contains encoded values;
   *         <code>false</code> if it contains decoded values (this will force
   *         all values to be encoded)
   * @throws IllegalArgumentException if unable to encode/decode the values properly
   * @throws ClassCastException if any values cannot be encoded to the expected type
   */
  @SuppressWarnings("unchecked")
  PersistedSet(
    Persisted persisted,
    Persister<T, PT> persister,
    String fname,
    Set<PersistedValue<T, PT>> pset,
    Set<?> set,
    boolean encoded
  ) {
    this.persisted = persisted;
    this.persister = persister;
    this.fname = fname;
    this.set = pset;
    if (encoded) {
       ((Set<PT>)set).forEach(
         pt -> pset.add(new PersistedValue<>(persisted, persister, fname)
               .setEncodedValue(pt))
       );
    } else {
      ((Set<T>)set).forEach(
        t -> {
          final PersistedValue<T, PT> pval = new PersistedValue<>(
            persisted, persister, fname
          ).setDecodedValue(t);

          pval.getEncodedValue(); // force it to be encoded
          pset.add(pval);
        });
    }
  }

  /**
   * Instantiates a new <code>PersistedSet</code> object.
   *
   * @author paouelle
   *
   * @param  persisted the non-<code>null</code> persisted annotation
   * @param  persister the non-<code>null</code> persister
   * @param  fname the non-<code>null</code> field name
   * @param  set the non-<code>null</code> encoded/decoded set
   * @param  encoded <code>true</code> if the set contains encoded values;
   *         <code>false</code> if it contains decoded values (this will force
   *         all values to be encoded)
   * @throws IllegalArgumentException if unable to encode/decode the values properly
   * @throws ClassCastException if any values cannot be encoded to the expected type
   */
  @SuppressWarnings("unchecked")
  PersistedSet(
    Persisted persisted,
    Persister<T, PT> persister,
    String fname,
    Set<?> set,
    boolean encoded
  ) {
    this(persisted, persister, fname, PersistedSet.newSet(set), set, encoded);
  }

  /**
   * Gets the persisted set.
   *
   * @author paouelle
   *
   * @return the non-<code>null</code> persisted set
   */
  public Set<PersistedValue<T, PT>> getPersistedSet() {
    return set;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.Set#size()
   */
  @Override
  public int size() {
    return set.size();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.Set#isEmpty()
   */
  @Override
  public boolean isEmpty() {
    return set.isEmpty();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.Set#iterator()
   */
  @Override
  public Iterator<T> iterator() {
    return new TransformIterator<PersistedValue<T, PT>, T>(set.iterator()) {
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
   * @see java.util.LinkedHashSet#spliterator()
   */
  @Override
  public Spliterator<T> spliterator() {
    return new TransformSpliterator<PersistedValue<T, PT>, T>(set.spliterator()) {
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
    return set.stream().map(pv -> pv.getDecodedValue());
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
    return set.parallelStream().map(pv -> pv.getDecodedValue());
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.Set#add(java.lang.Object)
   */
  @Override
  public boolean add(T e) {
    return set.add(
      new PersistedValue<>(persisted, persister, fname).setDecodedValue(e)
    );
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.Set#remove(java.lang.Object)
   */
  @Override
  public boolean remove(Object o) {
    return set.remove(
      new PersistedValue<>(persisted, persister, fname).setDecodedValue(
        persister.getDecodedClass().cast(o)
      )
    );
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.Set#clear()
   */
  @Override
  public void clear() {
    set.clear();
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
    return set.toString();
  }
}

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

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;
import java.util.SortedSet;
import java.util.Spliterator;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Stream;

import org.helenus.driver.persistence.Persisted;
import org.helenus.driver.persistence.Persister;

/**
 * The <code>PersistedSortedSet</code> class provides a {@link SortedSet}
 * implementation suitable to hold persisted values.
 *
 * @copyright 2015-2016 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Nov 2, 2016 - paouelle - Creation
 *
 * @param <T> the decoded type of the elements
 * @param <PT> the persisted type from which we are decoding and to which we are
 *             encoding each elements
 *
 * @since 1.0
 */
public class PersistedSortedSet<T, PT>
  extends TreeSet<T> implements PersistedObject<T, PT> {
  /**
   * Holds the serialVersionUID.
   *
   * @author <a href="mailto:paouelle@enlightedinc.com">paouelle</a>
   */
  private static final long serialVersionUID = -8520854665439809920L;

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
   * Holds an underlying map keyed by decoded values to their persisted ones.
   *
   * @author paouelle
   */
  private final TreeMap<T, PersistedValue<T, PT>> map;

  /**
   * Instantiates a new <code>PersistedSortedSet</code> object.
   *
   * @author paouelle
   *
   * @param set the non-<code>null</code> set to clone
   */
  @SuppressWarnings("unchecked")
  PersistedSortedSet(PersistedSortedSet<T, PT> set) {
    this.persisted = set.persisted;
    this.persister = set.persister;
    this.fname = set.fname;
    this.map = (TreeMap<T, PersistedValue<T, PT>>)set.map.clone();
  }

  /**
   * Instantiates a new <code>PersistedSortedSet</code> object.
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
  PersistedSortedSet(
    Persisted persisted,
    Persister<T, PT> persister,
    String fname,
    Set<?> set,
    boolean encoded
  ) {
    this.persisted = persisted;
    this.persister = persister;
    this.fname = fname;
    if (encoded) {
      // we must decoded all values to properly sort :-(
      this.map = ((Set<PT>)set).stream()
        .map(pt -> new PersistedValue<>(persisted, persister, fname).setEncodedValue(pt))
        .collect(org.helenus.util.stream.Collectors.toTreeMap(
          PersistedValue::getDecodedValue, pt -> pt
        ));
    } else {
      this.map = ((Set<T>)set).stream()
        .map(t -> {
          final PersistedValue<T, PT> pval = new PersistedValue<>(
              persisted, persister, fname
            ).setDecodedValue(t);

            pval.getEncodedValue(); // force it to be encoded
            return pval;
          })
          .collect(org.helenus.util.stream.Collectors.toTreeMap(
            PersistedValue::getDecodedValue, pval -> pval
          ));
    }
  }

  /**
   * Gets the persisted set.
   *
   * @author paouelle
   *
   * @return the non-<code>null</code> persisted set
   */
  public Collection<PersistedValue<T, PT>> getPersistedSet() {
    return map.values();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.SortedSet#comparator()
   */
  @Override
  public Comparator<? super T> comparator() {
    return map.comparator();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.HashSet#size()
   */
  @Override
  public int size() {
    return map.size();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.HashSet#isEmpty()
   */
  @Override
  public boolean isEmpty() {
    return map.isEmpty();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.HashSet#iterator()
   */
  @Override
  public Iterator<T> iterator() {
    return map.keySet().iterator();
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
    return map.keySet().spliterator();
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
    return map.keySet().stream();
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
    return map.keySet().parallelStream();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.HashSet#contains(java.lang.Object)
   */
  @Override
  public boolean contains(Object o) {
    return map.containsKey(o);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.HashSet#add(java.lang.Object)
   */
  @Override
  public boolean add(T e) {
    return (map.put(
      e, new PersistedValue<>(persisted, persister, fname).setDecodedValue(e)
    ) == null);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.HashSet#remove(java.lang.Object)
   */
  @Override
  public boolean remove(Object o) {
    return (map.remove(o) != null);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.HashSet#clear()
   */
  @Override
  public void clear() {
    map.clear();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.AbstractCollection#toString()
   */
  @Override
  public String toString() {
    return map.values().toString();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.HashSet#clone()
   */
  @Override
  public Object clone() {
    return new PersistedSortedSet<>(this);
  }
}

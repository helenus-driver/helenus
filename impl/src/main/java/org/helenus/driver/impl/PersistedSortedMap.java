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

import java.util.Comparator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.helenus.driver.persistence.Persisted;
import org.helenus.driver.persistence.Persister;

/**
 * The <code>PersistedSortedMap</code> class provides a {@link SortedMap}
 * implementation suitable to hold persisted values.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Nov 10, 2015 - paouelle - Creation
 *
 * @param <K> the type of the keys
 * @param <T> the decoded type of the values
 * @param <PT> the persisted type from which we are decoding and to which we are
 *             encoding each values
 *
 * @since 1.0
 */
public class PersistedSortedMap<K, T, PT>
  extends PersistedMap<K, T, PT>
  implements SortedMap<K, T>, PersistedObject<T, PT> {
  /**
   * Holds the underlying map.
   *
   * @author paouelle
   */
  final SortedMap<K, PersistedValue<T, PT>> map;

  /**
   * Instantiates a new <code>PersistedSortedMap</code> object.
   *
   * @author paouelle
   *
   * @param pmap the persisted map from which we are creating another view
   * @param map the map view we are creating
   */
  PersistedSortedMap(
    PersistedSortedMap<K, T, PT> pmap, SortedMap<K, PersistedValue<T, PT>> map
  ) {
    super(pmap, map);
    this.map = map;
  }

  /**
   * Instantiates a new <code>PersistedSortedMap</code> object.
   *
   * @author paouelle
   *
   * @param  persisted the non-<code>null</code> persisted annotation
   * @param  persister the non-<code>null</code> persister
   * @param  fname the non-<code>null</code> field name
   * @param  map the non-<code>null</code> encoded/decoded map
   * @param  encoded <code>true</code> if the map contains encoded values;
   *         <code>false</code> if it contains decoded values (this will force
   *         all values to be encoded)
   * @throws IllegalArgumentException if unable to encode/decode the values properly
   * @throws ClassCastException if any values cannot be encoded to the expected type
   */
  @SuppressWarnings("unchecked")
  PersistedSortedMap(
    Persisted persisted,
    Persister<T, PT> persister,
    String fname,
    Map<K, ?> map,
    boolean encoded
  ) {
    super(
      persisted,
      persister,
      fname,
      new TreeMap<>(),
      map,
      encoded
    );
    this.map = (SortedMap<K, PersistedValue<T, PT>>)map;
  }

  /**
   * {@inheritDoc}
   *
   * @author <a href="mailto:paouelle@enlightedinc.com">paouelle</a>
   *
   * @see org.helenus.driver.impl.PersistedMap#getPersistedMap()
   */
  @Override
  public SortedMap<K, PersistedValue<T, PT>> getPersistedMap() {
    return map;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.SortedMap#comparator()
   */
  @Override
  public Comparator<? super K> comparator() {
    return map.comparator();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.SortedMap#firstKey()
   */
  @Override
  public K firstKey() {
    return map.firstKey();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.SortedMap#lastKey()
   */
  @Override
  public K lastKey() {
    return map.lastKey();
  }

  /**
   * {@inheritDoc}
   *
   * @author <a href="mailto:paouelle@enlightedinc.com">paouelle</a>
   *
   * @see java.util.SortedMap#subMap(java.lang.Object, java.lang.Object)
   */
  @Override
  public SortedMap<K, T> subMap(K fromKey, K toKey) {
    return new PersistedSortedMap<>(this, map.subMap(fromKey, toKey));
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.SortedMap#headMap(java.lang.Object)
   */
  @Override
  public SortedMap<K, T> headMap(K toKey) {
    return new PersistedSortedMap<>(this, map.headMap(toKey));
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.SortedMap#tailMap(java.lang.Object)
   */
  @Override
  public SortedMap<K, T> tailMap(K fromKey) {
    return new PersistedSortedMap<>(this, map.tailMap(fromKey));
  }
}

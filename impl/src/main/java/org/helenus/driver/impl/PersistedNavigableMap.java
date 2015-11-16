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

import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;

import org.helenus.driver.persistence.Persisted;
import org.helenus.driver.persistence.Persister;

/**
 * The <code>PersistedNavigableMap</code> class provides a {@link NavigableMap}
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
public class PersistedNavigableMap<K, T, PT>
  extends PersistedSortedMap<K, T, PT>
  implements NavigableMap<K, T>, PersistedObject<T, PT> {
  /**
   * Holds the underlying map.
   *
   * @author paouelle
   */
  final NavigableMap<K, PersistedValue<T, PT>> map;

  /**
   * Instantiates a new <code>PersistedTreeMap</code> object.
   *
   * @author paouelle
   *
   * @param pmap the persisted map from which we are creating another view
   * @param map the map view we are creating
   */
  PersistedNavigableMap(
    PersistedNavigableMap<K, T, PT> pmap,
    NavigableMap<K, PersistedValue<T, PT>> map
  ) {
    super(pmap, map);
    this.map = map;
  }

  /**
   * Instantiates a new <code>PersistedTreeMap</code> object.
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
  PersistedNavigableMap(
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
      map,
      encoded
    );
    this.map = (NavigableMap<K, PersistedValue<T, PT>>)map;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.PersistedMap#getPersistedMap()
   */
  @Override
  public NavigableMap<K, PersistedValue<T, PT>> getPersistedMap() {
    return map;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.NavigableMap#lowerEntry(java.lang.Object)
   */
  @Override
  public java.util.Map.Entry<K, T> lowerEntry(K key) {
    final java.util.Map.Entry<K, PersistedValue<T, PT>> e = map.lowerEntry(key);

    return (e != null) ? new PersistedEntry(e) : null;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.NavigableMap#lowerKey(java.lang.Object)
   */
  @Override
  public K lowerKey(K key) {
    return map.lowerKey(key);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.NavigableMap#floorEntry(java.lang.Object)
   */
  @Override
  public java.util.Map.Entry<K, T> floorEntry(K key) {
    final java.util.Map.Entry<K, PersistedValue<T, PT>> e = map.floorEntry(key);

    return (e != null) ? new PersistedEntry(e) : null;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.NavigableMap#floorKey(java.lang.Object)
   */
  @Override
  public K floorKey(K key) {
    return map.floorKey(key);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.NavigableMap#ceilingEntry(java.lang.Object)
   */
  @Override
  public java.util.Map.Entry<K, T> ceilingEntry(K key) {
    final java.util.Map.Entry<K, PersistedValue<T, PT>> e = map.ceilingEntry(key);

    return (e != null) ? new PersistedEntry(e) : null;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.NavigableMap#ceilingKey(java.lang.Object)
   */
  @Override
  public K ceilingKey(K key) {
    return map.ceilingKey(key);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.NavigableMap#higherEntry(java.lang.Object)
   */
  @Override
  public java.util.Map.Entry<K, T> higherEntry(K key) {
    final java.util.Map.Entry<K, PersistedValue<T, PT>> e = map.higherEntry(key);

    return (e != null) ? new PersistedEntry(e) : null;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.NavigableMap#higherKey(java.lang.Object)
   */
  @Override
  public K higherKey(K key) {
    return map.higherKey(key);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.NavigableMap#firstEntry()
   */
  @Override
  public java.util.Map.Entry<K, T> firstEntry() {
    final java.util.Map.Entry<K, PersistedValue<T, PT>> e = map.firstEntry();

    return (e != null) ? new PersistedEntry(e) : null;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.NavigableMap#lastEntry()
   */
  @Override
  public java.util.Map.Entry<K, T> lastEntry() {
    final java.util.Map.Entry<K, PersistedValue<T, PT>> e = map.lastEntry();

    return (e != null) ? new PersistedEntry(e) : null;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.NavigableMap#pollFirstEntry()
   */
  @Override
  public java.util.Map.Entry<K, T> pollFirstEntry() {
    final java.util.Map.Entry<K, PersistedValue<T, PT>> e = map.pollFirstEntry();

    return (e != null) ? new PersistedEntry(e) : null;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.NavigableMap#pollLastEntry()
   */
  @Override
  public java.util.Map.Entry<K, T> pollLastEntry() {
    final java.util.Map.Entry<K, PersistedValue<T, PT>> e = map.pollLastEntry();

    return (e != null) ? new PersistedEntry(e) : null;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.NavigableMap#descendingMap()
   */
  @Override
  public NavigableMap<K, T> descendingMap() {
    return new PersistedNavigableMap<>(this, map.descendingMap());
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.NavigableMap#navigableKeySet()
   */
  @Override
  public NavigableSet<K> navigableKeySet() {
    return map.navigableKeySet();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.NavigableMap#descendingKeySet()
   */
  @Override
  public NavigableSet<K> descendingKeySet() {
    return map.descendingKeySet();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.NavigableMap#subMap(java.lang.Object, boolean, java.lang.Object, boolean)
   */
  @Override
  public NavigableMap<K, T> subMap(
    K fromKey, boolean fromInclusive, K toKey, boolean toInclusive
  ) {
    return new PersistedNavigableMap<>(
      this, map.subMap(fromKey, fromInclusive, toKey, toInclusive)
    );
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.NavigableMap#headMap(java.lang.Object, boolean)
   */
  @Override
  public NavigableMap<K, T> headMap(K toKey, boolean inclusive) {
    return new PersistedNavigableMap<>(this, map.headMap(toKey, inclusive));
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.NavigableMap#tailMap(java.lang.Object, boolean)
   */
  @Override
  public NavigableMap<K, T> tailMap(K fromKey, boolean inclusive) {
    return new PersistedNavigableMap<>(this, map.tailMap(fromKey, inclusive));
  }
}

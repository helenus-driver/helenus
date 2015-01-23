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
package com.github.helenusdriver.driver.impl;

import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.commons.collections4.iterators.TransformIterator;

import com.github.helenusdriver.persistence.Persisted;
import com.github.helenusdriver.persistence.Persister;

/**
 * The <code>PersistedMap</code> class provides a {@link Map} implementation
 * suitable to hold persisted values.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 19, 2015 - paouelle - Creation
 *
 * @param <K> the type of the keys
 * @param <T> the decoded type of the values
 * @param <PT> the persisted type from which we are decoding and to which we are
 *             encoding each values
 *
 * @since 1.0
 */
public class PersistedMap<K, T, PT>
  extends AbstractMap<K, T> implements PersistedObject<T, PT> {
  /**
   * Holds the persisted annotation for this map.
   *
   * @author paouelle
   */
  final Persisted persisted;

  /**
   * Holds the persister to use when encoding/decoding values.
   *
   * @author paouelle
   */
  final Persister<T, PT> persister;

  /**
   * Holds the field name holding this encoded map.
   *
   * @author paouelle
   */
  final String fname;

  /**
   * Holds the underlying map.
   *
   * @author paouelle
   */
  final Map<K, PersistedValue<T, PT>> map;

  /**
   * Holds the entry set view of this map.
   *
   * @author paouelle
   */
  private Set<Map.Entry<K, T>> eset = null;

  /**
   * Holds the values view of this map.
   *
   * @author paouelle
   */
  private Collection<T> vcol = null;

  /**
   * Instantiates a new <code>PersistedList</code> object.
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
  PersistedMap(
    Persisted persisted,
    Persister<T, PT> persister,
    String fname,
    Map<K, ?> map,
    boolean encoded
  ) {
    this.persisted = persisted;
    this.persister = persister;
    this.fname = fname;
    this.map = new LinkedHashMap<>(map.size()); // to preserve order
    if (encoded) {
      for (final Map.Entry<K, PT> e: ((Map<K, PT>)map).entrySet()) {
        this.map.put(
          e.getKey(),
          new PersistedValue<>(
            persisted, persister, fname).setEncodedValue(e.getValue()
          )
        );
      }
    } else {
      for (final Map.Entry<K, T> e: ((Map<K, T>)map).entrySet()) {
        final PersistedValue<T, PT> pval = new PersistedValue<>(
          persisted, persister, fname
        ).setDecodedValue(e.getValue());

        pval.getEncodedValue(); // force it to be encoded
        this.map.put(e.getKey(), pval);
      }
    }
  }

  /**
   * Gets the persisted map.
   *
   * @author paouelle
   *
   * @return the non-<code>null</code> persisted map
   */
  public Map<K, PersistedValue<T, PT>> getPersistedMap() {
    return map;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.Map#size()
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
   * @see java.util.Map#isEmpty()
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
   * @see java.util.Map#containsKey(java.lang.Object)
   */
  @Override
  public boolean containsKey(Object key) {
    return map.containsKey(key);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.Map#get(java.lang.Object)
   */
  @Override
  public T get(Object key) {
    final PersistedValue<T, PT> pval = map.get(key);

    return (pval != null) ? pval.getDecodedValue() : null;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.Map#put(java.lang.Object, java.lang.Object)
   */
  @Override
  public T put(K key, T value) {
    final PersistedValue<T, PT> pval = map.put(
      key,
      new PersistedValue<>(persisted, persister, fname).setDecodedValue(value)
    );

    return (pval != null) ? pval.getDecodedValue() : null;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.Map#remove(java.lang.Object)
   */
  @Override
  public T remove(Object key) {
    final PersistedValue<T, PT> pval = map.remove(key);

    return (pval != null) ? pval.getDecodedValue() : null;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.Map#clear()
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
   * @see java.util.Map#keySet()
   */
  @Override
  public Set<K> keySet() {
    return map.keySet();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.Map#values()
   */
  @Override
  public Collection<T> values() {
    if (vcol == null) {
      final Collection<PersistedValue<T, PT>> vcol = map.values();

      this.vcol = new AbstractCollection<T>() {
        @Override
        public int size() {
          return vcol.size();
        }
        @Override
        public boolean isEmpty() {
          return vcol.isEmpty();
        }
        @Override
        public Iterator<T> iterator() {
          return new TransformIterator<PersistedValue<T, PT>, T>(vcol.iterator()) {
            @Override
            protected T transform(PersistedValue<T, PT> pv) {
              return pv.getDecodedValue();
            }
          };
        }
        @Override
        public Stream<T> stream() {
          return vcol.stream().map(pv -> pv.getDecodedValue());
        }
        @Override
        public  Stream<T> parallelStream() {
          return vcol.parallelStream().map(pv -> pv.getDecodedValue());
        }
        @Override
        public void clear() {
          vcol.clear();
        }
        @Override
        public String toString() {
          return vcol.toString();
        }
      };
    }
    return vcol;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.util.Map#entrySet()
   */
  @Override
  public Set<Map.Entry<K, T>> entrySet() {
    if (eset == null) {
      final Set<Map.Entry<K, PersistedValue<T, PT>>> eset = map.entrySet();

      this.eset = new AbstractSet<Map.Entry<K, T>>() {
        @Override
        public int size() {
          return eset.size();
        }
        @Override
        public boolean isEmpty() {
          return eset.isEmpty();
        }
        @Override
        public Iterator<Map.Entry<K, T>> iterator() {
          return new TransformIterator<Map.Entry<K, PersistedValue<T, PT>>, Map.Entry<K, T>>(eset.iterator()) {
            @Override
            protected Map.Entry<K, T> transform(Map.Entry<K, PersistedValue<T, PT>> me) {
              return new Entry(me);
            }
          };
        }
        @Override
        public Stream<Map.Entry<K, T>> stream() {
          return eset.stream().map(me -> new Entry(me));
        }
        @Override
        public Stream<Map.Entry<K, T>> parallelStream() {
          return eset.parallelStream().map(me -> new Entry(me));
        }
        @Override
        public boolean remove(Object o) {
          if (!(o instanceof Map.Entry)) {
            return false;
          }
          @SuppressWarnings("unchecked")
          final Map.Entry<K, T> me = (Map.Entry<K, T>)o;

          return (map.remove(me.getKey()) != null);
        }
        @Override
        public void clear() {
          eset.clear();
        }
        @Override
        public String toString() {
          return eset.toString();
        }
      };
    }
    return eset;
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
    return map.toString();
  }

  /**
   * The <code>Entry</code> class provides an internal implementation for the
   * <code>Map.Entry</code> interface suitable for persisted entries.
   *
   * @copyright 2015-2015 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 19, 2015 - paouelle - Creation
   *
   * @since 2.0
   */
  @SuppressWarnings("javadoc")
  private class Entry implements Map.Entry<K, T> {
    private final Map.Entry<K, PersistedValue<T, PT>> me;

    Entry(final Map.Entry<K, PersistedValue<T, PT>> me) {
      this.me = me;
    }
    @Override
    public K getKey() {
      return me.getKey();
    }
    @Override
    public T getValue() {
      return me.getValue().getDecodedValue();
    }
    @Override
    public T setValue(T value) {
      return me.setValue(
        new PersistedValue<>(persisted, persister, fname).setDecodedValue(value)
      ).getDecodedValue();
    }
  }
}

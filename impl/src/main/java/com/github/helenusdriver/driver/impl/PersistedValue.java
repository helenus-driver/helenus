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

import java.io.IOException;

import java.util.Objects;

import com.github.helenusdriver.persistence.Persisted;
import com.github.helenusdriver.persistence.Persister;

/**
 * The <code>PersistedValue</code> class is used to track a persisted value by
 * keeping reference to both its encoded and decoded value.
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
public class PersistedValue<T, PT> implements PersistedObject<T, PT> {
  /**
   * Holds the persisted annotation for the corresponding field.
   *
   * @author paouelle
   */
  private final Persisted persisted;

  /**
   * Holds the persister to use when encoding/decoding the value.
   *
   * @author paouelle
   */
  private final Persister<T, PT> persister;

  /**
   * Holds the corresponding field name.
   *
   * @author paouelle
   */
  private final String fname;

  /**
   * Holds the decoded value.
   *
   * @author paouelle
   */
  private T decodedVal;

  /**
   * Holds a dirty flag indicating if the decoded value needs to be recomputed.
   *
   * @author paouelle
   */
  private boolean decodedDirty;

  /**
   * Holds the encoded value.
   *
   * @author paouelle
   */
  private PT encodedVal;

  /**
   * Holds a dirty flag indicating if the encoded value needs to be recomputed.
   *
   * @author paouelle
   */
  private boolean encodedDirty;

  /**
   * Instantiates a new <code>PersistedValue</code> object.
   *
   * @author paouelle
   *
   * @param persisted the non-<code>null</code> persisted annotation
   * @param persister the non-<code>null</code> persister
   * @param fname the non-<code>null</code> field name
   */
  PersistedValue(
    Persisted persisted, Persister<T, PT> persister, String fname
  ) {
    this.persisted = persisted;
    this.persister = persister;
    this.fname = fname;
    this.encodedVal = null;
    this.encodedDirty = false;
    this.decodedVal = null;
    this.decodedDirty = false;
  }

  /**
   * Gets the decoded value.
   *
   * @author paouelle
   *
   * @return the decoded value
   * @throws IllegalArgumentException if unable to decode the value properly
   */
  public T getDecodedValue() {
    if (decodedDirty) {
      try {
        this.decodedVal = persister.decode(encodedVal);
        this.decodedDirty = false;
      } catch (IOException e) {
        throw new IllegalArgumentException(
          "failed to decode field '"
          + fname
          + "' to "
          + persister.getDecodedClass().getName()
          + "' with persister: "
          + persister.getClass().getName(),
          e
        );
      }
    }
    return decodedVal;
  }

  /**
   * Gets the encoded value.
   *
   * @author paouelle
   *
   * @return the encoded value
   * @throws IllegalArgumentException if unable to encode the value properly
   * @throws ClassCastException if the value cannot be encoded to the expected type
   */
  public PT getEncodedValue() {
    if (encodedDirty) {
      try {
        this.encodedVal = persister.getPersistedClass().cast(
          persisted.as().CLASS.cast(persister.encode(decodedVal))
        );
        this.encodedDirty = false;
      } catch (IOException e) {
        throw new IllegalArgumentException(
          "failed to encode field '"
          + fname
          + "' to "
          + persisted.as().CQL
          + "' with persister: "
          + persister.getClass().getName(),
          e
        );
      }
    }
    return encodedVal;
  }

  /**
   * Sets the decoded value.
   *
   * @author paouelle
   *
   * @param  val the new decoded value
   * @return this for chaining
   */
  public PersistedValue<T, PT> setDecodedValue(T val) {
    this.decodedVal = val;
    this.decodedDirty = false;
    this.encodedVal = null;
    this.encodedDirty = true;
    return this;
  }

  /**
   * Sets the encoded value.
   *
   * @author paouelle
   *
   * @param  val the new encoded value
   * @return this for chaining
   */
  public PersistedValue<T, PT> setEncodedValue(PT val) {
    this.encodedVal = val;
    this.encodedDirty = false;
    this.decodedVal = null;
    this.decodedDirty = true;
    return this;
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
    // we hash based on encoded value
    return getEncodedValue().hashCode();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj instanceof PersistedValue) {
      @SuppressWarnings("unchecked")
      final PersistedValue<T, PT> pval = (PersistedValue<T,PT>)obj;

      // avoid encoding/decoding if we do not have to
      if (!encodedDirty && !pval.encodedDirty) {
        return Objects.equals(pval.encodedVal, encodedVal);
      }
      if (!decodedDirty && !pval.decodedDirty) {
        return Objects.equals(pval.decodedVal, decodedVal);
      }
      // give priority to encoded value
      if (!encodedDirty) {
        return Objects.equals(pval.getEncodedValue(), encodedVal);
      }
      if (!pval.encodedDirty) {
        return Objects.equals(pval.encodedVal, getEncodedValue());
      }
      if (!decodedDirty) {
        return Objects.equals(pval.getDecodedValue(), decodedVal);
      }
      if (!pval.decodedDirty) {
        return Objects.equals(pval.decodedVal, getDecodedValue());
      }
      // well, go with encoded values as last resort
      return Objects.equals(pval.getEncodedValue(), getEncodedValue());
    } else if (persister.getPersistedClass().isInstance(obj)) {
      final PT ptval = persister.getPersistedClass().cast(obj);

      return Objects.equals(ptval, getEncodedValue());
    } else if (persister.getDecodedClass().isInstance(obj)) {
      final T tval = persister.getDecodedClass().cast(obj);

      return Objects.equals(tval, getDecodedValue());
    }
    return false;
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
    return (
      (decodedDirty ? "?" : String.valueOf(decodedVal))
      + "≈"
      + (encodedDirty ? "?" : String.valueOf(encodedVal))
    );
  }
}

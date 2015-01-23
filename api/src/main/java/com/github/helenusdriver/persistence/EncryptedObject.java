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
package com.github.helenusdriver.persistence;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import com.github.helenusdriver.commons.lang3.SerializationUtils;

/**
 * The <code>EncryptedObject</code> class defines a wrapper around an object
 * which provides encryption support when the object is serialized.
 * <p>
 * <i>Note:</i> Decryption is lazily performed on-demand when the contained
 * object is accessed for the first time.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 15, 2015 - paouelle - Creation
 *
 * @param <T> the type of the object wrapped by this encrypted object.
 *
 * @since 1.0
 */
public class EncryptedObject<T extends Serializable> implements Serializable {
  /**
   * Holds the serialVersionUID.
   *
   * @author paouelle
   */
  private static final long serialVersionUID = -6109621505397950062L;

  /**
   * Holds the encrypted object.
   *
   * @author paouelle
   */
  private byte[] blob;

  /**
   * Holds the decrypted object.
   *
   * @author paouelle
   */
  private transient T object;

  /**
   * Flag indicating if the object has been encrypted already..
   *
   * @author paouelle
   */
  private transient boolean encrypted;

  /**
   * Flag indicating if the object has been decrypted and cached into
   * {@link #object}.
   *
   * @author paouelle
   */
  private transient boolean decrypted;

  /**
   * Instantiates a new <code>EncryptedObject</code> object.
   *
   * @author paouelle
   */
  public EncryptedObject() {
    this.object = null;
    this.encrypted = false;
    this.decrypted = true;
  }

  /**
   * Instantiates a new <code>EncryptedObject</code> object.
   *
   * @author paouelle
   *
   * @param object the object to encrypt
   */
  public EncryptedObject(T object) {
    this.object = object;
    this.encrypted = false;
    this.decrypted = true;
  }

  /**
   * Deserializes this object from the given stream.
   *
   * @author paouelle
   *
   * @param  in the input stream from which to deserialize this object
   * @throws IOException if an error occured during deserialization
   * @throws ClassNotFoundException if a class cannot be loaded
   */
  private synchronized void readObject(ObjectInputStream in)
    throws IOException, ClassNotFoundException {
    this.blob = (byte[])in.readObject();
    // update transient fields
    this.encrypted = true;
    this.decrypted = false;
    this.object = null;
  }

  /**
   * Serializes this object into the given stream.
   *
   * @author paouelle
   *
   * @param  out the output stream where to serialize this object
   * @throws IOException if an error occurred during serialization
   */
  private synchronized void writeObject(ObjectOutputStream out) throws IOException {
    if (!encrypted) {
      // TODO: encrypt the object
      this.blob = (object != null) ? SerializationUtils.serializeAndCompress(object) : null;
      this.encrypted = true;
    }
    out.writeObject(blob);
  }

  /**
   * Sets the object to encrypt.
   *
   * @author paouelle
   *
   * @param object the object to be encrypted
   */
  public synchronized void setObject(T object) {
    this.object = object;
    this.encrypted = false;
    this.decrypted = true;
  }

  /**
   * Gets the referenced object (decrypting it if required)
   *
   * @author paouelle
   *
   * @return the decrypted object
   */
  @SuppressWarnings("unchecked")
  public synchronized T getObject() {
    if (decrypted) {
      return object;
    }
    if (!encrypted) {
      return null;
    }
    // go ahead and decrypt it
    // TODO: decrypt the object
    this.object = (blob != null) ? (T)SerializationUtils.decompressAndDeserialize(blob) : null;
    this.decrypted = true;
    return object;
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
    return "<encrypted>";
  }
}

/*
 * Copyright (C) 2015-2016 The Helenus Driver Project Authors.
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
package org.helenus.driver;

/**
 * The <code>ExcludedKeyspaceKeyException</code> class extends on the
 * {@link IllegalArgumentException} for cases where a keyspace cannot be obtained
 * as the context contains a keyspace key value that was excluded for a specific
 * keyspace key.
 *
 * @copyright 2015-2016 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Apr 7, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
public class ExcludedKeyspaceKeyException extends IllegalArgumentException {
  /**
   * Holds the serialVersionUID.
   *
   * @author paouelle
   */
  private static final long serialVersionUID = -8462250603572084873L;

  /**
   * Instantiates a new <code>ExcludedKeyspaceKeyException</code> object.
   *
   * @author paouelle
   */
  public ExcludedKeyspaceKeyException() {}

  /**
   * Instantiates a new <code>ExcludedKeyspaceKeyException</code> object.
   *
   * @author paouelle
   *
   * @param msg the exception message
   */
  public ExcludedKeyspaceKeyException(String msg) {
    super(msg);
  }

  /**
   * Instantiates a new <code>ExcludedKeyspaceKeyException</code> object.
   *
   * @author paouelle
   *
   * @param cause the exception cause
   */
  public ExcludedKeyspaceKeyException(Throwable cause) {
    super(cause);
  }

  /**
   * Instantiates a new <code>ExcludedKeyspaceKeyException</code> object.
   *
   * @author paouelle
   *
   * @param message the exception message
   * @param cause the exception cause
   */
  public ExcludedKeyspaceKeyException(String message, Throwable cause) {
    super(message, cause);
  }
}

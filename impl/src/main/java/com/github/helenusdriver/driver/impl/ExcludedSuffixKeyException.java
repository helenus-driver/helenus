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

/**
 * The <code>ExcludedSuffixKeyException</code> class extends on the
 * {@link IllegalArgumentException} for cases where a keyspace cannot be obtained
 * as the context contains a suffix value that was excluded for a specific suffix
 * key.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Apr 7, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
public class ExcludedSuffixKeyException extends IllegalArgumentException {
  /**
   * Holds the serialVersionUID.
   *
   * @author paouelle
   */
  private static final long serialVersionUID = -8462250603572084873L;

  /**
   * Instantiates a new <code>ExcludedSuffixKeyException</code> object.
   *
   * @author paouelle
   */
  public ExcludedSuffixKeyException() {}

  /**
   * Instantiates a new <code>ExcludedSuffixKeyException</code> object.
   *
   * @author paouelle
   *
   * @param msg the exception message
   */
  public ExcludedSuffixKeyException(String msg) {
    super(msg);
  }

  /**
   * Instantiates a new <code>ExcludedSuffixKeyException</code> object.
   *
   * @author paouelle
   *
   * @param cause the exception cause
   */
  public ExcludedSuffixKeyException(Throwable cause) {
    super(cause);
  }

  /**
   * Instantiates a new <code>ExcludedSuffixKeyException</code> object.
   *
   * @author paouelle
   *
   * @param message the exception message
   * @param cause the exception cause
   */
  public ExcludedSuffixKeyException(String message, Throwable cause) {
    super(message, cause);
  }
}

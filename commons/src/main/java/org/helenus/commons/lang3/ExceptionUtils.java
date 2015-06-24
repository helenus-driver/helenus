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
package org.helenus.commons.lang3;

/**
 * The <code>ExceptionUtils</code> class extends the
 * {@link org.apache.commons.lang3.exception.ExceptionUtils}.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 15, 2015 - paouelle - Creation
 *
 * @since 2.0
 */
public class ExceptionUtils extends org.apache.commons.lang3.exception.ExceptionUtils {

  /**
   * Instantiates a new <code>ExceptionUtils</code> object.
   *
   * @author paouelle
   */
  public ExceptionUtils() {}

  /**
   * Initializes the cause for this exception.
   *
   * @author paouelle
   *
   * @param  e the exception to initialize its cause
   * @param  cause the cause to initialize the exception with
   * @return e the exception for chaining
   */
  public static <E extends Throwable> E withCause(E e, Throwable cause) {
    if (cause != null) {
      e.initCause(cause);
    }
    return e;
  }

  /**
   * Initializes the root cause for this exception.
   * <p>
   * This method will attach the specified cause to the end of the exception
   * set of causes.
   *
   * @author paouelle
   *
   * @param  e the exception to initialize its route cause
   * @param  cause the cause to initialize the exception with
   * @return e the exception for chaining
   */
  public static <E extends Throwable> E withRootCause(E e, Throwable cause) {
    Throwable t = org.apache.commons.lang3.exception.ExceptionUtils.getRootCause(e);

    if (t == null) {
      t = e;
    }
    t.initCause(cause);
    return e;
  }
}

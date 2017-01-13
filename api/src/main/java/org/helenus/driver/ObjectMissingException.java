/*
 * Copyright (C) 2015-2017 The Helenus Driver Project Authors.
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

import com.datastax.driver.core.Row;
import com.datastax.driver.core.UDTValue;

/**
 * The <code>ObjectMissingException</code> exception is thrown when unable
 * to convert a result row into a POJO object because the value is missing.
 *
 * @copyright 2015-2017 The Helenus Driver Project Authors
 *
 * @author The Helenus Driver Project Authors
 * @version 1 - Jan 10, 2017 - paouelle - Creation
 *
 * @since 3.0
 */
public class ObjectMissingException extends ObjectConversionException {
  /**
   * Holds the serial version UID for this class.
   *
   * @author paouelle
   */
  private static final long serialVersionUID = 424127400021190829L;

  /**
   * Instantiates a new <code>ObjectMissingException</code> object.
   *
   * @author paouelle
   *
   * @param clazz the POJO class we are trying to convert to
   * @param row the result row we are converting from
   * @param message the error message
   */
  public ObjectMissingException(Class<?> clazz, Row row, String message) {
    super(clazz, row, message);
  }

  /**
   * Instantiates a new <code>ObjectMissingException</code> object.
   *
   * @author paouelle
   *
   * @param clazz the POJO class we are trying to convert to
   * @param uval the UDT value we are converting from
   * @param message the error message
   */
  public ObjectMissingException(
    Class<?> clazz, UDTValue uval, String message
  ) {
    super(clazz, uval, message);
  }

  /**
   * Instantiates a new <code>ObjectMissingException</code> object.
   *
   * @author paouelle
   *
   * @param clazz the POJO class we are trying to convert to
   * @param row the result row we are converting from
   * @param message the error message
   * @param cause the exception that caused this error
   */
  public ObjectMissingException(
    Class<?> clazz, Row row, String message, Throwable cause
  ) {
    super(clazz, row, message, cause);
  }

  /**
   * Instantiates a new <code>ObjectMissingException</code> object.
   *
   * @author paouelle
   *
   * @param clazz the POJO class we are trying to convert to
   * @param uval the UDT value we are converting from
   * @param message the error message
   * @param cause the exception that caused this error
   */
  public ObjectMissingException(
    Class<?> clazz, UDTValue uval, String message, Throwable cause
  ) {
    super(clazz, uval, message, cause);
  }
}

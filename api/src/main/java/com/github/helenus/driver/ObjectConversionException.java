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
package com.github.helenus.driver;

import com.datastax.driver.core.Row;

/**
 * The <code>ObjectConversionException</code> exception is thrown when unable
 * to convert a result row into a POJO object.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author The Helenus Driver Project Authors
 * @version 1 - Jan 15, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
public class ObjectConversionException extends RuntimeException {
  /**
   * Holds the serial version UID for this class.
   *
   * @author paouelle
   */
  private static final long serialVersionUID = 4241658868346390825L;

  /**
   * Holds the class of the POJO we are trying to convert to.
   *
   * @author paouelle
   */
  private final Class<?> clazz;

  /**
   * Holds the Cassandra result row we were trying to convert from.
   *
   * @author paouelle
   */
  private final Row row;

  /**
   * Instantiates a new <code>ObjectConversionException</code> object.
   *
   * @author paouelle
   *
   * @param clazz the POJO class we are trying to convert to
   * @param row the result row we are converting from
   * @param message the error message
   */
  public ObjectConversionException(Class<?> clazz, Row row, String message) {
    this(clazz, row, message, null);
  }

  /**
   * Instantiates a new <code>ObjectConversionException</code> object.
   *
   * @author paouelle
   *
   * @param clazz the POJO class we are trying to convert to
   * @param row the result row we are converting from
   * @param message the error message
   * @param cause the exception that caused this error
   */
  public ObjectConversionException(
    Class<?> clazz, Row row, String message, Throwable cause
  ) {
    super(message, cause);
    this.clazz = clazz;
    this.row = row;
  }

  /**
   * Gets the POJO class we were trying to convert to.
   *
   * @author paouelle
   *
   * @return the POJO class we were trying to convert to
   */
  public Class<?> getObjectClass() {
    return clazz;
  }

  /**
   * Gets the Cassandra result row we were trying to convert from.
   *
   * @author paouelle
   *
   * @return the result row we were trying to convert from
   */
  public Row getRow() {
    return row;
  }
}

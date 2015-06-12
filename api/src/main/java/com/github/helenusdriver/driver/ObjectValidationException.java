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
package com.github.helenusdriver.driver;


/**
 * The <code>ObjectConversionException</code> exception can be thrown as a
 * result of validating a POJO object before inserting or updating it.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author The Helenus Driver Project Authors
 * @version 1 - Jun 12, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
public class ObjectValidationException extends RuntimeException {
  /**
   * Holds the serial version UID for this class.
   *
   * @author paouelle
   */
  private static final long serialVersionUID = -5293857700015274358L;

  /**
   * Holds the POJO that failed to be validated.
   *
   * @author paouelle
   */
  private final Object obj;

  /**
   * Holds the name of the column that failed validation.
   *
   * @author paouelle
   */
  private final String name;

  /**
   * Holds the value of the column that failed validation.
   *
   * @author <a href="mailto:paouelle@enlightedinc.com">paouelle</a>
   */
  private final Object val;

  /**
   * Instantiates a new <code>ObjectValidationException</code> object.
   *
   * @author paouelle
   *
   * @param obj the POJO that failed validation
   * @param name the name of the column that failed validation
   * @param val the value of the column that failed validation
   * @param message the error message
   */
  public ObjectValidationException(
    Object obj, String name, Object val, String message
  ) {
    this(obj, name, val, message, null);
  }

  /**
   * Instantiates a new <code>ObjectValidationException</code> object.
   *
   * @author paouelle
   *
   * @param obj the POJO that failed validation
   * @param name the name of the column that failed validation
   * @param val the value of the column that failed validation
   * @param message the error message
   * @param cause the exception that caused this error
   */
  public ObjectValidationException(
    Object obj, String name, Object val, String message, Throwable cause
  ) {
    super(message, cause);
    this.obj = obj;
    this.name = name;
    this.val = val;
  }

  /**
   * Gets the POJO object that failed validation.
   *
   * @author paouelle
   *
   * @return the POJO object that failed validation
   */
  public Object getObject() {
    return obj;
  }

  /**
   * Gets the name of the column that failed validation.
   *
   * @author paouelle
   *
   * @return the name of the column that failed validation
   */
  public String getColumnName() {
    return name;
  }

  /**
   * Gets the value of the column that failed validation.
   *
   * @author paouelle
   *
   * @return the value of the column that failed validation
   */
  public Object getColumnValue() {
    return val;
  }
}

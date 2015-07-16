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
package org.helenus.driver;

import java.util.LinkedHashMap;
import java.util.Map;

import lombok.NonNull;

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
public class ObjectValidationException extends IllegalArgumentException {
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
  private Object obj;

  /**
   * Holds the name of the column that failed validation.
   *
   * @author paouelle
   */
  private String name;

  /**
   * Holds the value of the column that failed validation.
   *
   * @author paouelle
   */
  private Object val;

  /**
   * Holds additional details about the error.
   *
   * @author paouelle
   */
  private volatile Map<String, Object> details = null;

  /**
   * Instantiates a new <code>ObjectValidationException</code> object.
   *
   * @author paouelle
   *
   * @param e the validation exception to copy (it will become the cause of the
   *        newly created exception)
   */
  public ObjectValidationException(ObjectValidationException e) {
    this(e.obj, e.name, e.val, e.getMessage(), e);
    if (e.details != null) {
      this.details = new LinkedHashMap<>(e.details);
    }
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
   * Sets the POJO object that failed validation.
   *
   * @author paouelle
   *
   * @param obj the POJO object that failed validation
   */
  public void setObject(Object obj) {
    this.obj = obj;
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
   * Sets the name of the column that failed validation.
   *
   * @author paouelle
   *
   * @param name the name of the column that failed validation
   */
  public void setColumnName(String name) {
    this.name = name;
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
   * Sets the value of the column that failed validation.
   *
   * @author paouelle
   *
   * @param val the value of the column that failed validation
   */
  public void setColumnValue(Object val) {
    this.val = val;
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

  /**
   * Gets the additional details about the error.
   *
   * @author paouelle
   *
   * @return map of additional details about the error or <code>null</code> if
   *         none added
   */
  public Map<String, Object> getDetails() {
    return details;
  }

  /**
   * Adds the specified detail about this error.
   *
   * @author paouelle
   *
   * @param  key the key for the detail
   * @param  value the value for the detail
   * @return this for chaining
   * @throws NullPointerException if <code>key</code> is <code>null</code>
   */
  public synchronized ObjectValidationException addDetail(
    @NonNull String key, Object value
  ) {
    if (details == null) {
      this.details = new LinkedHashMap<>(16);
    }
    details.put(key, value);
    return this;
  }
}

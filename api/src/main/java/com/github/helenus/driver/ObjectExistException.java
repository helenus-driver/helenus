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
import com.datastax.driver.core.exceptions.QueryExecutionException;

/**
 * The <code>ObjectExistException</code> exception when an "IF NOT EXISTS"
 * condition is specified on an "INSERT" request and the insert failed to be
 * applied because the object already existed.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 15, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
public class ObjectExistException extends QueryExecutionException {
  /**
   * Holds the serialVersionUID.
   *
   * @author paouelle
   */
  private static final long serialVersionUID = 1731682518787514542L;

  /**
   * Holds the row holding information about the failed insert.
   *
   * @author paouelle
   */
  private final Row row;

  /**
   * Instantiates a new <code>ObjectExistException</code> object.
   *
   * @author paouelle
   *
   * @param msg the error message
   */
  public ObjectExistException(String msg) {
    this(null, msg);
  }

  /**
   * Instantiates a new <code>ObjectExistException</code> object.
   *
   * @author paouelle
   *
   * @param row the result row
   * @param msg the error message
   */
  public ObjectExistException(Row row, String msg) {
    super(row != null ? msg + "; " + row : msg);
    this.row = row;
  }

  /**
   * Instantiates a new <code>ObjectExistException</code> object.
   *
   * @author paouelle
   *
   * @param msg the error message
   * @param cause the cause exception
   */
  public ObjectExistException(String msg, Throwable cause) {
    this(null, msg, cause);
  }

  /**
   * Instantiates a new <code>ObjectExistException</code> object.
   *
   * @author paouelle
   *
   * @param row the result row
   * @param msg the error message
   * @param cause the cause exception
   */
  public ObjectExistException(Row row, String msg, Throwable cause) {
    super(row != null ? msg + "; " + row : msg, cause);
    this.row = row;
  }

  /**
   * Gets the result row holding information about the failed insert.
   *
   * @author paouelle
   *
   * @return the result row or <code>null</code> if no rows were returned as
   *         part of the result set for the "INSERT" statement
   */
  public Row getRow() {
    return row;
  }
}

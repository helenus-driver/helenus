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


/**
 * The <code>ColumnPersistenceException</code> exception is thrown when unable
 * to persist a column.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author paouelle
 * @version 1 - Jan 15, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
public class ColumnPersistenceException extends RuntimeException {
  /**
   * Holds the serialVersionUID.
   *
   * @author paouelle
   */
  private static final long serialVersionUID = -3170316653313645401L;

  /**
   * Holds the class of the POJO we are trying to persist from.
   *
   * @author paouelle
   */
  private final Class<?> clazz;

  /**
   * Holds the field name we are trying to persist.
   *
   * @author paouelle
   */
  private final String name;

  /**
   * Instantiates a new <code>ColumnPersistenceException</code> object.
   *
   * @author paouelle
   *
   * @param clazz the POJO class we are trying to persist from
   * @param name the field we are trying to persist
   * @param message the error message
   */
  public ColumnPersistenceException(Class<?> clazz, String name, String message) {
    this(clazz, name, message, null);
  }

  /**
   * Instantiates a new <code>ColumnPersistenceException</code> object.
   *
   * @author paouelle
   *
   * @param clazz the POJO class we are trying to persist from
   * @param name the field we are trying to persist
   * @param message the error message
   * @param cause the exception that caused this error
   */
  public ColumnPersistenceException(
    Class<?> clazz, String name, String message, Throwable cause
  ) {
    super(message, cause);
    this.clazz = clazz;
    this.name = name;
  }

  /**
   * Gets the POJO class we were trying to persist from.
   *
   * @author paouelle
   *
   * @return the POJO class we were trying to persist from
   */
  public Class<?> getObjectClass() {
    return clazz;
  }

  /**
   * Gets the name of the column we are trying to persist.
   *
   * @author paouelle
   *
   * @return the name of the column we are trying to persist
   */
  public String getName() {
    return name;
  }
}

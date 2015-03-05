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

import org.apache.commons.lang3.StringUtils;

import com.datastax.driver.core.exceptions.InvalidQueryException;

/**
 * The <code>ObjectNotFoundException</code> can be thrown when no pojos were
 * found based on the query criteria or again if the keyspace or table was not
 * found.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author The Helenus Driver Project Authors
 * @version 1 - Feb 27, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
public class ObjectNotFoundException extends RuntimeException {
  /**
   * Holds the serialVersionUID.
   *
   * @author paouelle
   */
  private static final long serialVersionUID = -4416855253723036106L;

  /**
   * Handles special case where a keyspace is not defined by throwing a
   * {@link ObjectNotFoundException} if the specified exception matches that
   * specific condition.
   *
   * @author paouelle
   *
   * @param  clazz the pojo class we queried
   * @param  e the invalid query exception to handle
   * @throws ObjectNotFoundException if the keyspace is not found
   */
  public static void handleKeyspaceNotFound(
    Class<?> clazz, InvalidQueryException e
  ) throws ObjectNotFoundException {
    final String msg = e.getMessage();

    if (StringUtils.startsWithIgnoreCase(msg, "keyspace ")
        && StringUtils.endsWith(msg, "does not exist")) {
      throw new ObjectNotFoundException(clazz, msg);
    }
  }

  /**
   * Holds the class of the pojo we queried.
   *
   * @author paouelle
   */
  private final Class<?> clazz;

  /**
   * Instantiates a new <code>ObjectNotFoundException</code> object.
   *
   * @author paouelle
   *
   * @param clazz the pojo class we queried
   */
  public ObjectNotFoundException(Class<?> clazz) {
    this(clazz, null, null);
  }

  /**
   * Instantiates a new <code>ObjectNotFoundException</code> object.
   *
   * @author paouelle
   *
   * @param clazz the pojo class we queried
   * @param msg the error message
   */
  public ObjectNotFoundException(Class<?> clazz, String msg) {
    this(clazz, msg, null);
  }

  /**
   * Instantiates a new <code>ObjectNotFoundException</code> object.
   *
   * @author paouelle
   *
   * @param clazz the pojo class we queried
   * @param msg the error message
   * @param cause the exception that caused this error
   */
  public ObjectNotFoundException(Class<?> clazz, String msg, Throwable cause) {
    super(msg, cause);
    this.clazz = clazz;
  }

  /**
   * Gets the pojo class we queried.
   *
   * @author paouelle
   *
   * @return the pojo class we queried
   */
  public Class<?> getObjectClass() {
    return clazz;
  }
}

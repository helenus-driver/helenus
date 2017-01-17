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

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.datastax.driver.core.exceptions.InvalidQueryException;

/**
 * The <code>TooManyMatchesFoundException</code> can be thrown when too many
 * pojos were found based on the query criteria.
 *
 * @copyright 2015-2017 The Helenus Driver Project Authors
 *
 * @author The Helenus Driver Project Authors
 * @version 1 - Jan 17, 2017 - paouelle - Creation
 *
 * @since 1.0
 */
public class TooManyMatchesFoundException extends RuntimeException {
  /**
   * Holds the serialVersionUID.
   *
   * @author paouelle
   */
  private static final long serialVersionUID = -4416810932023036106L;

  /**
   * Holds additional details about the error.
   *
   * @author paouelle
   */
  private volatile Map<String, Object> details = null;

  /**
   * Handles special case where a keyspace is not defined by throwing a
   * {@link TooManyMatchesFoundException} if the specified exception matches that
   * specific condition.
   *
   * @author paouelle
   *
   * @param  clazz the pojo class we queried
   * @param  e the invalid query exception to handle
   * @throws TooManyMatchesFoundException if the keyspace is not found
   */
  public static void handleKeyspaceNotFound(
    Class<?> clazz, InvalidQueryException e
  ) throws TooManyMatchesFoundException {
    final String msg = e.getMessage();

    if (StringUtils.startsWithIgnoreCase(msg, "keyspace ")
        && StringUtils.endsWith(msg, "does not exist")) {
      throw new TooManyMatchesFoundException(clazz, msg);
    }
  }

  /**
   * Holds the class of the pojo we queried.
   *
   * @author paouelle
   */
  private final Class<?> clazz;

  /**
   * Instantiates a new <code>TooManyMatchesFoundException</code> object.
   *
   * @author paouelle
   *
   * @param clazz the pojo class we queried
   * @param msg the error message
   */
  public TooManyMatchesFoundException(Class<?> clazz, String msg) {
    this(clazz, msg, null);
  }

  /**
   * Instantiates a new <code>TooManyMatchesFoundException</code> object.
   *
   * @author paouelle
   *
   * @param clazz the pojo class we queried
   * @param msg the error message
   * @param cause the exception that caused this error
   */
  public TooManyMatchesFoundException(Class<?> clazz, String msg, Throwable cause) {
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
  public synchronized TooManyMatchesFoundException addDetail(
    String key, Object value
  ) {
    org.apache.commons.lang3.Validate.notNull(key, "invalid null key");
    if (details == null) {
      this.details = new LinkedHashMap<>(16);
    }
    details.put(key, value);
    return this;
  }
}

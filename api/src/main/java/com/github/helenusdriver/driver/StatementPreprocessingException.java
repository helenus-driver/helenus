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

import com.datastax.driver.core.exceptions.DriverException;

/**
 * The <code>StatementPreprocessingException</code> exception is thrown when a
 * statement is first preprocessed into a valid Cassandra's query before being
 * sent out for execution.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 15, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
public class StatementPreprocessingException extends DriverException {
  /**
   * Holds the serialVersionUID.
   *
   * @author paouelle
   */
  private static final long serialVersionUID = -6538963087326581071L;

  /**
   * Instantiates a new <code>StatementPreprocessingException</code> object.
   *
   * @author paouelle
   *
   * @param message the error message
   */
  public StatementPreprocessingException(String message) {
    super(message);
  }

  /**
   * Instantiates a new <code>StatementPreprocessingException</code> object.
   *
   * @author paouelle
   *
   * @param cause the cause of this exception
   */
  public StatementPreprocessingException(Throwable cause) {
    super(cause);
  }

  /**
   * Instantiates a new <code>StatementPreprocessingException</code> object.
   *
   * @author paouelle
   *
   * @param message the error message
   * @param cause the cause of this exception
   */
  public StatementPreprocessingException(String message, Throwable cause) {
    super(message, cause);
  }
}

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

import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

/**
 * The <code>IllegalCycleException</code> class defines an exception that can
 * be thrown when a cycle is detected in a directed graph.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 15, 2015 - paouelle - Creation
 *
 * @since 2.0
 */
public class IllegalCycleException extends RuntimeException {
  /**
   * Holds the serialVersionUID.
   *
   * @author paouelle
   */
  private static final long serialVersionUID = 8918495252713037231L;

  /**
   * Holds the cycle that was detected.
   *
   * @author paouelle
   */
  private final List<?> cycle;

  /**
   * Instantiates a new <code>IllegalCycleException</code> object.
   *
   * @author paouelle
   *
   * @param cycle the cycle that was detected
   */
  public IllegalCycleException(List<?> cycle) {
    this.cycle = cycle;
  }

  /**
   * Instantiates a new <code>IllegalCycleException</code> object.
   *
   * @author paouelle
   *
   * @param s the error message
   * @param cycle the cycle that was detected
   */
  public IllegalCycleException(String s, List<?> cycle) {
    super(s);
    this.cycle = cycle;
  }

  /**
   * Instantiates a new <code>IllegalCycleException</code> object.
   *
   * @author paouelle
   *
   * @param cause the cause for this exception
   * @param cycle the cycle that was detected
   */
  public IllegalCycleException(Throwable cause, List<?> cycle) {
    super(cause);
    this.cycle = cycle;
  }

  /**
   * Instantiates a new <code>IllegalCycleException</code> object.
   *
   * @author paouelle
   *
   * @param message the error message
   * @param cause the cause for this exception
   * @param cycle the cycle that was detected
   */
  public IllegalCycleException(String message, Throwable cause, List<?> cycle) {
    super(message, cause);
    this.cycle = cycle;
  }

  /**
   * Gets the cycle that was detected.
   *
   * @author paouelle
   *
   * @return the cycle that was detected
   */
  public List<?> getCycle() {
    return Collections.unmodifiableList(cycle);
  }

  /**
   * Gets the cycle that was detected.
   *
   * @author paouelle
   *
   * @return a stream for the cycle that was detected
   */
  public Stream<?> cycle() {
    return cycle.stream();
  }
}

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
package org.helenus.util.function;

/**
 * The <code>ERunnable</code> interface expands on the {@link Runnable}
 * interface to provide the ability to throw back exceptions.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 15, 2015 - paouelle - Creation
 *
 * @param <E> the type of exceptions that can be thrown by the command
 *
 * @since 2.0
 */
@FunctionalInterface
public interface ERunnable<E extends Throwable> {
  /**
   * Executes user-defined code.
   *
   * @author paouelle
   *
   * @throws E if an error occurs
   */
  public void run() throws E;
}

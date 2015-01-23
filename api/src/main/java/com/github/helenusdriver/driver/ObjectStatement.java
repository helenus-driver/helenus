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
 * The <code>ObjectStatement</code> interface extends the functionality of
 * the {@link GenericStatement} interface for statements that are associated with a
 * POJO and returns POJO objects.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 15, 2015 - paouelle - Creation
 *
 * @param <T> The type of POJO associated with this statement
 *
 * @since 1.0
 */
public interface ObjectStatement<T>
  extends GenericStatement<ObjectSet<T>, ObjectSetFuture<T>> {
  /**
   * Gets the POJO class associated with this statement.
   *
   * @author paouelle
   *
   * @return the POJO class associated with this statement
   */
  public Class<T> getObjectClass();
}

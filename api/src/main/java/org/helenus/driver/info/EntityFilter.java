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
package org.helenus.driver.info;

import org.helenus.driver.StatementManager;

/**
 * The <code>EntityFilter</code> interface provides a way for to extend the driver
 * such that it filters out columns from an entity's table at the time it is
 * introspected. Filters are typically registered directly with the
 * implementation of the {@link StatementManager}.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 15, 2015 - paouelle - Creation
 *
 * @since 2.0
 */
public interface EntityFilter {
  /**
   * Called to filter out the provided entity's table. It is possible from this
   * call to remove columns from the entity's table. Such columns will be
   * completely ignored from persistence.
   *
   * @author paouelle
   *
   * @param <T> the type of POJO class
   *
   * @param tinfo the non-<code>null</code> table info for the POJO class to be
   *        filtered
   */
  public <T> void filter(TableInfo<T> tinfo);
}

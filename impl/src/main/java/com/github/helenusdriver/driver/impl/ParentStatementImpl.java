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
package com.github.helenusdriver.driver.impl;

import java.util.stream.Stream;

import com.github.helenusdriver.driver.ObjectStatement;

/**
 * The <code>ParentStatementImpl</code> interface should be used by statements
 * that can be parent of others.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - May 21, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
public interface ParentStatementImpl {
  /**
   * Sets the parent for this statement.
   *
   * @author paouelle
   *
   * @param parent the parent for this statement
   */
  public void setParent(ParentStatementImpl parent);

  /**
   * Notifies the registered recorder and parent of the specified object statement.
   *
   * @author paouelle
   *
   * @param statement the non-<code>null</code> object statement that was recorded
   */
  public void recorded(ObjectStatement<?> statement);

  /**
   * Gets all object statements contained recursively in this parent statement.
   *
   * @author <a href="mailto:paouelle@enlightedinc.com">paouelle</a>
   *
   * @return a stream of all object statements contained recursively in this parent
   */
  public Stream<ObjectStatement<?>> objectStatements();
}

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
package org.helenus.driver.junit;

import org.helenus.driver.ObjectStatement;

/**
 * The <code>EnumCaptureListMatcher</code> interface defines a class that can
 * match the enum capture list against some expectation.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jun 30, 2015 - paouelle - Creation
 *
 * @param <T> the type of statements captured
 *
 * @since 1.0
 */
public interface StatementCaptureListMatcher<T extends ObjectStatement<?>> {
  /**
   * Called to process this matcher against the specified statement capture list.
   *
   * @author paouelle
   *
   * @param  list the non-<code>null</code> statement capture list
   * @throws Exception if an error occurs while matching
   */
  public void match(StatementCaptureList<T> list) throws Exception;
}

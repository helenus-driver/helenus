/*
 * Copyright (C) 2015-2016 The Helenus Driver Project Authors.
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
package com.datastax.driver.core.querybuilder;

import com.datastax.driver.core.RegularStatement;

/**
 * The <code>QueryBuilderBridge</code> class provides access to package private
 * functionality in Cassandra.
 *
 * @copyright 2015-2016 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 19, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
public class QueryBuilderBridge {
  /**
   * Prevents instantiation.
   *
   * @author paouelle
   */
  private QueryBuilderBridge() {}

  /**
   * Checks if the specified statement holds a counter assignment operation.
   *
   * @author paouelle
   *
   * @param  statement the raw statement to check
   * @return <code>true</code> if the statement holds a counter assignment
   *         operation; <code>false</code> otherwise
   */
  public static boolean isCounterOp(RegularStatement statement) {
    return ((statement instanceof BuiltStatement)
             ? ((BuiltStatement)statement).isCounterOp()
             : false);
  }

  /**
   * Gets the keyspace associated with the statement if any.
   *
   * @author paouelle
   *
   * @param  statement the statement for which to get the keyspace
   * @return the associated statement or <code>null</code> if unknown
   */
  public static String getKeyspace(RegularStatement statement) {
    return ((statement instanceof BuiltStatement)
        ? ((BuiltStatement)statement).getKeyspace()
        : null);
  }
}

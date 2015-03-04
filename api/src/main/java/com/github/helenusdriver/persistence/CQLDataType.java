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
package com.github.helenusdriver.persistence;


/**
 * The <code>CQLDataType</code> interface is used to identify CQL data types.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author The Helenus Driver Project Authors
 * @version 1 - Mar 4, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
public interface CQLDataType {
  /**
   * Gets a name representation for this data type.
   *
   * @author paouelle
   *
   * @return a non-<code>null</code> name representation for this data type
   */
  public String name();

  /**
   * Checks if this data type represents a collection.
   *
   * @author paouelle
   *
   * @return <code>true</code> if this data type represent a collection;
   *         <code>false</code> otherwise
   */
  public boolean isCollection();

  /**
   * Checks if this data type represents a user-defined type.
   *
   * @author paouelle
   *
   * @return <code>true</code> if this data type represents a user-defined
   *         type; <code>false</code> otherwise
   */
  public boolean isUserDefined();

  /**
   * Get a CQL representation of this data type.
   *
   * @author paouelle
   *
   * @return a CQL representation of this data type
   */
  public String toCQL();
}

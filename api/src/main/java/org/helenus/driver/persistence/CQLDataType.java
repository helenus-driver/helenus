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
package org.helenus.driver.persistence;

import java.util.List;


/**
 * The <code>CQLDataType</code> interface is used to identify CQL data types.
 *
 * @copyright 2015-2017 The Helenus Driver Project Authors
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
   * Checks whether this data type is frozen.
   * <p>
   * This applies to user defined types, tuples and nested collections. Frozen
   * types are serialized as a single value in Cassandra's storage engine,
   * whereas non-frozen types are stored in a form that allows updates to
   * individual subfields.
   *
   * @author paouelle
   *
   * @return whether this data type is frozen
   */
  public boolean isFrozen();

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
   * Checks if this data type represents a tuple.
   *
   * @author paouelle
   *
   * @return <code>true</code> if this data type represent a tuple;
   *         <code>false</code> otherwise
   */
  public boolean isTuple();

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
   * Gets the corresponding Cassandra data type.
   *
   * @author paouelle
   *
   * @return the non-<code>null</code> corresponding Cassandra data type
   */
  public com.datastax.driver.core.DataType getDataType();

  /**
   * Gets the main data type for this definition.
   *
   * @author paouelle
   *
   * @return the non-<code>null</code> data type for this definition
   */
  public CQLDataType getMainType();

  /**
   * Gets the data type for element values to the collection data type
   * represented by this definition.
   *
   * @author paouelle
   *
   * @return the data type for the collection's element values or
   *         <code>null</code> if this definition doesn't represent a
   *         collection
   */
  public CQLDataType getElementType();

  /**
   * Gets the data type for all arguments of the collection data type
   * represented by this definition.
   *
   * @author paouelle
   *
   * @return a non-<code>null</code> list of all argument data types for the
   *         collection
   */
  public List<CQLDataType> getArgumentTypes();

  /**
   * Gets the data type for the first argument of the collection data type
   * represented by this definition.
   *
   * @author paouelle
   *
   * @return the first argument data type for the collection or <code>null</code>
   *         if this definition doesn't represent a collection
   */
  public CQLDataType getFirstArgumentType();

  /**
   * Checks if altering a column from this data type to a specified data
   * type is supported.
   * <p>
   * <i>Note:</i> For collections, the data type must remain the same and the
   * arguments must also be alterable.
   *
   * @author paouelle
   *
   * @param  to the data type to change the column to
   * @return <code>true</code> if the conversion is supported; <code>false</code>
   *         otherwise
   */
  public boolean isAlterableTo(CQLDataType to);

  /**
   * Get a CQL representation of this data type.
   *
   * @author paouelle
   *
   * @return a CQL representation of this data type
   */
  public String toCQL();
}

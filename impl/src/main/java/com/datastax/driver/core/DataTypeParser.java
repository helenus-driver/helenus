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
package com.datastax.driver.core;

import java.util.List;
import java.util.stream.Collectors;

import org.helenus.driver.impl.DataTypeImpl.Definition;
import org.helenus.driver.persistence.CQLDataType;
import org.helenus.driver.persistence.Ordering;

/**
 * The <code>DataTypeParser</code> provides access to the
 * {@link DataTypeClassNameParser} package private class functionality.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Apr 1, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
public class DataTypeParser extends DataTypeClassNameParser {
  /**
   * Converts the specified data type to a cql data type.
   *
   * @author paouelle
   *
   * @param  t the data type to convert
   * @return the corresponding CQL data type
   */
  private static CQLDataType toCQL(DataType t) {
    if (t instanceof UserType) {
      final UserType ut = (UserType)t;

      return new CQLDataType() {
        @Override
        public String name() {
          return ut.getTypeName();
        }
        @Override
        public boolean isCollection() {
          return false;
        }
        @Override
        public boolean isUserDefined() {
          return true;
        }
        @Override
        public CQLDataType getMainType() {
          return this;
        }
        @Override
        public CQLDataType getElementType() {
          return null;
        }
        @Override
        public List<CQLDataType> getArgumentTypes() {
          return null;
        }
        @Override
        public CQLDataType getFirstArgumentType() {
          return null;
        }
        @Override
        public boolean isAlterableTo(CQLDataType to) {
          return false; // never alterable
        }
        @Override
        public String toCQL() {
          return "frozen<" + name() + '>';
        }
      };
    } else if (t instanceof TupleType) {
      final TupleType tt = (TupleType)t;

      return new CQLDataType() {
        @Override
        public String name() {
          return "tuple";
        }
        @Override
        public boolean isCollection() {
          return false;
        }
        @Override
        public boolean isUserDefined() {
          return false;
        }
        @Override
        public CQLDataType getMainType() {
          return this;
        }
        @Override
        public CQLDataType getElementType() {
          return null;
        }
        @Override
        public List<CQLDataType> getArgumentTypes() {
          return null;
        }
        @Override
        public CQLDataType getFirstArgumentType() {
          return null;
        }
        @Override
        public boolean isAlterableTo(CQLDataType to) {
          return false; // never alterable - TODO: fix tuples
        }
        @SuppressWarnings("synthetic-access")
        @Override
        public String toCQL() {
          return tt.getComponentTypes().stream()
            .map(ct -> DataTypeParser.toCQL(ct).toCQL())
            .collect(Collectors.joining(",", "frozen<tuple<", ">>"));
        }
      };
    }
    return new Definition(
      org.helenus.driver.persistence.DataType.valueOf(t.getName().name()),
      t.getTypeArguments().stream()
        .map(ta -> DataTypeParser.toCQL(ta))
        .collect(Collectors.toList())
    );
  }

  /**
   * Converts the specified validator string to a CQL data type.
   *
   * @author paouelle
   *
   * @param  validator the validator to be converted
   * @param  protocolVersion the protocol version being used
   * @param  codecRegistry the codec registry to be used
   * @return the corresponding CQL data type
   */
  public static CQLDataType validatorToCQL(
    String validator, ProtocolVersion protocolVersion, CodecRegistry codecRegistry
  ) {
    return DataTypeParser.toCQL(
      DataTypeClassNameParser.parseOne(validator, protocolVersion, codecRegistry)
    );
  }

  /**
   * Converts the specified type string to a CQL data type.
   *
   * @author paouelle
   *
   * @param  type the type to be converted
   * @param  protocolVersion the protocol version being used
   * @param  codecRegistry the codec registry to be used
   * @return the corresponding CQL data type
   */
  public static CQLDataType typeToCQL(
    String type, ProtocolVersion protocolVersion, CodecRegistry codecRegistry
  ) {
    if (type.startsWith("org.apache.cassandra.db.marshal.FrozenType(")) {
      type = type.substring(43, type.length() - 1);
    }
    return DataTypeParser.toCQL(
      DataTypeClassNameParser.parseOne(type, protocolVersion, codecRegistry)
    );
  }

  /**
   * Checks if the specified validator string is reversed.
   *
   * @author paouelle
   *
   * @param  validator the validator to check
   * @return <code>true</code> if the validator is reversed; <code>false</code>
   *         otherwise
   */
  public static boolean isReversed(String validator) {
    return DataTypeClassNameParser.isReversed(validator);
  }

  /**
   * Gets the specified ordering for the specified validator string.
   *
   * @author paouelle
   *
   * @param  validator the validator to check
   * @return the corresponding ordering
   */
  public static Ordering getOrderingFrom(String validator) {
    return (
      DataTypeClassNameParser.isReversed(validator)
      ? Ordering.DESCENDING
      : Ordering.ASCENDING
    );
  }

  /**
   * Checks if the specified validator string is a user type.
   *
   * @author paouelle
   *
   * @param  validator the validator to check
   * @return <code>true</code> if the validator is a user type; <code>false</code>
   *         otherwise
   */
  public static boolean isUserType(String validator) {
    return DataTypeClassNameParser.isUserType(validator);
  }

  /**
   * Checks if the specified validator string is a tuple type.
   *
   * @author paouelle
   *
   * @param  validator the validator to check
   * @return <code>true</code> if the validator is a tuple type; <code>false</code>
   *         otherwise
   */
  public static boolean isTupleType(String validator) {
    return DataTypeClassNameParser.isTupleType(validator);
  }

  /**
   * Prevents instantiation.
   *
   * @author paouelle
   */
  private DataTypeParser() {}
}

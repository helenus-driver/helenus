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
import org.helenus.driver.impl.StatementManagerImpl;
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
   * @param  mgr the non-<code>null</code> statement manager
   * @param  t the data type to convert
   * @param  isFrozen <code>true</code> if the data type is frozen; <code>false</code>
   *         otherwise
   * @return the corresponding CQL data type
   */
  private static CQLDataType toCQL(
    StatementManagerImpl mgr, DataType t, boolean isFrozen
  ) {
    if (t instanceof UserType) {
      final UserType ut = (UserType)t;

      return new CQLDataType() {
        @Override
        public String name() {
          return ut.getTypeName();
        }
        @Override
        public boolean isFrozen() {
          return true;
        }
        @Override
        public boolean isCollection() {
          return false;
        }
        @Override
        public boolean isTuple() {
          return false;
        }
        @Override
        public boolean isUserDefined() {
          return true;
        }
        @Override
        public DataType getDataType() {
          return ut;
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
        public boolean isFrozen() {
          return true;
        }
        @Override
        public boolean isCollection() {
          return false;
        }
        @Override
        public boolean isTuple() {
          return true;
        }
        @Override
        public boolean isUserDefined() {
          return false;
        }
        @Override
        public DataType getDataType() {
          return tt;
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
        @SuppressWarnings("synthetic-access")
        @Override
        public String toCQL() {
          return tt.getComponentTypes().stream()
            .map(ct -> DataTypeParser.toCQL(mgr, ct, true).toCQL())
            .collect(Collectors.joining(",", "frozen<tuple<", ">>"));
        }
      };
    }
    return new Definition(
      mgr,
      org.helenus.driver.persistence.DataType.valueOf(t.getName().name()),
      t.getTypeArguments().stream()
        .map(ta -> DataTypeParser.toCQL(mgr, ta, true))
        .collect(Collectors.toList()),
      isFrozen
    );
  }

  /**
   * Converts the specified validator string to a CQL data type.
   *
   * @author paouelle
   *
   * @param  mgr the non-<code>null</code> statement manager
   * @param  validator the validator to be converted
   * @return the corresponding CQL data type
   */
  public static CQLDataType validatorToCQL(
    StatementManagerImpl mgr, String validator
  ) {
    return DataTypeParser.toCQL(
      mgr,
      DataTypeClassNameParser.parseOne(
        validator,
        mgr.getProtocolVersion(),
        mgr.getCodecRegistry()
      ),
      DataTypeClassNameParser.isFrozen(validator)
    );
  }

  /**
   * Converts the specified type string to a CQL data type.
   *
   * @author paouelle
   *
   * @param  mgr the non-<code>null</code> statement manager
   * @param  type the type to be converted
   * @return the corresponding CQL data type
   */
  public static CQLDataType typeToCQL(
    StatementManagerImpl mgr, String type
  ) {
    final boolean isFrozen;

    if (type.startsWith("org.apache.cassandra.db.marshal.FrozenType(")) {
      type = type.substring(43, type.length() - 1);
      isFrozen = true;
    } else {
      isFrozen = false;
    }
    return DataTypeParser.toCQL(
      mgr,
      DataTypeClassNameParser.parseOne(
        type,
        mgr.getProtocolVersion(),
        mgr.getCodecRegistry()
      ),
      isFrozen
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

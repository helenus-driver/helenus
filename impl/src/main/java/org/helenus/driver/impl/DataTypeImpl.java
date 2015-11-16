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
package org.helenus.driver.impl;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import java.math.BigDecimal;
import java.math.BigInteger;

import java.io.IOException;

import java.net.InetAddress;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.EnumMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZoneId;

import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.StringUtils;

import org.helenus.commons.lang3.reflect.ReflectionUtils;
import org.helenus.driver.StatementBuilder;
import org.helenus.driver.persistence.CQLDataType;
import org.helenus.driver.persistence.Column;
import org.helenus.driver.persistence.DataType;
import org.helenus.driver.persistence.Persisted;
import org.helenus.driver.persistence.Persister;
import org.helenus.driver.persistence.UDTEntity;

/**
 * The <code>DataTypeImpl</code> class provides definition for Cassandra data types
 * used for columns when one cannot rely on the default behavior where the type is
 * inferred from the field type.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author The Helenus Driver Project Authors
 * @version 1 - Jan 19, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
public class DataTypeImpl {
  /**
   * Holds the decoders for each possible Cassandra data types.
   *
   * @author paouelle
   */
  private static final Map<DataType, DataDecoder<?>[]> decoders
    = new EnumMap<>(DataType.class);

  /**
   * Initializes the decoders map for the specified data type.
   *
   * @author paouelle
   *
   * @param type the non-<code>null</code> data type
   * @param decoders valid decoders supporting to decode from this data type
   */
  private static void init(DataType type, DataDecoder<?>... decoders) {
    DataTypeImpl.decoders.put(type, decoders);
  }

  static {
    DataTypeImpl.init(DataType.ASCII, DataDecoder.asciiToEnum, DataDecoder.asciiToClass, DataDecoder.asciiToLocale, DataDecoder.asciiToZoneId, DataDecoder.asciiToString);
    DataTypeImpl.init(DataType.BIGINT, DataDecoder.bigintToLong);
    DataTypeImpl.init(DataType.BLOB, DataDecoder.blobToByteArray, DataDecoder.blobToByteBuffer);
    DataTypeImpl.init(DataType.BOOLEAN, DataDecoder.booleanToBoolean);
    DataTypeImpl.init(DataType.COUNTER, DataDecoder.counterToLong, DataDecoder.counterToAtomicLong);
    DataTypeImpl.init(DataType.DECIMAL, DataDecoder.decimalToBigDecimal);
    DataTypeImpl.init(DataType.DOUBLE, DataDecoder.doubleToDouble);
    DataTypeImpl.init(DataType.FLOAT, DataDecoder.floatToFloat);
    DataTypeImpl.init(DataType.INET, DataDecoder.inetToInetAddress);
    DataTypeImpl.init(DataType.INT, DataDecoder.intToInteger);
    DataTypeImpl.init(DataType.TEXT, DataDecoder.textToString);
    DataTypeImpl.init(DataType.TIMESTAMP, DataDecoder.timestampToLong, DataDecoder.timestampToDate, DataDecoder.timestampToInstant);
    DataTypeImpl.init(DataType.UUID, DataDecoder.uuidToUUID);
    DataTypeImpl.init(DataType.VARCHAR, DataDecoder.varcharToString);
    DataTypeImpl.init(DataType.VARINT, DataDecoder.varintToBigInteger);
    DataTypeImpl.init(DataType.TIMEUUID, DataDecoder.timeuuidToUUID);
  }

  /**
   * Unwraps the specific class if it is an {@link Optional}.
   *
   * @author paouelle
   *
   * @param  clazz the class to unwrap if required
   * @param  type the corresponding type for the class
   * @return the element of the optional class if it is an optional otherwise
   *         the class itself
   */
  public static Class<?> unwrapOptionalIfPresent(Class<?> clazz, Type type) {
    if (Optional.class.isAssignableFrom(clazz)) {
      if (type instanceof ParameterizedType) {
        final ParameterizedType ptype = (ParameterizedType)type;
        final Type atype = ptype.getActualTypeArguments()[0]; // optional will always have 1 argument

        return ReflectionUtils.getRawClass(atype);
      }
    }
    return clazz;
  }

  /**
   * The <code>Definition</code> class provides a data type definition for a CQL
   * data type.
   *
   * @copyright 2015-2015 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 19, 2015 - paouelle - Creation
   *
   * @since 1.0
   */
  public static class Definition implements CQLDataType {
    /**
     * Holds the data type for this definition if not a user-defined type.
     *
     * @author paouelle
     */
    private final CQLDataType type;

    /**
     * Holds the arguments data type if this definition represents a collection.
     *
     * @author paouelle
     */
    private final List<CQLDataType> arguments;

    /**
     * Instantiates a new <code>Definition</code> object.
     *
     * @author paouelle
     *
     * @param type the non-<code>null</code> data type for this definition
     * @param arguments the non-<code>null</code> list of arguments' data types
     *        if the type represents a collection (may be empty)
     */
    public Definition(DataType type, DataType... arguments) {
      this.type = type;
      this.arguments = Arrays.asList((CQLDataType[])arguments);
    }

    /**
     * Instantiates a new <code>Definition</code> object.
     *
     * @author paouelle
     *
     * @param type the non-<code>null</code> data type for this definition
     * @param arguments the non-<code>null</code> list of arguments' data types
     *        if the type represents a collection (may be empty)
     */
    public Definition(DataType type, List<CQLDataType> arguments) {
      this.type = type;
      this.arguments = arguments;
    }

    /**
     * Instantiates a new <code>Definition</code> object.
     *
     * @author paouelle
     *
     * @param types the non-<code>null</code> list of data types for this
     *        definition (the first one correspond to the data type and the
     *        remaining are the argument data types)
     */
    public Definition(List<CQLDataType> types) {
      this.type = types.remove(0);
      this.arguments = Collections.unmodifiableList(types);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.persistence.CQLDataType#name()
     */
    @Override
    public String name() {
      return type.name();
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.persistence.CQLDataType#isCollection()
     */
    @Override
    public boolean isCollection() {
      return !arguments.isEmpty();
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.persistence.CQLDataType#isUserDefined()
     */
    @Override
    public boolean isUserDefined() {
      return type instanceof UDTClassInfoImpl;
    }

    /**
     * Gets the data type for this definition.
     *
     * @author paouelle
     *
     * @return the non-<code>null</code> data type for this definition
     */
    public CQLDataType getType() {
      return type;
    }

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
    public CQLDataType getElementType() {
      if (arguments.size() == 0) {
        return null;
      }
      // assume the element type if the last one in the arguments list
      return arguments.get(arguments.size() - 1);
    }

    /**
     * Gets the data type for all arguments of the collection data type
     * represented by this definition.
     *
     * @author paouelle
     *
     * @return a non-<code>null</code> list of all argument data types for the
     *         collection
     */
    public List<CQLDataType> getArgumentTypes() {
      return arguments;
    }

    /**
     * Gets the data type for the first argument of the collection data type
     * represented by this definition.
     *
     * @author paouelle
     *
     * @return the first argument data type for the collection or <code>null</code>
     *         if this definition doesn't represent a collection
     */
    public CQLDataType getFirstArgumentType() {
      return (arguments.isEmpty() ? null : arguments.get(0));
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.persistence.CQLDataType#isAlterableTo(org.helenus.driver.persistence.CQLDataType)
     */
    @Override
    public boolean isAlterableTo(CQLDataType to) {
      if (to instanceof Definition) {
        final Definition tod = (Definition)to;

        if (!type.isAlterableTo(tod.type)) {
          return false;
        }
        if (isCollection()) { // check arguments
          if (arguments.size() != tod.arguments.size()) {
            return false;
          }
          for (int i = 0; i < arguments.size(); i++) {
            if (!arguments.get(i).isAlterableTo(tod.arguments.get(i))) {
              return false;
            }
          }
        }
        return true;
      }
      if (isCollection() || isUserDefined()) {
        return false;
      }
      return type.isAlterableTo(to);
    }

    /**
     * Gets a decoder for this data type to decode to the specified field.
     *
     * @author paouelle
     *
     * @param  field the field to decode to its declared type
     * @param  mandatory if the field is mandatory or represents a primary key
     * @return a decoder suitable to decode from this data type to the given field
     * @throws NullPointerException if <code>field</code> is <code>null</code>
     * @throws IllegalArgumentException if the combination of field and data types
     *         is not supported
     */
    @SuppressWarnings("synthetic-access")
    public DataDecoder<?> getDecoder(Field field, boolean mandatory) {
      org.apache.commons.lang3.Validate.notNull(field, "invalid null field");
      final Persisted persisted = field.getAnnotation(Persisted.class);
      final Class<?> clazz = ClassUtils.primitiveToWrapper(
        DataTypeImpl.unwrapOptionalIfPresent(field.getType(), field.getGenericType())
      );

      // check if we are dealing with a collection
      if (isCollection()) {
        if (List.class.isAssignableFrom(clazz)) {
          if (persisted != null) {
            // if persisted then we need to decode: List<persisted.as()>
            return DataDecoder.list(persisted.as().CLASS, mandatory);
          }
          final Type type = field.getGenericType();

          if (type instanceof ParameterizedType) {
            final ParameterizedType ptype = (ParameterizedType)type;
            final Type atype = ptype.getActualTypeArguments()[0]; // lists will always have 1 argument

            return DataDecoder.list(ReflectionUtils.getRawClass(atype), mandatory);
          }
          throw new IllegalArgumentException(
            "unable to determine element type for field: "
            + field.getDeclaringClass().getName()
            + "."
            + field.getName()
          );
        } else if (Set.class.isAssignableFrom(clazz)) {
          if (persisted != null) {
            // if persisted then we need to decode: Set<persisted.as()>
            return DataDecoder.set(persisted.as().CLASS, mandatory);
          }
          final Type type = field.getGenericType();

          if (type instanceof ParameterizedType) {
            final ParameterizedType ptype = (ParameterizedType)type;
            final Type atype = ptype.getActualTypeArguments()[0]; // sets will always have 1 argument

            return DataDecoder.set(ReflectionUtils.getRawClass(atype), mandatory);
          }
          throw new IllegalArgumentException(
            "unable to determine element type for field: "
            + field.getDeclaringClass().getName()
            + "."
            + field.getName()
          );
        } else if (SortedMap.class.isAssignableFrom(clazz)) {
          final Type type = field.getGenericType();

          if (type instanceof ParameterizedType) {
            final ParameterizedType ptype = (ParameterizedType)type;
            final Type ktype = ptype.getActualTypeArguments()[0]; // maps will always have 2 arguments

            if (persisted != null) {
              // if persisted then we need to decode: Map<?,persisted.as()>
              return DataDecoder.sortedMap(
                ReflectionUtils.getRawClass(ktype), persisted.as().CLASS, mandatory
              );
            }
            final Type vtype = ptype.getActualTypeArguments()[1]; // maps will always have 2 arguments

            return DataDecoder.sortedMap(
              ReflectionUtils.getRawClass(ktype),
              ReflectionUtils.getRawClass(vtype),
              mandatory
            );
          }
          throw new IllegalArgumentException(
            "unable to determine elements type for field: "
            + field.getDeclaringClass().getName()
            + "."
            + field.getName()
          );
        } else if (Map.class.isAssignableFrom(clazz)) {
          final Type type = field.getGenericType();

          if (type instanceof ParameterizedType) {
            final ParameterizedType ptype = (ParameterizedType)type;
            final Type ktype = ptype.getActualTypeArguments()[0]; // maps will always have 2 arguments

            if (persisted != null) {
              // if persisted then we need to decode: Map<?,persisted.as()>
              return DataDecoder.map(
                ReflectionUtils.getRawClass(ktype), persisted.as().CLASS, mandatory
              );
            }
            final Type vtype = ptype.getActualTypeArguments()[1]; // maps will always have 2 arguments

            return DataDecoder.map(
              ReflectionUtils.getRawClass(ktype),
              ReflectionUtils.getRawClass(vtype),
              mandatory
            );
          }
          throw new IllegalArgumentException(
            "unable to determine elements type for field: "
            + field.getDeclaringClass().getName()
            + "."
            + field.getName()
          );
        }
      } else if (isUserDefined()) {
        return DataDecoder.udt((UDTClassInfoImpl<?>)type);
      }
      // if we get here then we have a system data type that is not a collection
      final DataDecoder<?> decoder;

      if (persisted != null) {
        // if persisted then we need to decode: persisted.as()
        decoder = DataTypeImpl.getDecoder((DataType)type, persisted.as().CLASS);
      } else {
        // check if we can get a decoder for the field data type
        decoder = DataTypeImpl.getDecoder((DataType)type, clazz);
      }
      if (decoder != null) { // found one so return it
        return decoder;
      }
      // oh well we cannot convert that type
      throw new IllegalArgumentException(
        "unsupported type to convert to: "
        + clazz.getName()
        + " for field: "
        + field.getDeclaringClass().getName()
        + "."
        + field.getName()
      );
    }

    /**
     * Gets a decoder for this data type to decode to the specified class.
     *
     * @author paouelle
     *
     * @param  clazz the collection class to decode to its declared super type
     * @return a decoder suitable to decode from this data type to the given class
     * @throws NullPointerException if <code>clazz</code> is <code>null</code>
     * @throws IllegalArgumentException if the combination of class and data types
     *         is not supported
     * @throws IllegalStateException if this definition is not representing a
     *         collection
     */
    @SuppressWarnings("synthetic-access")
    public DataDecoder<?> getDecoder(Class<?> clazz) {
      org.apache.commons.lang3.Validate.notNull(clazz, "invalid null class");
      org.apache.commons.lang3.Validate.validState(isCollection(), "not a collection definition");
      if (List.class.isAssignableFrom(clazz)) {
        final Type type = clazz.getGenericSuperclass();

        if (type instanceof ParameterizedType) {
          final ParameterizedType ptype = (ParameterizedType)type;
          final Type atype = ptype.getActualTypeArguments()[0]; // lists will always have 1 argument

          return DataDecoder.list(ReflectionUtils.getRawClass(atype), true);
        }
        throw new IllegalArgumentException(
          "unable to determine element type for class: "
          + clazz.getName()
        );
      } else if (Set.class.isAssignableFrom(clazz)) {
        final Type type = clazz.getGenericSuperclass();

        if (type instanceof ParameterizedType) {
          final ParameterizedType ptype = (ParameterizedType)type;
          final Type atype = ptype.getActualTypeArguments()[0]; // sets will always have 1 argument

          return DataDecoder.set(ReflectionUtils.getRawClass(atype), true);
        }
        throw new IllegalArgumentException(
          "unable to determine element type for class: "
          + clazz.getName()
        );
      } else if (SortedMap.class.isAssignableFrom(clazz)) {
        final Type type = clazz.getGenericSuperclass();

        if (type instanceof ParameterizedType) {
          final ParameterizedType ptype = (ParameterizedType)type;
          final Type ktype = ptype.getActualTypeArguments()[0]; // maps will always have 2 arguments
          final Type vtype = ptype.getActualTypeArguments()[1]; // maps will always have 2 arguments

          return DataDecoder.sortedMap(
            ReflectionUtils.getRawClass(ktype),
            ReflectionUtils.getRawClass(vtype),
            true
          );
        }
        throw new IllegalArgumentException(
          "unable to determine elements type for class: "
          + clazz.getName()
        );
      } else if (Map.class.isAssignableFrom(clazz)) {
        final Type type = clazz.getGenericSuperclass();

        if (type instanceof ParameterizedType) {
          final ParameterizedType ptype = (ParameterizedType)type;
          final Type ktype = ptype.getActualTypeArguments()[0]; // maps will always have 2 arguments
          final Type vtype = ptype.getActualTypeArguments()[1]; // maps will always have 2 arguments

          return DataDecoder.map(
            ReflectionUtils.getRawClass(ktype),
            ReflectionUtils.getRawClass(vtype),
            true
          );
        }
        throw new IllegalArgumentException(
          "unable to determine elements type for class: "
          + clazz.getName()
        );
      }
      // oh well we cannot convert that type
      throw new IllegalArgumentException(
        "unsupported collection type to convert to: "
        + clazz.getSuperclass().getName()
        + " for class: "
        + clazz.getName()
      );
    }

    /**
     * Encodes the specified value using the given persister based on this
     * definition.
     *
     * @author paouelle
     *
     * @param <T> the decoded type
     * @param <PT> the persisted type
     *
     * @param  val the value to be encoded
     * @param  persisted the persisted annotation for the persister
     * @param  persister the persister to use
     * @param  fname the non-<code>null</code> field name
     * @return the corresponding decoded value or <code>null</code> if the value
     *         was <code>null</code> to start with or no encoding was required
     * @throws IOException if an encoding error occurs
     * @throws ClassCastException if this definition represents a collection
     *         and the value cannot be type casted to the expected collection
     *         type or the encoded value cannot be type casted to the persisted
     *         class
     */
    @SuppressWarnings("unchecked")
    public <T, PT> Object encode(
      Object val, Persisted persisted, Persister<T, PT> persister, String fname
    ) throws IOException {
      if ((val == null)
          || (persisted == null)
          || (persister == null)
          || (val instanceof PersistedObject)) {
        return val;
      }
      if (type == DataType.LIST) {
        return new PersistedList<>(persisted, persister, fname, (List<T>)val, false);
      }
      if (type == DataType.SET) {
        return new PersistedSet<>(persisted, persister, fname, (Set<T>)val, false);
      }
      if (type == DataType.MAP) {
        return new PersistedMap<>(persisted, persister, fname, (Map<?, T>)val, false);
      }
      if (type == DataType.SORTED_MAP) {
        return new PersistedNavigableMap<>(persisted, persister, fname, (Map<?, T>)val, false);
      }
      // for all else, simply encode the value directly
      final PersistedValue<T, PT> pval = new PersistedValue<>(
        persisted, persister, fname
      ).setDecodedValue((T)val);

      pval.getEncodedValue(); // force it to be decoded
      return pval;
    }

    /**
     * Encodes the specified element value using the given persister based on this
     * definition.
     *
     * @author paouelle
     *
     * @param <T> the decoded type
     * @param <PT> the persisted type
     *
     * @param  val the element value to be encoded
     * @param  persisted the persisted annotation for the persister
     * @param  persister the persister to use
     * @param  fname the non-<code>null</code> field name
     * @return the corresponding decoded value or <code>null</code> if the value
     *         was <code>null</code> to start with or no encoding was required
     * @throws IOException if an encoding error occurs
     */
    @SuppressWarnings("unchecked")
    public <T, PT> Object encodeElement(
      Object val, Persisted persisted, Persister<T, PT> persister, String fname
    ) throws IOException {
      if ((val == null)
          || (persisted == null)
          || (persister == null)
          || (val instanceof PersistedObject)) {
        return val;
      }
      if ((type == DataType.LIST)
          || (type == DataType.SET)
          || (type == DataType.MAP)) {
        // encode the element value directly
        final PersistedValue<T, PT> pval = new PersistedValue<>(
            persisted, persister, fname
        ).setDecodedValue((T)val);

        pval.getEncodedValue(); // force it to be decoded
        return pval;
      }
      throw new IOException("field is not a list, a set, or a map");
    }

    /**
     * Decodes the specified value using the given persister based on this
     * definition.
     *
     * @author paouelle
     *
     * @param <T> the decoded type
     * @param <PT> the persisted type
     *
     * @param  val the value to be encoded
     * @param  persisted the persisted annotation for the persister
     * @param  persister the persister to use
     * @param  fname the non-<code>null</code> field name
     * @return the corresponding decoded value or <code>null</code> if the value
     *         was <code>null</code> to start with or no decoding was required
     * @throws IOException if an encoding error occurs
     * @throws ClassCastException if this definition represents a collection
     *         and the value cannot be type casted to the expected collection
     *         type or again the value to decode cannot be type casted to the
     *         persisted class
     */
    @SuppressWarnings("unchecked")
    public <T, PT> Object decode(
      Object val, Persisted persisted, Persister<T, PT> persister, String fname
    ) throws IOException {
      if ((val == null)
          || (persisted == null)
          || (persister == null)
          || (val instanceof PersistedObject)) {
        return val;
      }
      if (type == DataType.LIST) {
        return new PersistedList<>(persisted, persister, fname, (List<PT>)val, true);
      }
      if (type == DataType.SET) {
        return new PersistedSet<>(persisted, persister, fname, (Set<PT>)val, true);
      }
      if (type == DataType.MAP) {
        return new PersistedMap<>(persisted, persister, fname, (Map<?,PT>)val, true);
      }
      if (type == DataType.SORTED_MAP) {
        return new PersistedNavigableMap<>(persisted, persister, fname, (Map<?,PT>)val, true);
      }
      // for all else, simply decode the value directly
      return persister.decode(
        persister.getPersistedClass().cast(persisted.as().CLASS.cast(val))
      );
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.persistence.CQLDataType#toCQL()
     */
    @Override
    public String toCQL() {
      if (!isCollection()) {
        return type.toCQL();
      }
      final List<String> cqls = new ArrayList<>(arguments.size());
      final StringBuilder sb = new StringBuilder();

      for (final CQLDataType atype: arguments) {
        cqls.add(atype.toCQL());
      }
      sb
        .append(type.toCQL())
        .append('<')
        .append(StringUtils.join(cqls, ','))
        .append('>');
      return sb.toString();
    }

    /**
     * Gets all user-defined types this definition is dependent on.
     *
     * @author paouelle
     *
     * @return a stream of all class infos for the user-defined types this
     *         definition depends on
     */
    public Stream<UDTClassInfoImpl<?>> udts() {
      if (type instanceof UDTClassInfoImpl) {
        return Stream.of((UDTClassInfoImpl<?>)type);
      }
      final CQLDataType etype = getElementType();

      if (etype instanceof UDTClassInfoImpl) {
        return Stream.of((UDTClassInfoImpl<?>)etype);
      }
      return Stream.empty();
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
      return toCQL();
    }
  };

  /**
   * Gets a decoder for the specified target class given a data type.
   *
   * @author paouelle
   *
   * @param  type the non-<code>null</code> data type for which to get a decoder
   * @param  clazz the non-<code>null</code> class to decode to
   * @return the corresponding decoder or <code>null</code> if none found
   */
  private static DataDecoder<?> getDecoder(DataType type, Class<?> clazz) {
    final DataDecoder<?>[] decoders = DataTypeImpl.decoders.get(type);

    // quick check if there is one then use that one no matter what the given
    // class is
    if (decoders.length == 1) {
      return decoders[0];
    }
    // search the defined list of decoders for the right one
    for (final DataDecoder<?> decoder: decoders) {
      if (decoder.canDecodeTo(clazz)) {
        return decoder;
      }
    }
    return null;
  }

  /**
   * Infers the data type for the specified field.
   *
   * @author paouelle
   *
   * @param  field the non-<code>null</code> field to infer the CQL data type for
   * @param  clazz the non-<code>null</code> class for which to infer the CQL
   *         data type for the field
   * @param  types the non-<code>null</code> list where to add the inferred type and
   *         its arguments
   * @throws IllegalArgumentException if the data type cannot be inferred from
   *         the field
   */
  private static void inferDataTypeFrom(
    Field field,
    Class<?> clazz,
    List<CQLDataType> types
  ) {
    inferDataTypeFrom(
      "field: " + field.getDeclaringClass().getName() + "." + field.getName(),
      field.getGenericType(),
      clazz,
      types,
      field.getAnnotation(Persisted.class)
    );
  }

  /**
   * Infers the data type for the specified class.
   *
   * @author paouelle
   *
   * @param  clazz the non-<code>null</code> class for which to infer the CQL
   *         data type for
   * @param  types the non-<code>null</code> list where to add the inferred type and
   *         its arguments
   * @throws IllegalArgumentException if the data type cannot be inferred from
   *         the class
   */
  private static void inferDataTypeFrom(
    Class<?> clazz, List<CQLDataType> types
  ) {
    // at this point we are trying to infer the type from the superclass in
    // order to generate a fake field, so make sure to continue on with the
    // class super class
    inferDataTypeFrom(
      "class: " + clazz.getName(),
      clazz.getGenericSuperclass(),
      clazz.getSuperclass(),
      types,
      clazz.getAnnotation(Persisted.class)
    );
  }

  /**
   * Infers the data type for the specified field or class.
   *
   * @author paouelle
   *
   * @param  trace the non-<code>null</code> field or class trace string
   *         to infer the CQL data type for
   * @param  type the generic type for the field or class to infer the CQL data
   *         type for
   * @param  clazz the non-<code>null</code> class for which to infer the CQL
   *         data type for
   * @param  types the non-<code>null</code> list where to add the inferred type and
   *         its arguments
   * @param  persisted the persisted annotation to consider for the field or class
   * @throws IllegalArgumentException if the data type cannot be inferred from
   *         the field or class
   */
  private static void inferDataTypeFrom(
    String trace,
    Type type,
    Class<?> clazz,
    List<CQLDataType> types,
    Persisted persisted
  ) {
    // check if the class is annotated as a UDT in which case we kind of allow
    // collections of collections since clazz is actually a udt
    final boolean isUDT = clazz.getAnnotation(UDTEntity.class) != null;

    if (Optional.class.isAssignableFrom(clazz)) {
      org.apache.commons.lang3.Validate.isTrue(
        !isUDT && types.isEmpty(),
        "collection of optionals is not supported in %s",
        trace
      );
      if (type instanceof ParameterizedType) {
        final ParameterizedType ptype = (ParameterizedType)type;
        final Type atype = ptype.getActualTypeArguments()[0]; // optional will always have 1 argument

        if (persisted != null) {
          types.add(persisted.as());
        } else {
          DataTypeImpl.inferBasicDataTypeFrom(trace, ReflectionUtils.getRawClass(atype), types, persisted);
        }
        return;
      }
    } else if (List.class.isAssignableFrom(clazz)) {
      if (isUDT) {
        // we need to this because a UDT can actually implement List and in such case,
        // we want to return its UDT type and not a list
        DataTypeImpl.inferBasicDataTypeFrom(trace, clazz, types, persisted);
        return;
      }
      org.apache.commons.lang3.Validate.isTrue(
        types.isEmpty(),
        "collection of collections is not supported in %s",
        trace
      );
      if (type instanceof ParameterizedType) {
        final ParameterizedType ptype = (ParameterizedType)type;
        final Type atype = ptype.getActualTypeArguments()[0]; // lists will always have 1 argument

        types.add(DataType.LIST);
        if (persisted != null) {
          types.add(persisted.as());
        } else {
          DataTypeImpl.inferDataTypeFrom(trace, type, ReflectionUtils.getRawClass(atype), types, persisted);
        }
        return;
      }
    } else if (Set.class.isAssignableFrom(clazz)) {
      if (isUDT) {
        // we need to this because a UDT can actually implement Set and in such case,
        // we want to return its UDT type and not a set
        DataTypeImpl.inferBasicDataTypeFrom(trace, clazz, types, persisted);
        return;
      }
      org.apache.commons.lang3.Validate.isTrue(
        types.isEmpty(),
        "collection of collections is not supported in %s",
        trace
      );
      if (type instanceof ParameterizedType) {
        final ParameterizedType ptype = (ParameterizedType)type;
        final Type atype = ptype.getActualTypeArguments()[0]; // sets will always have 1 argument

        types.add(DataType.SET);
        if (persisted != null) {
          types.add(persisted.as());
        } else {
          DataTypeImpl.inferDataTypeFrom(trace, type, ReflectionUtils.getRawClass(atype), types, persisted);
        }
        return;
      }
    } else if (SortedMap.class.isAssignableFrom(clazz)) {
      if (isUDT) {
        // we need to this because a UDT can actually implement Map and in such case,
        // we want to return its UDT type and not a map
        DataTypeImpl.inferBasicDataTypeFrom(trace, clazz, types, persisted);
        return;
      }
      org.apache.commons.lang3.Validate.isTrue(
        types.isEmpty(),
        "collection of collections is not supported in %s",
        trace
      );
      if (type instanceof ParameterizedType) {
        final ParameterizedType ptype = (ParameterizedType)type;
        final Type ktype = ptype.getActualTypeArguments()[0]; // maps will always have 2 arguments
        final Type vtype = ptype.getActualTypeArguments()[1]; // maps will always have 2 arguments

        types.add(DataType.SORTED_MAP);
        // don't consider the @Persister annotation for the key
        DataTypeImpl.inferDataTypeFrom(trace, type, ReflectionUtils.getRawClass(ktype), types, null);
        if (persisted != null) {
          types.add(persisted.as());
        } else {
          DataTypeImpl.inferDataTypeFrom(trace, type, ReflectionUtils.getRawClass(vtype), types, persisted);
        }
        return;
      }
    } else if (Map.class.isAssignableFrom(clazz)) {
      if (isUDT) {
        // we need to this because a UDT can actually implement Map and in such case,
        // we want to return its UDT type and not a map
        DataTypeImpl.inferBasicDataTypeFrom(trace, clazz, types, persisted);
        return;
      }
      org.apache.commons.lang3.Validate.isTrue(
        types.isEmpty(),
        "collection of collections is not supported in %s",
        trace
      );
      if (type instanceof ParameterizedType) {
        final ParameterizedType ptype = (ParameterizedType)type;
        final Type ktype = ptype.getActualTypeArguments()[0]; // maps will always have 2 arguments
        final Type vtype = ptype.getActualTypeArguments()[1]; // maps will always have 2 arguments

        types.add(DataType.MAP);
        // don't consider the @Persister annotation for the key
        DataTypeImpl.inferDataTypeFrom(trace, type, ReflectionUtils.getRawClass(ktype), types, null);
        if (persisted != null) {
          types.add(persisted.as());
        } else {
          DataTypeImpl.inferDataTypeFrom(trace, type, ReflectionUtils.getRawClass(vtype), types, persisted);
        }
        return;
      }
    }
    DataTypeImpl.inferBasicDataTypeFrom(trace, clazz, types, persisted);
  }

  /**
   * Infers the data type for the specified field or class once it has already
   * been processed for optional and collections.
   *
   * @author paouelle
   *
   * @param  trace the non-<code>null</code> field or class trace string
   *         to infer the CQL data type for
   * @param  clazz the non-<code>null</code> class for which to infer the CQL
   *         data type for the field
   * @param  types the non-<code>null</code> list where to add the inferred type and
   *         its arguments
   * @param  persisted the persisted annotation to consider for the field
   * @throws IllegalArgumentException if the data type cannot be inferred from
   *         the field
   */
  private static void inferBasicDataTypeFrom(
    String trace,
    Class<?> clazz,
    List<CQLDataType> types,
    Persisted persisted
  ) {
    clazz = ClassUtils.primitiveToWrapper(clazz);
    if (persisted != null) {
      types.add(persisted.as());
    } else if (String.class == clazz) {
      types.add(DataType.TEXT);
    } else if (Integer.class == clazz) {
      types.add(DataType.INT);
    } else if (Long.class == clazz) {
      types.add(DataType.BIGINT);
    } else if (Boolean.class == clazz) {
      types.add(DataType.BOOLEAN);
    } else if (Date.class.isAssignableFrom(clazz)) {
      types.add(DataType.TIMESTAMP);
    } else if (Double.class == clazz) {
      types.add(DataType.DOUBLE);
    } else if (Float.class == clazz) {
      types.add(DataType.FLOAT);
    } else if (UUID.class.isAssignableFrom(clazz)) {
      types.add(DataType.UUID);
    } else if ((clazz.isArray() && (Byte.TYPE == clazz.getComponentType()))
               || ByteBuffer.class.isAssignableFrom(clazz)) {
      types.add(DataType.BLOB);
    } else if (BigDecimal.class.isAssignableFrom(clazz)) {
      types.add(DataType.DECIMAL);
    } else if (BigInteger.class.isAssignableFrom(clazz)) {
      types.add(DataType.VARINT);
    } else if (InetAddress.class.isAssignableFrom(clazz)) {
      types.add(DataType.INET);
    } else if (AtomicLong.class.isAssignableFrom(clazz)) {
      types.add(DataType.COUNTER);
    } else if (clazz.isEnum()) {
      types.add(DataType.ASCII);
    } else if (Class.class == clazz) {
      types.add(DataType.ASCII);
    } else if (Locale.class == clazz) {
      types.add(DataType.ASCII);
    } else if (ZoneId.class.isAssignableFrom(clazz)) {
      types.add(DataType.ASCII);
    } else if (Instant.class == clazz) {
      types.add(DataType.TIMESTAMP);
    } else {
      // check if it is a user-defined type (possibly defining it if not defined yet)
      try {
        final ClassInfoImpl<?> cinfo
          = (ClassInfoImpl<?>)StatementBuilder.getClassInfo(clazz);

        org.apache.commons.lang3.Validate.isTrue(
          cinfo instanceof UDTClassInfoImpl,
          "unable to infer data type in %s", trace
        );
        types.add((UDTClassInfoImpl<?>)cinfo);
      } catch (Exception e) {
        throw new IllegalArgumentException(
          "unable to infer data type in " + trace, e
        );
      }
    }
  }

  /**
   * Infers the data type from the specified field.
   *
   * @author paouelle
   *
   * @param  field the field from which to infer the data type
   * @return a non-<code>null</code> data type definition
   * @throws NullPointerException if <code>field</code> is <code>null</code>
   * @throws IllegalArgumentException if the data type cannot be inferred from
   *         the field or it is persisted but the persister cannot be instantiate
   */
  public static Definition inferDataTypeFrom(Field field) {
    org.apache.commons.lang3.Validate.notNull(field, "invalid null field");
    final Persisted persisted = field.getAnnotation(Persisted.class);

    if (persisted == null) {
      final Column.Data cdata = field.getAnnotation(Column.Data.class);

      if (cdata != null) {
        final List<CQLDataType> itypes = new ArrayList<>(3); // full set o inferred type if processed
        DataType type = cdata.type();
        final List<CQLDataType> atypes
          = new ArrayList<>(Arrays.asList(cdata.arguments()));

        if (!atypes.isEmpty() && (type == DataType.INFERRED)) {
          // we have an inferred type for the collection so calculate as
          // if it was all inferred and extract only the part we need
          DataTypeImpl.inferDataTypeFrom(field, field.getType(), itypes);
          org.apache.commons.lang3.Validate.isTrue(
            itypes.get(0) instanceof DataType,
            "missing data type value in field: %s.%s",
            field.getDeclaringClass().getName(),
            field.getName()
          );
          type = (DataType)itypes.get(0);
        }
        if (type != DataType.INFERRED) {
          org.apache.commons.lang3.Validate.isTrue(
            !(atypes.size() < type.NUM_ARGUMENTS),
            "missing argument data type(s) for '%s' in field: %s.%s",
            type.CQL,
            field.getDeclaringClass().getName(),
            field.getName()
          );
          org.apache.commons.lang3.Validate.isTrue(
            !((type.NUM_ARGUMENTS == 0) && (atypes.size() != 0)),
            "data type '%s' is not a collection in field: %s.%s",
            type.CQL,
            field.getDeclaringClass().getName(),
            field.getName()
          );
          org.apache.commons.lang3.Validate.isTrue(
            !(atypes.size() > type.NUM_ARGUMENTS),
            "too many argument data type(s) for '%s' in field: %s.%s",
            type.CQL,
            field.getDeclaringClass().getName(),
            field.getName()
          );
          if (type.NUM_ARGUMENTS != 0) {
            org.apache.commons.lang3.Validate.isTrue(
              ((DataType)atypes.get(0)).NUM_ARGUMENTS == 0,
              "collection of collection is not supported in field: %s.%s",
              field.getDeclaringClass().getName(),
              field.getName()
            );
            org.apache.commons.lang3.Validate.isTrue(
              !((type.NUM_ARGUMENTS > 1)
                && (((DataType)atypes.get(1)).NUM_ARGUMENTS != 0)),
              "map of collection is not supported in field: %s.%s",
              field.getDeclaringClass().getName(),
              field.getName()
            );
            if ((((DataType)atypes.get(0)) == DataType.INFERRED)
                || ((type.NUM_ARGUMENTS > 1)
                    && (((DataType)atypes.get(1)) == DataType.INFERRED))) {
              // we have an inferred part for the collection so calculate as
              // if it was all inferred and extract only the part we need
              if (itypes.isEmpty()) { // only do it if not already done!!!
                DataTypeImpl.inferDataTypeFrom(field, field.getType(), itypes);
              }
              for (int i = 0; i < atypes.size(); i++) {
                if (atypes.get(i) == DataType.INFERRED) {
                  atypes.set(i, itypes.get(i + 1)); // since index 1 corresponds to the collection type
                }
              }
            }
          }
          // TODO: should probably check that the CQL specified matches a supported class for it
          return new Definition(type, atypes);
        }
      }
    }
    // if we get here then the type was either not inferred or there was no
    // Column.Data annotation or there was a Persisted annotation
    final Column.Data cdata = field.getAnnotation(Column.Data.class);

    if (cdata != null) { // also annotated with @Column.Data
      // only type supported here is either INFERRED which falls through as if
      // no data type specified or BLOB
      final DataType type = cdata.type();

      if (type == DataType.BLOB) {
        return new Definition(DataType.BLOB);
      }
      org.apache.commons.lang3.Validate.isTrue(
        type == DataType.INFERRED,
        "unsupported data type '%s' in @Persisted annotated field: %s.%s",
        type.CQL,
        field.getDeclaringClass().getName(),
        field.getName()
      );
    }
    final List<CQLDataType> types = new ArrayList<>(3);

    DataTypeImpl.inferDataTypeFrom(field, field.getType(), types);
    return new Definition(types);
  }

  /**
   * Infers the data type from the specified class' superclass.
   *
   * @author paouelle
   *
   * @param  type the collection type being inferred
   * @param  clazz the class from which to infer the data type of its superclass
   * @return a non-<code>null</code> data type definition
   * @throws NullPointerException if <code>type</code> or <code>clazz</code> is
   *         <code>null</code>
   * @throws IllegalArgumentException if the argument data type cannot be inferred
   *         from the superclass or the specified type is not a collection data type
   */
  public static Definition inferDataTypeFrom(DataType type, Class<?> clazz) {
    org.apache.commons.lang3.Validate.notNull(type, "invalid null type");
    org.apache.commons.lang3.Validate.notNull(clazz, "invalid null class");
    org.apache.commons.lang3.Validate.isTrue(
      type.NUM_ARGUMENTS != 0,
      "data type '%s' is not a collection in class: %s",
      type.CQL,
      clazz.getName()
    );
    final Persisted persisted = clazz.getAnnotation(Persisted.class);

    if (persisted == null) {
      final UDTEntity.Data cdata = clazz.getAnnotation(UDTEntity.Data.class);

      if (cdata != null) { // we were not asked to infer the argument type
        final List<CQLDataType> atypes
          = new ArrayList<>(Arrays.asList(cdata.arguments()));

        org.apache.commons.lang3.Validate.isTrue(
          !(atypes.size() < type.NUM_ARGUMENTS),
          "missing argument data type(s) for '%s' in class: %s",
          type.CQL,
          clazz.getName()
        );
        org.apache.commons.lang3.Validate.isTrue(
          !(atypes.size() > type.NUM_ARGUMENTS),
          "too many argument data type(s) for '%s' in class: %s",
          type.CQL,
          clazz.getName()
        );
        if (type.NUM_ARGUMENTS != 0) {
          org.apache.commons.lang3.Validate.isTrue(
            ((DataType)atypes.get(0)).NUM_ARGUMENTS == 0,
            "collection of collection is not supported in class: %s",
            clazz.getName()
          );
          org.apache.commons.lang3.Validate.isTrue(
            !((type.NUM_ARGUMENTS > 1)
              && (((DataType)atypes.get(1)).NUM_ARGUMENTS != 0)),
            "map of collection is not supported in class: %s",
            clazz.getName()
          );
          if ((((DataType)atypes.get(0)) == DataType.INFERRED)
              || ((type.NUM_ARGUMENTS > 1)
                  && (((DataType)atypes.get(1)) == DataType.INFERRED))) {
            // we have an inferred part for the collection so calculate as
            // if it was all inferred and extract only the part we need
            final List<CQLDataType> types = new ArrayList<>(3);

            DataTypeImpl.inferDataTypeFrom(clazz, types);
            for (int i = 0; i < atypes.size(); i++) {
              if (atypes.get(i) == DataType.INFERRED) {
                atypes.set(i, types.get(i + 1)); // since index 1 corresponds to the collection type
              }
            }
          }
        }
        return new Definition(type, atypes);
      }
    }
    // if we get here then the type was either not inferred or there was no
    // UDTEntity.Data annotation or there was a Persisted annotation
    final List<CQLDataType> types = new ArrayList<>(3);

    DataTypeImpl.inferDataTypeFrom(clazz, types);
    return new Definition(types);
  }

  /**
   * Determines if the given data type is either the same as or is a superclass or
   * super interface of, the class or interface represented by the specified
   * <code>clazz</code> parameter. It returns <code>true</code> if so; otherwise
   * it returns <code>false</code>.
   *
   * @author paouelle
   *
   * @param  type the data type to check
   * @param  clazz the class to check
   * @return <code>true</code> if the given class can be assigned to the given data
   *         type
   */
  public static boolean isAssignableFrom(CQLDataType type, Class<?> clazz) {
    if (type.isUserDefined()) {
      return ((UDTClassInfoImpl<?>)type).getObjectClass().isAssignableFrom(clazz);
    }
    // search the defined list of decoders for the right one
    for (final DataDecoder<?> decoder: DataTypeImpl.decoders.get(type)) {
      if (decoder.canDecodeTo(clazz)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Determines if the given object is assignment compatible with this data
   * type.
   *
   * @author paouelle
   *
   * @param  type the data type to check
   * @param  object the object to check
   * @return <code>true</code> if the given object can be assigned to a column
   *         of this data type; <code>false</code> otherwise
   */
  public static boolean isInstance(CQLDataType type, Object object) {
    return (
      (object != null)
      ? DataTypeImpl.isAssignableFrom(type, object.getClass())
      : true  // null are always compatible
    );
  }
}

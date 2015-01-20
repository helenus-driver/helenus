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
package com.github.helenus.driver.impl;

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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZoneId;

import org.apache.commons.lang3.StringUtils;

import com.github.helenus.commons.lang3.reflect.ReflectionUtils;
import com.github.helenus.persistence.Column;
import com.github.helenus.persistence.DataType;
import com.github.helenus.persistence.Persisted;
import com.github.helenus.persistence.Persister;

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
  public static class Definition {
    /**
     * Holds the data type for this definition.
     *
     * @author paouelle
     */
    private final DataType type;

    /**
     * Holds the arguments data type if this definition represents a collection.
     *
     * @author paouelle
     */
    private final List<DataType> arguments;

    /**
     * Instantiates a new <code>Definition</code> object.
     *
     * @author paouelle
     *
     * @param type the non-<code>null</code> data type for this definition
     * @param arguments the non-<code>null</code> list of arguments' data types
     *        if the type represents a collection (may be empty)
     */
    Definition(DataType type, DataType... arguments) {
      this.type = type;
      this.arguments = Arrays.asList(arguments);
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
    Definition(List<DataType> types) {
      this.type = types.remove(0);
      this.arguments = Collections.unmodifiableList(types);
    }

    /**
     * Gets the data type for this definition.
     *
     * @author paouelle
     *
     * @return the non-<code>null</code> data type for this definition
     */
    public DataType getType() {
      return type;
    }

    /**
     * Checks if this definition represents a collection.
     *
     * @author paouelle
     *
     * @return <code>true</code> if this definition represent a collection;
     *         <code>false</code> otherwise
     */
    public boolean isCollection() {
      return !arguments.isEmpty();
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
    public DataType getElementType() {
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
    public List<DataType> getArgumentTypes() {
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
    public DataType getFirstArgumentType() {
      return (arguments.isEmpty() ? null : arguments.get(0));
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
      final Class<?> clazz = field.getType();

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
            // if persisted then we need to decode: Setpersisted.as()>
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
        } else if (Map.class.isAssignableFrom(clazz)) {
          final Type type = field.getGenericType();

          if (type instanceof ParameterizedType) {
            final ParameterizedType ptype = (ParameterizedType)type;
            final Type ktype = ptype.getActualTypeArguments()[0]; // lists will always have 2 arguments

            if (persisted != null) {
              // if persisted then we need to decode: Map<?,persisted.as()>
              return DataDecoder.map(
                ReflectionUtils.getRawClass(ktype), persisted.as().CLASS, mandatory
              );
            }
            final Type vtype = ptype.getActualTypeArguments()[1]; // lists will always have 2 arguments

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
      }
      final DataDecoder<?> decoder;

      if (persisted != null) {
        // if persisted then we need to decode: persisted.as()
        decoder = DataTypeImpl.getDecoder(type, persisted.as().CLASS);
      } else {
        // check if we can get a decoder for the field data type
        decoder = DataTypeImpl.getDecoder(type, clazz);
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
      if ((type == DataType.LIST) || (type == DataType.SET)) {
        // encode the element value directly
        final PersistedValue<T, PT> pval = new PersistedValue<>(
            persisted, persister, fname
        ).setDecodedValue((T)val);

        pval.getEncodedValue(); // force it to be decoded
        return pval;
      }
      throw new IOException("field is not a list or a set");
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
      // for all else, simply decode the value directly
      return persister.decode(
        persister.getPersistedClass().cast(persisted.as().CLASS.cast(val))
      );
    }

    /**
     * Get a CQL representation of this data type definition.
     *
     * @author paouelle
     *
     * @return a CQL representation of this data type definition
     */
    public String toCQL() {
      if (!isCollection()) {
        return type.CQL;
      }
      final List<String> cqls = new ArrayList<>(arguments.size());
      final StringBuilder sb = new StringBuilder();

      for (final DataType atype: arguments) {
        cqls.add(atype.CQL);
      }
      sb
        .append(type.CQL)
        .append('<')
        .append(StringUtils.join(cqls, ','))
        .append('>');
      return sb.toString();
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
    Field field, Class<?> clazz, List<DataType> types
  ) {
    inferDataTypeFrom(field, clazz, types, field.getAnnotation(Persisted.class));
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
   * @param  persisted the persisted annotation to consider for the field
   * @throws IllegalArgumentException if the data type cannot be inferred from
   *         the field
   */
  private static void inferDataTypeFrom(
    Field field, Class<?> clazz, List<DataType> types, Persisted persisted
  ) {
    if (List.class.isAssignableFrom(clazz)) {
      org.apache.commons.lang3.Validate.isTrue(
        types.isEmpty(),
        "collection of collection is not supported in field: %s",
        field
      );
      final Type type = field.getGenericType();

      if (type instanceof ParameterizedType) {
        final ParameterizedType ptype = (ParameterizedType)type;
        final Type atype = ptype.getActualTypeArguments()[0]; // lists will always have 1 argument

        types.add(DataType.LIST);
        if (persisted != null) {
          types.add(persisted.as());
        } else {
          DataTypeImpl.inferDataTypeFrom(field, ReflectionUtils.getRawClass(atype), types);
        }
        return;
      }
    } else if (Set.class.isAssignableFrom(clazz)) {
      org.apache.commons.lang3.Validate.isTrue(
        types.isEmpty(),
        "collection of collection is not supported in field: %s",
        field
      );
      final Type type = field.getGenericType();

      if (type instanceof ParameterizedType) {
        final ParameterizedType ptype = (ParameterizedType)type;
        final Type atype = ptype.getActualTypeArguments()[0]; // sets will always have 1 argument

        types.add(DataType.SET);
        if (persisted != null) {
          types.add(persisted.as());
        } else {
          DataTypeImpl.inferDataTypeFrom(field, ReflectionUtils.getRawClass(atype), types);
        }
        return;
      }
    } else if (Map.class.isAssignableFrom(clazz)) {
      org.apache.commons.lang3.Validate.isTrue(
        types.isEmpty(),
        "collection of collection is not supported in field: %s",
        field
      );
      final Type type = field.getGenericType();

      if (type instanceof ParameterizedType) {
        final ParameterizedType ptype = (ParameterizedType)type;
        final Type ktype = ptype.getActualTypeArguments()[0]; // maps will always have 2 arguments
        final Type vtype = ptype.getActualTypeArguments()[1]; // maps will always have 2 arguments

        types.add(DataType.MAP);
        // don't consider the @Persister annotation for the key
        DataTypeImpl.inferDataTypeFrom(field, ReflectionUtils.getRawClass(ktype), types, null);
        if (persisted != null) {
          types.add(persisted.as());
        } else {
          DataTypeImpl.inferDataTypeFrom(field, ReflectionUtils.getRawClass(vtype), types);
        }
        return;
      }
    }
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
      throw new IllegalArgumentException("unable to infer data type in field: " + field);
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
        final DataType type = cdata.type();
        DataType[] atypes = cdata.arguments();

        if (type != DataType.INFERRED) {
          org.apache.commons.lang3.Validate.isTrue(
            !(atypes.length < type.NUM_ARGUMENTS),
            "missing argument data type(s) for '%s' in field: %s.%s",
            type.CQL,
            field.getDeclaringClass().getName(),
            field.getName()
          );
          org.apache.commons.lang3.Validate.isTrue(
            !((type.NUM_ARGUMENTS == 0) && (atypes.length != 0)),
            "data type '%s' is not a collection in field: %s.%s",
            type.CQL,
            field.getDeclaringClass().getName(),
            field.getName()
          );
          org.apache.commons.lang3.Validate.isTrue(
            !(atypes.length > type.NUM_ARGUMENTS),
            "too many argument data type(s) for '%s' in field: %s.%s",
            type.CQL,
            field.getDeclaringClass().getName(),
            field.getName()
          );
          if (type.NUM_ARGUMENTS != 0) {
            org.apache.commons.lang3.Validate.isTrue(
              atypes[0].NUM_ARGUMENTS == 0,
              "collection of collection is not supported in field: %s.%s",
              field.getDeclaringClass().getName(),
              field.getName()
            );
            org.apache.commons.lang3.Validate.isTrue(
              !((type.NUM_ARGUMENTS > 1) && (atypes[1].NUM_ARGUMENTS != 0)),
              "collection of collection is not supported in field: %s.%s",
              field.getDeclaringClass().getName(),
              field.getName()
            );
            if ((atypes[0] == DataType.INFERRED)
                || ((type.NUM_ARGUMENTS > 1) && (atypes[1] == DataType.INFERRED))) {
              // we have an inferred part for the collection so calculate as
              // if it was all inferred and extract only the part we need
              final List<DataType> types = new ArrayList<>(3);

              DataTypeImpl.inferDataTypeFrom(field, field.getType(), types);
              atypes = atypes.clone(); // clone it since we will replace inferred
              for (int i = 0; i < atypes.length; i++) {
                if (atypes[i] == DataType.INFERRED) {
                  atypes[i] = types.get(i + 1); // since index 1 corresponds to the collection type
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
    final List<DataType> types = new ArrayList<>(3);

    DataTypeImpl.inferDataTypeFrom(field, field.getType(), types);
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
  public static boolean isAssignableFrom(DataType type, Class<?> clazz) {
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
  public static boolean isInstance(DataType type, Object object) {
    return (
      (object != null)
      ? DataTypeImpl.isAssignableFrom(type, object.getClass())
      : true  // null are always compatible
    );
  }
}

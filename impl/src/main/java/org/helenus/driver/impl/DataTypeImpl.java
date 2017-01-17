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
package org.helenus.driver.impl;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.MutableTriple;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.DataType.CollectionType;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.TypeCodec.AbstractMapCodec;
import com.datastax.driver.core.exceptions.CodecNotFoundException;
import com.google.common.reflect.TypeToken;

import org.helenus.commons.lang3.reflect.ReflectionUtils;
import org.helenus.driver.StatementBuilder;
import org.helenus.driver.codecs.ArgumentsCodec;
import org.helenus.driver.codecs.LinkedHashSetCodec;
import org.helenus.driver.codecs.MandatoryCollectionCodec;
import org.helenus.driver.codecs.MandatoryMapCodec;
import org.helenus.driver.codecs.MandatoryPairCodec;
import org.helenus.driver.codecs.MandatoryTripleCodec;
import org.helenus.driver.codecs.PairCodec;
import org.helenus.driver.codecs.SortedMapCodec;
import org.helenus.driver.codecs.SortedSetCodec;
import org.helenus.driver.codecs.TripleCodec;
import org.helenus.driver.persistence.CQLDataType;
import org.helenus.driver.persistence.Column;
import org.helenus.driver.persistence.DataType;
import org.helenus.driver.persistence.UDTEntity;
import org.helenus.driver.persistence.UDTRootEntity;

/**
 * The <code>DataTypeImpl</code> class provides definition for Cassandra data types
 * used for columns when one cannot rely on the default behavior where the type is
 * inferred from the field type.
 *
 * @copyright 2015-2017 The Helenus Driver Project Authors
 *
 * @author The Helenus Driver Project Authors
 * @version 1 - Jan 19, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
public class DataTypeImpl {
  /**
   * Finds and loads the specified class.
   * <p>
   * <i>Note:</i> This method is designed to use the thread context class loader
   * if set; otherwise it falls back to the primordial class loader.
   *
   * @author paouelle
   *
   * @param  name the name of the class to find and load
   * @return the corresponding non-<code>null</code> class
   * @throws LinkageError if the linkage fails
   * @throws ExceptionInInitializerError if the initialization provoked by this
   *         method fails
   * @throws ClassNotFoundException if the class cannot be located
   */
  public static Class<?> findClass(String name) throws ClassNotFoundException {
    final ClassLoader otccl = Thread.currentThread().getContextClassLoader();

    return (otccl != null) ? Class.forName(name, true, otccl) : Class.forName(name);
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
   * Unwraps the specific type if it is an {@link Optional}.
   *
   * @author paouelle
   *
   * @param  type the type to unwrap if required
   * @return the element of the optional class if it is an optional otherwise
   *         the type itself
   */
  public static Type unwrapOptionalIfPresent(Type type) {
    if (Optional.class.isAssignableFrom(ReflectionUtils.getRawClass(type))) {
      if (type instanceof ParameterizedType) {
        final ParameterizedType ptype = (ParameterizedType)type;
        final Type atype = ptype.getActualTypeArguments()[0]; // optional will always have 1 argument

        return atype;
      }
    }
    return type;
  }

  /**
   * Checks if at least one argument from the given list is defined to be inferred.
   *
   * @author paouelle
   *
   * @param  args the list of arguments to check
   * @return <code>true</code> if at least one argument is defined to be
   *         inferred; <code>false</code> otherwise
   */
  public static boolean anyArgumentsInferred(List<CQLDataType> args) {
    for (final CQLDataType dt: args) {
      if (dt == DataType.INFERRED) {
        return true;
      }
    }
    return false;
  }

  /**
   * The <code>Definition</code> class provides a data type definition for a CQL
   * data type.
   *
   * @copyright 2015-2017 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 19, 2015 - paouelle - Creation
   *
   * @since 1.0
   */
  public static class Definition implements CQLDataType {
    /**
     * Gets a codec for a given data type to decode to the specified type.
     *
     * @author paouelle
     *
     * @param  trace a string indicating where the codec is required
     * @param  dtype the CQL data type for which to find a codec
     * @param  type the type to decode to
     * @param  mandatory if the field is mandatory or represents a primary key
     * @param  codecRegistry the codec registry to use when finding a codec
     * @return a suitable codec for the given data type and specified type
     * @throws IllegalArgumentException if not supported
     */
    @SuppressWarnings("unchecked")
    private static TypeCodec<?> getBasicCodec(
      String trace,
      CQLDataType dtype,
      Type type,
      boolean mandatory,
      CodecRegistry codecRegistry
    ) {
      @SuppressWarnings("rawtypes")
      final TypeToken token = TypeToken.of(type);

      // first check if it is a UDT
      if (dtype instanceof UDTClassInfoImpl) {
        return ((UDTClassInfoImpl<?>)dtype).getCodec(token, codecRegistry);
      }
      try { // next try the codec registry directly
        return codecRegistry.codecFor(dtype.getDataType(), token);
      } catch (CodecNotFoundException e) { // ignore and continue with our internal ones
      } // else - oh well try to get our own codecs from our internal codec providers
      if (dtype instanceof DataType) {
        try {
          return ((DataType)dtype).codecFor(ReflectionUtils.getRawClass(type));
        } catch (CodecNotFoundException e) {
          throw new IllegalArgumentException(
            "unable to find a suitable codec to convert to: "
            + type.getTypeName()
            + " for "
            + trace,
            e
          );
        }
      }
      throw new IllegalArgumentException(
        "unsupported basic type to convert to: "
        + type.getTypeName()
        + " for "
        + trace
      );
    }

    /**
     * Gets a codec for a given data type to decode to the specified type.
     *
     * @author paouelle
     *
     * @param  trace a string indicating where the codec is required
     * @param  dtype the CQL data type for which to find a codec
     * @param  type the type to decode to
     * @param  mandatory if the field is mandatory or represents a primary key
     * @param  codecRegistry the codec registry to use when finding a codec
     * @return a suitable codec for the given data type and specified type
     * @throws IllegalArgumentException if not supported
     */
    @SuppressWarnings("unchecked")
    private static TypeCodec<?> getCodec(
      String trace,
      CQLDataType dtype,
      Type type,
      boolean mandatory,
      CodecRegistry codecRegistry
    ) {
      if (dtype instanceof Definition) {
        return ((Definition)dtype).getCodec(trace, type, mandatory, codecRegistry);
      }
      return Definition.getBasicCodec(trace, dtype, type, mandatory, codecRegistry);
    }

    /**
     * Holds the data type for this definition if not a user-defined type.
     * <p>
     * <i>Note:</i> This will either be a {@link DataType} or
     * {@link UDTClassInfoImpl}.
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
     * Holds the corresponding Datastax data type.
     *
     * @author paouelle
     */
    public final com.datastax.driver.core.DataType dtype;

    /**
     * Holds a flag indicating if this definition is frozen or not.
     * <p>
     * <i>Note:</i> Only applies to collections.
     *
     * @author paouelle
     */
    public final boolean isFrozen;

    /**
     * Instantiates a new <code>Definition</code> object.
     *
     * @author paouelle
     *
     * @param  type the non-<code>null</code> primitive data type for this
     *         definition
     * @throws IllegalArgumentException if the data type is not a primitive one
     */
    public Definition(DataType type) {
      this.type = type;
      this.arguments = Collections.emptyList();
      this.dtype = type.getDataType();
      this.isFrozen = false;
    }

    /**
     * Instantiates a new <code>Definition</code> object.
     *
     * @author paouelle
     *
     * @param  mgr the non-<code>null</code> statement manager
     * @param  type the non-<code>null</code> data type for this definition
     * @param  arguments the non-<code>null</code> list of arguments' data types
     *         if the type represents a collection (may be empty)
     * @param  isFrozen <code>true</code> if the data type is frozen; <code>false</code>
     *         otherwise
     * @throws IllegalArgumentException if the argument list is empty and the
     *         data type is not a primitive one
     */
    public Definition(
      StatementManagerImpl mgr,
      DataType type,
      List<CQLDataType> arguments,
      boolean isFrozen
    ) {
      this.type = type;
      this.arguments = arguments;
      this.isFrozen = isCollection() ? isFrozen : false;
      this.dtype = resolveDataType(mgr);
    }

    /**
     * Instantiates a new <code>Definition</code> object.
     *
     * @author paouelle
     *
     * @param  mgr the non-<code>null</code> statement manager
     * @param  types the non-<code>null</code> list of data types for this
     *         definition (the first one correspond to the data type and the
     *         remaining are the argument data types)
     * @param  isFrozen <code>true</code> if the data type is frozen; <code>false</code>
     *         otherwise
     * @throws IllegalArgumentException if remaining argument list is empty and
     *         the data type is not a primitive one
     */
    public Definition(
      StatementManagerImpl mgr,
      List<CQLDataType> types,
      boolean isFrozen
    ) {
      this.type = types.remove(0);
      this.arguments = Collections.unmodifiableList(types);
      this.isFrozen = isCollection() ? isFrozen : false;
      this.dtype = resolveDataType(mgr);
    }

    /**
     * Resolves a Cassandra data type for this data type definition.
     *
     * @author paouelle
     *
     * @param  mgr the non-<code>null</code> statement manager
     * @return a non-<code>null</code> Cassandra data type for this definition
     */
    private com.datastax.driver.core.DataType resolveDataType(
      StatementManagerImpl mgr
    ) {
      if (type.isCollection()) {
        switch (((DataType)type).NAME) {
          case LIST:
            return com.datastax.driver.core.DataType.list(
              arguments.get(0).getDataType(), isFrozen()
            );
          case SET:
            return com.datastax.driver.core.DataType.set(
              arguments.get(0).getDataType(), isFrozen()
            );
          case MAP:
            return com.datastax.driver.core.DataType.map(
              arguments.get(0).getDataType(),
              arguments.get(1).getDataType(),
              isFrozen()
            );
          default:
        }
      } else if (type.isTuple()) {
        return mgr.getCluster().getMetadata().newTupleType(
          arguments.stream()
            .map(CQLDataType::getDataType)
            .collect(Collectors.toList())
        );
      }
      return type.getDataType();
    }

    /**
     * Gets a codec for this data type to decode to the specified type.
     *
     * @author paouelle
     *
     * @param  trace a string indicating where the codec is required
     * @param  type the type to decode to
     * @param  mandatory if the field is mandatory or represents a primary key
     * @param  codecRegistry the codec registry to use when finding a codec
     * @return a suitable codec for this data type and specified type
     * @throws IllegalArgumentException if not supported
     */
    @SuppressWarnings({"synthetic-access", "unchecked", "rawtypes"})
    private TypeCodec<?> getCodec(
      String trace, Type type, boolean mandatory, CodecRegistry codecRegistry
    ) {
      final TypeToken token = TypeToken.of(type);
      final Class<?> clazz = ReflectionUtils.getRawClass(type);

      if (isCollection()) { // check if we are dealing with a collection
        if (List.class.isAssignableFrom(clazz)) {
          if (type instanceof ParameterizedType) {
            final ParameterizedType ptype = (ParameterizedType)type;
            final Type atype = ptype.getActualTypeArguments()[0]; // lists will always have 1 argument
            final TypeCodec<?> acodec = Definition.getCodec(
              trace, arguments.get(0), atype, mandatory, codecRegistry
            );
            final TypeCodec<?> codec = TypeCodec.list(acodec);

            return new ArgumentsCodec(
              !mandatory ? codec : new MandatoryCollectionCodec(codec, () -> new ArrayList<>(8)),
              acodec
            );
          }
          throw new IllegalArgumentException("unable to determine element type for " + trace);
        } else if (LinkedHashSet.class.isAssignableFrom(clazz)
                  || (Set.class.equals(clazz)
                      && (this.type.getMainType() == DataType.ORDERED_SET))) {
          if (type instanceof ParameterizedType) {
            final ParameterizedType ptype = (ParameterizedType)type;
            final Type atype = ptype.getActualTypeArguments()[0]; // sets will always have 1 argument
            final TypeCodec<?> acodec = Definition.getCodec(
              trace, arguments.get(0), atype, mandatory, codecRegistry
            );
            final TypeCodec<?> codec = new LinkedHashSetCodec((CollectionType)dtype, token, acodec);

            return new ArgumentsCodec(
              !mandatory ? codec : new MandatoryCollectionCodec(codec, () -> new LinkedHashSet<>(8)),
              acodec
            );
          }
          throw new IllegalArgumentException(
            "unable to determine element type for " + trace
          );
        } else if (SortedSet.class.isAssignableFrom(clazz)
                   || (Set.class.equals(clazz)
                       && (this.type.getMainType() == DataType.SORTED_SET))) {
          if (type instanceof ParameterizedType) {
            final ParameterizedType ptype = (ParameterizedType)type;
            final Type atype = ptype.getActualTypeArguments()[0]; // sets will always have 1 argument
            final TypeCodec<?> acodec = Definition.getCodec(
              trace, arguments.get(0), atype, mandatory, codecRegistry
            );
            final TypeCodec<?> codec = new SortedSetCodec((CollectionType)dtype, token, acodec);

            return new ArgumentsCodec(
              !mandatory ? codec : new MandatoryCollectionCodec(codec, () -> new TreeSet<>()),
              acodec
            );
          }
          throw new IllegalArgumentException(
            "unable to determine element type for " + trace
          );
        } else if (Set.class.isAssignableFrom(clazz)) {
          if (type instanceof ParameterizedType) {
            final ParameterizedType ptype = (ParameterizedType)type;
            final Type atype = ptype.getActualTypeArguments()[0]; // sets will always have 1 argument
            final TypeCodec<?> acodec = Definition.getCodec(
              trace, arguments.get(0), atype, mandatory, codecRegistry
            );
            final TypeCodec<?> codec = TypeCodec.set(acodec);

            return new ArgumentsCodec(
              !mandatory ? codec : new MandatoryCollectionCodec(codec, () -> new LinkedHashSet<>(8)),
              acodec
            );
          }
          throw new IllegalArgumentException(
            "unable to determine element type for " + trace
          );
        } else if (SortedMap.class.isAssignableFrom(clazz)
                   || (Map.class.equals(clazz)
                       && (this.type.getMainType() == DataType.SORTED_MAP))) {
          if (type instanceof ParameterizedType) {
            final ParameterizedType ptype = (ParameterizedType)type;
            final Type ktype = ptype.getActualTypeArguments()[0]; // maps will always have 2 arguments
            final TypeCodec<?> kcodec = Definition.getCodec(
              trace, arguments.get(0), ktype, mandatory, codecRegistry
            );
            final Type vtype = ptype.getActualTypeArguments()[1]; // maps will always have 2 arguments
            final TypeCodec<?> vcodec = Definition.getCodec(
              trace, arguments.get(1), vtype, mandatory, codecRegistry
            );
            final TypeCodec<?> codec = new SortedMapCodec((CollectionType)dtype, token, kcodec, vcodec);

            return new ArgumentsCodec(
              !mandatory ? codec : new MandatoryMapCodec(codec, () -> new TreeMap<>()),
              kcodec,
              vcodec
            );
          }
          throw new IllegalArgumentException(
            "unable to determine key & value type for " + trace
          );
        } else if (Map.class.isAssignableFrom(clazz)) {
          if (type instanceof ParameterizedType) {
            final ParameterizedType ptype = (ParameterizedType)type;
            final Type ktype = ptype.getActualTypeArguments()[0]; // maps will always have 2 arguments
            final Class<?> kclazz = ReflectionUtils.getRawClass(ktype);
            final TypeCodec<?> kcodec = Definition.getCodec(
              trace, arguments.get(0), ktype, mandatory, codecRegistry
            );
            final Type vtype = ptype.getActualTypeArguments()[1]; // maps will always have 2 arguments
            final TypeCodec<?> vcodec = Definition.getCodec(
              trace, arguments.get(1), vtype, mandatory, codecRegistry
            );

            if (kclazz.isEnum()) {
              // for enums, let's make sure the codec creates enum maps instead of standard maps
              final TypeCodec<?> codec = new AbstractMapCodec(kcodec, vcodec) {
                @Override
                protected Map<?, ?> newInstance(int size) {
                  return new EnumMap(kclazz);
                }
              };
              return new ArgumentsCodec(
                !mandatory ? codec : new MandatoryMapCodec(codec, () -> new EnumMap(kclazz)),
                kcodec,
                vcodec
              );
            } // else - standard maps
            final TypeCodec<?> codec = TypeCodec.map(kcodec, vcodec);

            return new ArgumentsCodec(
              !mandatory ? codec : new MandatoryMapCodec(codec, () -> new HashMap<>(8)),
              kcodec,
              vcodec
            );
          }
          throw new IllegalArgumentException(
            "unable to determine key & value type for " + trace
          );
        }
      } else if (isTuple()) {
        if (Pair.class.isAssignableFrom(clazz)) {
          if (type instanceof ParameterizedType) {
            final ParameterizedType ptype = (ParameterizedType)type;
            final Type a1type = ptype.getActualTypeArguments()[0]; // pairs will always have 2 arguments
            final Type a2type = ptype.getActualTypeArguments()[1];
            final TypeCodec<?> a1codec = Definition.getCodec(
              trace, arguments.get(0), a1type, mandatory, codecRegistry
            );
            final TypeCodec<?> a2codec = Definition.getCodec(
              trace, arguments.get(1), a2type, mandatory, codecRegistry
            );
            final TypeCodec<?> codec = new PairCodec((TupleType)dtype, token, a1codec, a2codec);

            return new ArgumentsCodec(
              !mandatory ? codec : new MandatoryPairCodec(codec, () -> new MutablePair<>()),
              a1codec,
              a2codec
            );
          }
          throw new IllegalArgumentException("unable to determine element type for " + trace);
        } else if (Triple.class.isAssignableFrom(clazz)) {
          if (type instanceof ParameterizedType) {
            final ParameterizedType ptype = (ParameterizedType)type;
            final Type a1type = ptype.getActualTypeArguments()[0]; // triples will always have 3 arguments
            final Type a2type = ptype.getActualTypeArguments()[1];
            final Type a3type = ptype.getActualTypeArguments()[2];
            final TypeCodec<?> a1codec = Definition.getCodec(
              trace, arguments.get(0), a1type, mandatory, codecRegistry
            );
            final TypeCodec<?> a2codec = Definition.getCodec(
              trace, arguments.get(1), a2type, mandatory, codecRegistry
            );
            final TypeCodec<?> a3codec = Definition.getCodec(
              trace, arguments.get(2), a3type, mandatory, codecRegistry
            );
            final TypeCodec<?> codec = new TripleCodec((TupleType)dtype, token, a1codec, a2codec, a3codec);

            return new ArgumentsCodec(
              !mandatory ? codec : new MandatoryTripleCodec(codec, () -> new MutableTriple<>()),
              a1codec,
              a2codec,
              a3codec
            );
          }
          throw new IllegalArgumentException("unable to determine element type for " + trace);
        }
      }
      return Definition.getBasicCodec(trace, this.type, type, mandatory, codecRegistry);
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
     * @see org.helenus.driver.persistence.CQLDataType#isFrozen()
     */
    @Override
    public boolean isFrozen() {
      return type.isFrozen() || (isCollection() && isFrozen);
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
      return type.isCollection();
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.persistence.CQLDataType#isTuple()
     */
    @Override
    public boolean isTuple() {
      return type.isTuple();
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
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.persistence.CQLDataType#getDataType()
     */
    @Override
    public com.datastax.driver.core.DataType getDataType() {
      return dtype;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.persistence.CQLDataType#getMainType()
     */
    @Override
    public CQLDataType getMainType() {
      return type;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.persistence.CQLDataType#getElementType()
     */
    @Override
    public CQLDataType getElementType() {
      if (arguments.size() == 0) {
        return null;
      }
      // assume the element type if the last one in the arguments list
      return arguments.get(arguments.size() - 1);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.persistence.CQLDataType#getArgumentTypes()
     */
    @Override
    public List<CQLDataType> getArgumentTypes() {
      return arguments;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.persistence.CQLDataType#getFirstArgumentType()
     */
    @Override
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
        if (isCollection() || isTuple()) { // check arguments
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
      if (isCollection() || isUserDefined() || isTuple()) {
        return false;
      }
      return type.isAlterableTo(to);
    }

    /**
     * Gets a codec for this data type to decode to the specified field.
     *
     * @author paouelle
     *
     * @param  field the field decode to its declared type
     * @param  mandatory if the field is mandatory or represents a primary key
     * @param  codecRegistry the codec registry to use when finding a codec
     * @return a suitable codec for this data type and given field
     * @throws NullPointerException if <code>field</code> or <code>codecRegistry</code>
     *         is <code>null</code>
     * @throws IllegalArgumentException if the combination of field and data types
     *         is not supported
     */
    public TypeCodec<?> getCodec(
      Field field, boolean mandatory, CodecRegistry codecRegistry
    ) {
      org.apache.commons.lang3.Validate.notNull(field, "invalid null field");
      return getCodec(
        "field: " + field.getDeclaringClass().getName() + "." + field.getName(),
        DataTypeImpl.unwrapOptionalIfPresent(field.getGenericType()),
        mandatory,
        codecRegistry
      );
    }

    /**
     * Gets a codec for this collection data type to decode to the specified class.
     *
     * @author paouelle
     *
     * @param  clazz the collection class to decode to its declared super type
     * @param  codecRegistry the codec registry to use when finding a codec
     * @return a suitable codec for this collection data type and given class
     * @throws NullPointerException if <code>clazz</code> or <code>codecRegistry</code>
     *         is <code>null</code>
     * @throws IllegalArgumentException if the combination of class and data types
     *         is not supported
     * @throws IllegalStateException if this definition is not representing a
     *         collection
     */
    @SuppressWarnings("synthetic-access")
    public TypeCodec<?> getCodec(Class<?> clazz, CodecRegistry codecRegistry) {
      org.apache.commons.lang3.Validate.notNull(clazz, "invalid null class");
      org.apache.commons.lang3.Validate.validState(isCollection(), "not a collection definition");
      return getCodec(
        "class: " + clazz.getName(),
        clazz.getGenericSuperclass(),
        true, // mandatory by design
        codecRegistry
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
      if (isCollection() || isTuple()) {
        final List<String> cqls = new ArrayList<>(arguments.size());
        final StringBuilder sb = new StringBuilder();

        for (final CQLDataType atype: arguments) {
          cqls.add(atype.toCQL());
        }
        if (isFrozen()) {
          sb.append("frozen<");
        }
        sb
          .append(type.toCQL())
          .append('<')
          .append(StringUtils.join(cqls, ','))
          .append('>');
        if (isFrozen()) {
          sb.append('>');
        }
        return sb.toString();
      }
      return type.toCQL();
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
      return arguments.stream()
        .filter(t -> t instanceof UDTClassInfoImpl)
        .map(t -> (UDTClassInfoImpl<?>)t);
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
   * Infers the data type for the specified field.
   *
   * @author paouelle
   *
   * @param  field the non-<code>null</code> field to infer the CQL data type for
   * @param  clazz the non-<code>null</code> class for which to infer the CQL
   *         data type for the field
   * @param  types the non-<code>null</code> list where to add the inferred type and
   *         its arguments
   * @param  isFrozen <code>true</code> if the data type is frozen; <code>false</code>
   *         otherwise
   * @throws IllegalArgumentException if the data type cannot be inferred from
   *         the field
   */
  private static void inferDataTypeFrom(
    Field field, Class<?> clazz, List<CQLDataType> types, boolean isFrozen
  ) {
    DataTypeImpl.inferDataTypeFrom(
      "field: " + field.getDeclaringClass().getName() + "." + field.getName(),
      field.getGenericType(),
      clazz,
      types,
      isFrozen
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
   * @param  isFrozen <code>true</code> if the data type is frozen; <code>false</code>
   *         otherwise
   * @throws IllegalArgumentException if the data type cannot be inferred from
   *         the class
   */
  private static void inferDataTypeFrom(
    Class<?> clazz, List<CQLDataType> types, boolean isFrozen
  ) {
    // at this point we are trying to infer the type from the superclass in
    // order to generate a fake field, so make sure to continue on with the
    // class super class
    DataTypeImpl.inferDataTypeFrom(
      "class: " + clazz.getName(),
      clazz.getGenericSuperclass(),
      clazz.getSuperclass(),
      types,
      isFrozen
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
   * @param  isFrozen <code>true</code> if the data type is frozen; <code>false</code>
   *         otherwise
   * @throws IllegalArgumentException if the data type cannot be inferred from
   *         the field or class
   */
  private static void inferDataTypeFrom(
    String trace,
    Type type,
    Class<?> clazz,
    List<CQLDataType> types,
    boolean isFrozen
  ) {
    // check if the class is annotated as a UDT in which case we kind of allow
    // collections of collections since clazz is actually a udt
    final boolean isUDT = (
      (clazz.getAnnotation(UDTEntity.class) != null)
      || (ReflectionUtils.findFirstClassAnnotatedWith(clazz, UDTRootEntity.class) != null)
    );

    if (Optional.class.isAssignableFrom(clazz)) {
      org.apache.commons.lang3.Validate.isTrue(
        !isUDT && types.isEmpty(),
        "collection of optionals is not supported in %s",
        trace
      );
      if (type instanceof ParameterizedType) {
        final ParameterizedType ptype = (ParameterizedType)type;
        final Type atype = ptype.getActualTypeArguments()[0]; // optional will always have 1 argument

        DataTypeImpl.inferDataTypeFrom(trace, atype, ReflectionUtils.getRawClass(atype), types, true);
        return;
      }
    } else if (List.class.isAssignableFrom(clazz)) {
      if (isUDT) {
        // we need to this because a UDT can actually implement List and in such case,
        // we want to return its UDT type and not a list
        DataTypeImpl.inferBasicDataTypeFrom(trace, clazz, types);
        return;
      }
      if (type instanceof ParameterizedType) {
        final ParameterizedType ptype = (ParameterizedType)type;
        final Type atype = ptype.getActualTypeArguments()[0]; // lists will always have 1 argument

        types.add(DataType.LIST);
        DataTypeImpl.inferDataTypeFrom(trace, atype, ReflectionUtils.getRawClass(atype), types, true);
        return;
      }
    } else if (Set.class.isAssignableFrom(clazz)) {
      if (isUDT) {
        // we need to this because a UDT can actually implement Set and in such case,
        // we want to return its UDT type and not a set
        DataTypeImpl.inferBasicDataTypeFrom(trace, clazz, types);
        return;
      }
      if (type instanceof ParameterizedType) {
        final ParameterizedType ptype = (ParameterizedType)type;
        final Type atype = ptype.getActualTypeArguments()[0]; // sets will always have 1 argument

        types.add(LinkedHashSet.class.isAssignableFrom(clazz) ? DataType.ORDERED_SET : DataType.SET);
        DataTypeImpl.inferDataTypeFrom(trace, atype, ReflectionUtils.getRawClass(atype), types, true);
        return;
      }
    } else if (SortedMap.class.isAssignableFrom(clazz)) {
      if (isUDT) {
        // we need to this because a UDT can actually implement Map and in such case,
        // we want to return its UDT type and not a map
        DataTypeImpl.inferBasicDataTypeFrom(trace, clazz, types);
        return;
      }
      if (type instanceof ParameterizedType) {
        final ParameterizedType ptype = (ParameterizedType)type;
        final Type ktype = ptype.getActualTypeArguments()[0]; // maps will always have 2 arguments
        final Type vtype = ptype.getActualTypeArguments()[1]; // maps will always have 2 arguments

        types.add(DataType.SORTED_MAP);
        DataTypeImpl.inferDataTypeFrom(trace, ktype, ReflectionUtils.getRawClass(ktype), types, true);
        DataTypeImpl.inferDataTypeFrom(trace, vtype, ReflectionUtils.getRawClass(vtype), types, true);
        return;
      }
    } else if (Map.class.isAssignableFrom(clazz)) {
      if (isUDT) {
        // we need to this because a UDT can actually implement Map and in such case,
        // we want to return its UDT type and not a map
        DataTypeImpl.inferBasicDataTypeFrom(trace, clazz, types);
        return;
      }
      if (type instanceof ParameterizedType) {
        final ParameterizedType ptype = (ParameterizedType)type;
        final Type ktype = ptype.getActualTypeArguments()[0]; // maps will always have 2 arguments
        final Type vtype = ptype.getActualTypeArguments()[1]; // maps will always have 2 arguments

        types.add(DataType.MAP);
        // don't consider the @Persister annotation for the key
        DataTypeImpl.inferDataTypeFrom(trace, ktype, ReflectionUtils.getRawClass(ktype), types, true);
        DataTypeImpl.inferDataTypeFrom(trace, vtype, ReflectionUtils.getRawClass(vtype), types, true);
        return;
      }
    } else if (Pair.class.isAssignableFrom(clazz)) {
      if (type instanceof ParameterizedType) {
        final ParameterizedType ptype = (ParameterizedType)type;
        final Type a1type = ptype.getActualTypeArguments()[0]; // pairs will always have 2 arguments
        final Type a2type = ptype.getActualTypeArguments()[1];

        types.add(DataType.TUPLE);
        DataTypeImpl.inferDataTypeFrom(trace, a1type, ReflectionUtils.getRawClass(a1type), types, true);
        DataTypeImpl.inferDataTypeFrom(trace, a2type, ReflectionUtils.getRawClass(a2type), types, true);
        return;
      }
    } else if (Triple.class.isAssignableFrom(clazz)) {
      if (type instanceof ParameterizedType) {
        final ParameterizedType ptype = (ParameterizedType)type;
        final Type a1type = ptype.getActualTypeArguments()[0]; // triples will always have 3 arguments
        final Type a2type = ptype.getActualTypeArguments()[1];
        final Type a3type = ptype.getActualTypeArguments()[2];

        types.add(DataType.TUPLE);
        DataTypeImpl.inferDataTypeFrom(trace, a1type, ReflectionUtils.getRawClass(a1type), types, true);
        DataTypeImpl.inferDataTypeFrom(trace, a2type, ReflectionUtils.getRawClass(a2type), types, true);
        DataTypeImpl.inferDataTypeFrom(trace, a3type, ReflectionUtils.getRawClass(a3type), types, true);
        return;
      }
    }
    DataTypeImpl.inferBasicDataTypeFrom(trace, clazz, types);
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
   * @throws IllegalArgumentException if the data type cannot be inferred from
   *         the field
   */
  private static void inferBasicDataTypeFrom(
    String trace, Class<?> clazz, List<CQLDataType> types
  ) {
    final DataType dtype = DataType.valueOf(clazz);

    if (dtype != null) {
      types.add(dtype);
    } else {
      // check if it is a user-defined type (possibly defining it if not defined yet)
      try {
        final ClassInfoImpl<?> cinfo
          = (ClassInfoImpl<?>)StatementBuilder.getClassInfo(clazz);

        org.apache.commons.lang3.Validate.isTrue(
          cinfo instanceof UDTClassInfoImpl,
          "unable to infer data type in %s",
          trace
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
   * @param  mgr the non-<code>null</code> statement manager
   * @param  field the field from which to infer the data type
   * @param  isFrozen <code>true</code> if the data type is frozen; <code>false</code>
   *         otherwise
   * @return a non-<code>null</code> data type definition
   * @throws NullPointerException if <code>field</code> is <code>null</code>
   * @throws IllegalArgumentException if the data type cannot be inferred from
   *         the field or it is persisted but the persister cannot be instantiate
   */
  public static Definition inferDataTypeFrom(
    StatementManagerImpl mgr, Field field, boolean isFrozen
  ) {
    org.apache.commons.lang3.Validate.notNull(field, "invalid null field");
    final Column.Data cdata = field.getAnnotation(Column.Data.class);

    if (cdata != null) {
      final List<CQLDataType> itypes = new ArrayList<>(3); // full set o inferred type if processed
      final List<CQLDataType> atypes
        = new ArrayList<>(Arrays.asList(cdata.arguments()));
      DataType type = cdata.type();

      if (!atypes.isEmpty() && (type == DataType.INFERRED)) {
        // we have an inferred type for the collection or tuple so calculate as
        // if it was all inferred and extract only the part we need
        DataTypeImpl.inferDataTypeFrom(field, field.getType(), itypes, isFrozen);
        org.apache.commons.lang3.Validate.isTrue(
          itypes.get(0) instanceof DataType,
          "missing data type value in field: %s.%s",
          field.getDeclaringClass().getName(),
          field.getName()
        );
        type = (DataType)itypes.get(0);
      }
      if (type != DataType.INFERRED) {
        if (type.NUM_ARGUMENTS == -1) { // variable # of args
          org.apache.commons.lang3.Validate.isTrue(
            !(atypes.size() <= 0),
            "missing argument data type(s) for '%s' in field: %s.%s",
            type.CQL,
            field.getDeclaringClass().getName(),
            field.getName()
          );
        } else {
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
        }
        if (type.NUM_ARGUMENTS != 0) {
          if (DataTypeImpl.anyArgumentsInferred(atypes)) {
            // we have an inferred part for the collection/tuple so calculate as
            // if it was all inferred and extract only the part we need
            if (itypes.isEmpty()) { // only do it if not already done!!!
              DataTypeImpl.inferDataTypeFrom(field, field.getType(), itypes, isFrozen);
            }
            for (int i = 0; i < atypes.size(); i++) {
              if (atypes.get(i) == DataType.INFERRED) {
                atypes.set(i, itypes.get(i + 1)); // since index 1 corresponds to the collection type
              }
            }
          }
        }
        // TODO: should probably check that the CQL specified matches a supported class for it
        return new Definition(mgr, type, atypes, isFrozen);
      } // else - the type was either not inferred
      // only type supported here is either INFERRED which falls through as if
      // no data type specified or BLOB
      if (type == DataType.BLOB) {
        return new Definition(DataType.BLOB);
      }
      org.apache.commons.lang3.Validate.isTrue(
        type == DataType.INFERRED,
        "unsupported data type '%s' in annotated field: %s.%s",
        type.CQL,
        field.getDeclaringClass().getName(),
        field.getName()
      );
    } // else - no Column.Data specified so infer completely
    final List<CQLDataType> types = new ArrayList<>(3);

    DataTypeImpl.inferDataTypeFrom(field, field.getType(), types, isFrozen);
    return new Definition(mgr, types, isFrozen);
  }

  /**
   * Infers the data type from the specified class' superclass.
   *
   * @author paouelle
   *
   * @param  mgr the non-<code>null</code> statement manager
   * @param  type the collection type being inferred
   * @param  isFrozen <code>true</code> if the data type is frozen; <code>false</code>
   *         otherwise
   * @param  clazz the class from which to infer the data type of its superclass
   * @return a non-<code>null</code> data type definition
   * @throws NullPointerException if <code>type</code> or <code>clazz</code> is
   *         <code>null</code>
   * @throws IllegalArgumentException if the argument data type cannot be inferred
   *         from the superclass or the specified type is not a collection data type
   */
  public static Definition inferDataTypeFrom(
    StatementManagerImpl mgr, DataType type, boolean isFrozen, Class<?> clazz
  ) {
    org.apache.commons.lang3.Validate.notNull(type, "invalid null type");
    org.apache.commons.lang3.Validate.notNull(clazz, "invalid null class");
    org.apache.commons.lang3.Validate.isTrue(
      type.isCollection(),
      "data type '%s' is not a collection in class: %s",
      type.CQL,
      clazz.getName()
    );
    final UDTEntity.Data cdata = clazz.getAnnotation(UDTEntity.Data.class);

    if (cdata != null) { // we were not asked to infer the argument type
      final List<CQLDataType> atypes
        = new ArrayList<>(Arrays.asList(cdata.arguments()));

      if (type.NUM_ARGUMENTS == -1) { // variable # of args
        org.apache.commons.lang3.Validate.isTrue(
          !(atypes.size() <= 0),
          "missing argument data type(s) for '%s' in class: %s",
          type.CQL,
          clazz.getName()
        );
      } else {
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
      }
      if (type.NUM_ARGUMENTS != 0) {
        if (DataTypeImpl.anyArgumentsInferred(atypes)) {
          // we have an inferred part for the collection so calculate as
          // if it was all inferred and extract only the part we need
          final List<CQLDataType> types = new ArrayList<>(3);

          DataTypeImpl.inferDataTypeFrom(clazz, types, isFrozen);
          for (int i = 0; i < atypes.size(); i++) {
            if (atypes.get(i) == DataType.INFERRED) {
              atypes.set(i, types.get(i + 1)); // since index 1 corresponds to the collection type
            }
          }
        }
      }
      return new Definition(mgr, type, atypes, isFrozen);
    }
    // if we get here then the type was either not inferred or there was no
    // UDTEntity.Data annotation
    final List<CQLDataType> types = new ArrayList<>(3);

    DataTypeImpl.inferDataTypeFrom(clazz, types, isFrozen);
    return new Definition(mgr, types, isFrozen);
  }
}

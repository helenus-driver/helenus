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

import java.util.HashMap;
import java.util.Map;

import java.nio.ByteBuffer;

import org.apache.commons.lang3.StringUtils;

import com.datastax.driver.core.MetadataBridge;
import com.datastax.driver.core.ParseUtils;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.datastax.driver.core.UserTypeBridge;
import com.datastax.driver.core.exceptions.InvalidTypeException;

import org.helenus.driver.ObjectConversionException;

/**
 * The <code>UDTCodecImpl</code> class provides a suitable codec for UDT types
 * based on a given class info.
 *
 * @copyright 2015-2017 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 19, 2017 - paouelle - Creation
 *
 * @param <T> The type of POJO represented by this codec
 *
 * @since 3.0
 */
public class UDTCodecImpl<T> extends TypeCodec<T> {
  /**
   * Holds the class info for the udt.
   *
   * @author paouelle
   */
  private final UDTClassInfoImpl<T> cinfo;

  /**
   * Holds the keyspace for this codec.
   *
   * @author paouelle
   */
  private final String keyspace;

  /**
   * Holds the internal codec.
   *
   * @author paouelle
   */
  private volatile TypeCodec<UDTValue> icodec;

  /**
   * Holds a flag indicating if the definition was registered with the cluster.
   *
   * @author paouelle
   */
  private volatile boolean registered;

  /**
   * Instantiates a new default <code>UDTCodecImpl</code> object.
   *
   * @author paouelle
   *
   * @param cinfo the udt class info
   * @param keyspace the keyspace for which to create a codec
   */
  public UDTCodecImpl(UDTClassInfoImpl<T> cinfo, String keyspace) {
    super(
      UserTypeBridge.instantiate(cinfo.mgr, cinfo, keyspace),
      cinfo.getObjectClass()
    );
    this.cinfo = cinfo;
    this.keyspace = keyspace;
    this.icodec = TypeCodec.userType(getUserType());
    this.registered = false;
  }

  /**
   * Instantiates a new <code>UDTCodecImpl</code> object.
   *
   * @author paouelle
   *
   * @param cinfo the udt class info
   * @param definition the registered user type definition to use when decoding
   */
  public UDTCodecImpl(UDTClassInfoImpl<T> cinfo, UserType definition) {
    super(definition, cinfo.getObjectClass());
    this.cinfo = cinfo;
    this.icodec = TypeCodec.userType(definition);
    this.keyspace = definition.getKeyspace();
    this.registered = true;
  }

  /**
   * Gets a {@link UDTValue} corresponding to the given object based on the
   * user defined type for this codec.
   *
   * @author paouelle
   *
   * @param  object the pojo object from which to create a user-defined value
   * @return the {@link UDTValue} corresponding to the specified pojo
   * @throws IllegalArgumentException if the combination of fields and data types
   *         is not supported
   */
  @SuppressWarnings("unchecked")
  private UDTValue getUDTValue(T object) {
    if (object == null) {
      return null;
    }
    // get the table for this UDT
    final TableInfoImpl<T> table = cinfo.getTableImpl();

    if (table == null) {
      return null;
    }
    final UDTValue uval = getUserType().newValue();
    int i = -1;

    for (final UserType.Field coldef: uval.getType()) {
      i++;
      // find the field in the table for this column
      final String cname = coldef.getName();
      final FieldInfoImpl<T> field = table.getColumnImpl(cname);

      if (field != null) {
        // now let's set the value for this column
        uval.set(i, field.getValue(object), (TypeCodec<Object>)field.getCodec(keyspace));
      }
    }
    return uval;
  }

  /**
   * Gets the user type for this codec.
   *
   * @author paouelle
   *
   * @return the user type for this codec
   */
  protected UserType getUserType() {
    return (UserType)getCqlType();
  }

  /**
   * Registers a new cluster-defined definition for the user type being decoded
   * by this codec.
   *
   * @author paouelle
   *
   * @param  definition the new definition for the user type to register
   * @throws IllegalArgumentException if the definition doesn't correspond to
   *         this one
   */
  protected void register(UserType definition) {
    final UserType old = getUserType();

    org.apache.commons.lang3.Validate.isTrue(
      old.getTypeName().equals(definition.getTypeName()),
      "incompatible udt definition 's type registered: %s; expecting: %s",
      definition.getTypeName(),
      old.getTypeName()
    );
    org.apache.commons.lang3.Validate.isTrue(
      old.getKeyspace().equals(definition.getKeyspace()),
      "incompatible udt definition's keyspace registered: %s; expecting: %s",
      definition.getKeyspace(),
      old.getKeyspace()
    );
    this.icodec = TypeCodec.userType(definition);
    this.registered = true;
  }

  /**
   * Deregisters the current definition from this codec as it was removed from
   * the cluster.
   *
   * @author paouelle
   *
   * @param  definition the definition for the user type to deregister
   * @throws IllegalArgumentException if the definition doesn't correspond to
   *         this one
   */
  protected void deregister(UserType definition) {
    final UserType old = getUserType();

    org.apache.commons.lang3.Validate.isTrue(
      old.getTypeName().equals(definition.getTypeName()),
      "incompatible udt definition 's type deregistered: %s; expecting: %s",
      definition.getTypeName(),
      old.getTypeName()
    );
    org.apache.commons.lang3.Validate.isTrue(
      old.getKeyspace().equals(definition.getKeyspace()),
      "incompatible udt definition's keyspace deregistered: %s; expecting: %s",
      definition.getKeyspace(),
      old.getKeyspace()
    );
    this.registered = false;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.datastax.driver.core.TypeCodec#parse(java.lang.String)
   */
  @Override
  public T parse(String value) throws InvalidTypeException {
    if (StringUtils.isEmpty(value) || value.equals("NULL")) {
      return null;
    }
    final TableInfoImpl<T> table = cinfo.getTableImpl();
    final Map<String, String> values = new HashMap<>(cinfo.getColumns().size() * 3 / 2);
    int i = ParseUtils.skipSpaces(value, 0);

    if (value.charAt(i++) != '{') {
      throw new InvalidTypeException(
        "cannot parse '"
        + keyspace
        + "."
        + cinfo.getName()
        + "' value from \""
        + value
        + "\", at character "
        + i
        + " expecting '{' but got '"
        + value.charAt(i)
        + "'"
      );
    }
    i = ParseUtils.skipSpaces(value, i);
    if (value.charAt(i) == '}') {
      try {
        return cinfo.getObject(keyspace, values);
      } catch (ObjectConversionException e) {
        throw new InvalidTypeException(
          "failed to parse '"
          + keyspace
          + "."
          + cinfo.getName()
          + "'; "
          + e.getMessage(), e
        );
      }
    }
    while (i < value.length()) {
      int n;

      try {
        n = ParseUtils.skipCQLId(value, i);
      } catch (IllegalArgumentException e) {
        throw new InvalidTypeException(
          "cannot parse '"
          + keyspace
          + "."
          + cinfo.getName()
          + "' value from \""
          + value
          + "\", cannot parse a CQL identifier at character "
          + i, e
        );
      }
      final String name = value.substring(i, n);

      i = n;
      final FieldInfoImpl<T> finfo = table.getColumnImpl(name);

      if (finfo == null) {
        throw new InvalidTypeException(
          "unknown field "
          + name
          + " in value \""
          + value
          + "\""
        );
      }
      i = ParseUtils.skipSpaces(value, i);
      if (value.charAt(i++) != ':') {
        throw new InvalidTypeException(
          "cannot parse '"
          + keyspace
          + "."
          + cinfo.getName()
          + "' value from \""
          + value
          + "\", at character "
          + i
          + " expecting ':' but got '"
          + value.charAt(i)
          + "'"
        );
      }
      i = ParseUtils.skipSpaces(value, i);
      try {
        n = ParseUtils.skipCQLValue(value, i);
      } catch (IllegalArgumentException e) {
        throw new InvalidTypeException(
          "cannot parse '"
          + keyspace
          + "."
          + cinfo.getName()
          + "' value from \""
          + value
          + "\", invalid CQL value at character "
          + i, e
        );
      }
      final String input = value.substring(i, n);

      values.put(name, input);
      i = n;
      i = ParseUtils.skipSpaces(value, i);
      if (value.charAt(i) == '}') {
        try {
          return cinfo.getObject(keyspace, values);
        } catch (ObjectConversionException e) {
          throw new InvalidTypeException(
            "failed to parse '"
            + keyspace
            + "."
            + cinfo.getName()
            + "'; "
            + e.getMessage(), e
          );
        }
      }
      if (value.charAt(i) != ',') {
        throw new InvalidTypeException(
          "cannot parse '"
          + keyspace
          + "."
          + cinfo.getName()
          + "' value from \""
          + value
          + "\", at character "
          + i
          + " expecting ',' but got '"
          + value.charAt(i)
          + "'"
        );
      }
      ++i; // skip ','
      i = ParseUtils.skipSpaces(value, i);
    }
    throw new InvalidTypeException(
      "Malformed '"
      + cinfo.getName()
      + "' value \""
      + value
      + "\", missing closing '}'"
    );
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.datastax.driver.core.TypeCodec#format(java.lang.Object)
   */
  @SuppressWarnings("unchecked")
  @Override
  public String format(T value) throws InvalidTypeException {
    if (value == null) {
      return "NULL";
    }
    final TableInfoImpl<T> table = cinfo.getTableImpl();
    final StringBuilder sb = new StringBuilder(200);
    int i = 0;

    sb.append('{');
    for (UserType.Field field: (UserType)icodec.getCqlType()) {
      final String cname = MetadataBridge.escapeId(field.getName());
      if (i > 0) {
        sb.append(',');
      }
      sb.append(cname).append(':');
      final FieldInfoImpl<T> finfo = table.getColumnImpl(cname);

      if (finfo != null) {
        sb.append(((TypeCodec<Object>)finfo.getCodec(keyspace)).format(finfo.getValue(value)));
      } else {
        sb.append("NULL");
      }
      i += 1;
    }
    sb.append('}');
    return sb.toString();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.datastax.driver.core.TypeCodec#serialize(java.lang.Object, com.datastax.driver.core.ProtocolVersion)
   */
  @Override
  public ByteBuffer serialize(T value, ProtocolVersion protocolVersion)
    throws InvalidTypeException {
    if (!registered) {
      throw new InvalidTypeException(
        "failed to serialize '"
        + keyspace
        + "."
        + cinfo.getName()
        + "'; no cluster-defined definition registered"
      );
    }
    return icodec.serialize(getUDTValue(value), protocolVersion);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.datastax.driver.core.TypeCodec#deserialize(java.nio.ByteBuffer, com.datastax.driver.core.ProtocolVersion)
   */
  @Override
  public T deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion)
    throws InvalidTypeException {
    if (!registered) {
      throw new InvalidTypeException(
        "failed to deserialize '"
        + keyspace
        + "."
        + cinfo.getName()
        + "'; no cluster-defined definition registered"
      );
    }
    try {
      return cinfo.getObject(icodec.deserialize(bytes, protocolVersion));
    } catch (ObjectConversionException e) {
      throw new InvalidTypeException(
        "failed to deserialize '"
        + keyspace
        + "."
        + cinfo.getName()
        + "'; "
        + e.getMessage(), e
      );
    }
  }
}

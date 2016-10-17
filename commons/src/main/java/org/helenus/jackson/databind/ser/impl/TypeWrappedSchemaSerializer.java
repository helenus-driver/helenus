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
package org.helenus.jackson.databind.ser.impl;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonFormatVisitorWrapper;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.ser.impl.TypeWrappedSerializer;

/**
 * The <code>TypeWrappedSchemaSerializer</code> class extends on
 * {@link TypeWrappedSerializer} to provide support for Json schemas.
 *
 * @copyright 2015-2016 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Sep 15, 2016 - paouelle - Creation
 *
 * @since 1.0
 */
public class TypeWrappedSchemaSerializer extends JsonSerializer<Object> {
  /**
   * Holds the type serializer being wrapped.
   *
   * @author paouelle
   */
  private final TypeSerializer typeSerializer;

  /**
   * Holds the value serializer being wrapped.
   *
   * @author paouelle
   */
  private final JsonSerializer<Object> serializer;

  /**
   * Instantiates a new <code>TypeWrappedSchemaSerializer</code> object.
   *
   * @author paouelle
   *
   * @param typeSer the type serializer being wrapped
   * @param ser the value serializer being wrapped
   */
  @SuppressWarnings("unchecked")
  public TypeWrappedSchemaSerializer(TypeSerializer typeSer, JsonSerializer<?> ser) {
    this.typeSerializer = typeSer;
    this.serializer = (JsonSerializer<Object>) ser;
  }

  /**
   * Instantiates a new <code>TypeWrappedSchemaSerializer</code> object.
   *
   * @author paouelle
   *
   * @param ser the type wrapped serializer being replaced
   */
  @SuppressWarnings("unchecked")
  public TypeWrappedSchemaSerializer(TypeWrappedSerializer ser) {
    this.typeSerializer = ser.typeSerializer();
    this.serializer = ser.valueSerializer();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.fasterxml.jackson.databind.JsonSerializer#serialize(java.lang.Object, com.fasterxml.jackson.core.JsonGenerator, com.fasterxml.jackson.databind.SerializerProvider)
   */
  @Override
  public void serialize(
    Object value, JsonGenerator jgen, SerializerProvider provider
  ) throws IOException {
    serializer.serializeWithType(value, jgen, provider, typeSerializer);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.fasterxml.jackson.databind.JsonSerializer#serializeWithType(java.lang.Object, com.fasterxml.jackson.core.JsonGenerator, com.fasterxml.jackson.databind.SerializerProvider, com.fasterxml.jackson.databind.jsontype.TypeSerializer)
   */
  @Override
  public void serializeWithType(
    Object value, JsonGenerator jgen,
    SerializerProvider provider,
    TypeSerializer typeSer
  ) throws IOException {
    /* Is this an erroneous call? For now, let's assume it is not, and
     * that type serializer is just overridden if so
     */
    serializer.serializeWithType(value, jgen, provider, typeSer);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.fasterxml.jackson.databind.JsonSerializer#acceptJsonFormatVisitor(com.fasterxml.jackson.databind.jsonFormatVisitors.JsonFormatVisitorWrapper, com.fasterxml.jackson.databind.JavaType)
   */
  @Override
  public void acceptJsonFormatVisitor(
    JsonFormatVisitorWrapper visitor,
    JavaType type
  ) throws JsonMappingException {
    serializer.acceptJsonFormatVisitor(visitor, type);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.fasterxml.jackson.databind.JsonSerializer#handledType()
   */
  @Override
  public Class<Object> handledType() {
    return Object.class;
  }

  /**
   * Gets the value serializer.
   *
   * @author paouelle
   *
   * @return the value serializer
   */
  public JsonSerializer<Object> valueSerializer() {
    return serializer;
  }

  /**
   * Gets the type serializer.
   *
   * @author paouelle
   *
   * @return the type serializer
   */
  public TypeSerializer typeSerializer() {
    return typeSerializer;
  }
}

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
package org.helenus.jackson.databind.ser;

import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.DefaultSerializerProvider;
import com.fasterxml.jackson.databind.ser.SerializerFactory;
import com.fasterxml.jackson.databind.ser.impl.TypeWrappedSerializer;

import org.helenus.jackson.databind.ser.impl.TypeWrappedSchemaSerializer;

/**
 * The <code>DefaultSchemaSerializerProvider</code> class extends on
 * {@link DefaultSerializerProvider} to provide support for Json schemas since
 * the {@link TypeWrappedSerializer} class automatically created by the
 * {@link SerializerProvider#findTypedValueSerializer} methods doesn't properly
 * handle schemas for referenced types.
 *
 * @copyright 2015-2016 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Sep 15, 2016 - paouelle - Creation
 *
 * @since 1.0
 */
public class DefaultSchemaSerializerProvider extends DefaultSerializerProvider {
  /**
   * Holds the serialVersionUID.
   *
   * @author paouelle
   */
  private static final long serialVersionUID = 120584867376992720L;

  /**
   * Instantiates a new <code>DefaultSchemaSerializerProvider</code> object.
   *
   * @author paouelle
   */
  public DefaultSchemaSerializerProvider() {}

  /**
   * Instantiates a new <code>DefaultSchemaSerializerProvider</code> object.
   *
   * @author paouelle
   *
   * @param src the serializer provider to copy
   */
  protected DefaultSchemaSerializerProvider(DefaultSchemaSerializerProvider src) {
    super(src);
  }

  /**
   * Instantiates a new <code>DefaultSchemaSerializerProvider</code> object.
   *
   * @author paouelle
   *
   * @param src blueprint object used as the baseline for this instance
   * @param config the serialization config
   * @param f the serializer factory
   */
  protected DefaultSchemaSerializerProvider(
    SerializerProvider src,
    SerializationConfig config,
    SerializerFactory f
  ) {
    super(src, config, f);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.fasterxml.jackson.databind.ser.DefaultSerializerProvider#copy()
   */
  @Override
  public DefaultSerializerProvider copy() {
    if (getClass() != DefaultSchemaSerializerProvider.class) {
      return super.copy();
    }
    return new DefaultSchemaSerializerProvider(this);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.fasterxml.jackson.databind.ser.DefaultSerializerProvider#createInstance(com.fasterxml.jackson.databind.SerializationConfig, com.fasterxml.jackson.databind.ser.SerializerFactory)
   */
  @Override
  public DefaultSchemaSerializerProvider createInstance(SerializationConfig config, SerializerFactory jsf) {
    return new DefaultSchemaSerializerProvider(this, config, jsf);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.fasterxml.jackson.databind.SerializerProvider#findTypedValueSerializer(java.lang.Class, boolean, com.fasterxml.jackson.databind.BeanProperty)
   */
  @Override
  public JsonSerializer<Object> findTypedValueSerializer(
    Class<?> valueType, boolean cache, BeanProperty property
  ) throws JsonMappingException {
    JsonSerializer<Object> s = super.findTypedValueSerializer(valueType, cache, property);

    if (s instanceof TypeWrappedSerializer) {
      s = new TypeWrappedSchemaSerializer((TypeWrappedSerializer)s);
      if (cache) { // re-cache our overridden version
        _serializerCache.addTypedSerializer(valueType, s);
      }
    }
    return s;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.fasterxml.jackson.databind.SerializerProvider#findTypedValueSerializer(com.fasterxml.jackson.databind.JavaType, boolean, com.fasterxml.jackson.databind.BeanProperty)
   */
  @Override
  public JsonSerializer<Object> findTypedValueSerializer(
    JavaType valueType, boolean cache, BeanProperty property
  ) throws JsonMappingException {
    JsonSerializer<Object> s = super.findTypedValueSerializer(valueType, cache, property);

    if (s instanceof TypeWrappedSerializer) {
      s = new TypeWrappedSchemaSerializer((TypeWrappedSerializer)s);
      if (cache) { // re-cache our overridden version
        _serializerCache.addTypedSerializer(valueType, s);
      }
    }
    return s;
  }
}

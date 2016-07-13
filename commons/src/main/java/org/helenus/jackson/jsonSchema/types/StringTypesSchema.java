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
package org.helenus.jackson.jsonSchema.types;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import java.time.ZoneId;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.type.SimpleType;
import com.fasterxml.jackson.module.jsonSchema.types.ReferenceSchema;
import com.fasterxml.jackson.module.jsonSchema.types.StringSchema;

/**
 * The <code>StringTypesSchema</code> class extends the {@link StringSchema}
 * class to provide additional information about the Java type being referenced
 * as a string.
 *
 * @copyright 2015-2016 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jun 27, 2016 - paouelle - Creation
 *
 * @since 1.0
 */
public class StringTypesSchema extends StringSchema {
  /**
   * Holds the type or base type associated with the string schema.
   *
   * @author paouelle
   */
  @JsonIgnore
  private JavaType javaType;

  /**
   * Holds the type or sub-types associated with the string schema.
   *
   * @author paouelle
   */
  @JsonIgnore
  private final Set<JavaType> javaTypes;

  /**
   * Instantiates a new <code>StringTypesSchema</code> object.
   *
   * @author paouelle
   *
   * @param schema the reference schema for which to create a string one
   * @param jtype the corresponding Java type
   */
  public StringTypesSchema(ReferenceSchema schema, JavaType jtype) {
    Set<JavaType> jtypes = null;

    // ReferenceTypesSchema
    if (schema instanceof ReferenceTypesSchema) {
      this.javaType = ((ReferenceTypesSchema)schema).getJavaType();
      jtypes = ((ReferenceTypesSchema)schema).getJavaTypes();
    }
    if (javaType == null) {
      if (Optional.class.isAssignableFrom(jtype.getRawClass())) {
        jtype = jtype.containedType(0);
      }
      this.javaType = jtype;
      jtypes = ReferenceTypesSchema.getJsonSubTypesFrom(jtype.getRawClass())
        .map(c -> SimpleType.construct(c))
        .collect(Collectors.toCollection(LinkedHashSet::new));
      if (jtypes.isEmpty()) {
        jtypes.add(jtype);
      }
    }
    this.javaTypes = jtypes;
    // JsonSchema
    setId(schema.getId());
    set$ref(schema.get$ref());
    set$schema(schema.get$schema());
    setDisallow(schema.getDisallow());
    setExtends(schema.getExtends());
    setRequired(schema.getRequired());
    setReadonly(schema.getReadonly());
    setDescription(schema.getDescription());
    // SimpleTypeSchema
    setDefault(schema.getDefault());
    setTitle(schema.getTitle());
    setPathStart(schema.getPathStart());
    setLinks(schema.getLinks());
  }

  /**
   * Instantiates a new <code>StringTypesSchema</code> object.
   *
   * @author paouelle
   *
   * @param schema the string schema for which to create a string one
   * @param jtype the corresponding Java type
   */
  public StringTypesSchema(StringSchema schema, JavaType jtype) {
    Set<JavaType> jtypes = null;

    // ReferenceTypesSchema
    if (schema instanceof StringTypesSchema) {
      this.javaType = ((StringTypesSchema)schema).getJavaType();
      jtypes = ((StringTypesSchema)schema).getJavaTypes();
    }
    if (javaType == null) {
      if (jtype == null) {
        jtypes = Collections.emptySet();
      } else {
        if (Optional.class.isAssignableFrom(jtype.getRawClass())) {
          jtype = jtype.containedType(0);
        }
        this.javaType = jtype;
        jtypes = ReferenceTypesSchema.getJsonSubTypesFrom(jtype.getRawClass())
          .map(c -> SimpleType.construct(c))
          .collect(Collectors.toCollection(LinkedHashSet::new));
        if (jtypes.isEmpty()) {
          jtypes.add(jtype);
        }
      }
    }
    this.javaTypes = jtypes;
    // JsonSchema
    setId(schema.getId());
    set$ref(schema.get$ref());
    set$schema(schema.get$schema());
    setDisallow(schema.getDisallow());
    setExtends(schema.getExtends());
    setRequired(schema.getRequired());
    setReadonly(schema.getReadonly());
    setDescription(schema.getDescription());
    // SimpleTypeSchema
    setDefault(schema.getDefault());
    setTitle(schema.getTitle());
    setPathStart(schema.getPathStart());
    setLinks(schema.getLinks());
    // ValuetypeSchema
    getEnums().addAll(schema.getEnums());
    setFormat(schema.getFormat());
    // StringSchema
    setMaxLength(schema.getMaxLength());
    setMinLength(schema.getMinLength());
    setPattern(schema.getPattern());
    // ----
    if (jtype != null) {
      if (Locale.class.isAssignableFrom(jtype.getRawClass())) {
        setPattern("^([a-zA-Z]{2,8}(_[a-zA-Z]{2}|[0-9]{3})?([-_]([0-9][0-9a-zA-Z]{3}|[0-9a-zA-Z]{5,8}))?)?$");
      } else if (ZoneId.class.isAssignableFrom(jtype.getRawClass())) {
        setEnums(
          Stream.of(TimeZone.getAvailableIDs())
            .collect(Collectors.toCollection(TreeSet::new))
        );
      } else if (UUID.class.isAssignableFrom(jtype.getRawClass())) {
        setPattern("^(?i)[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$");
      } else if (Class.class.isAssignableFrom(jtype.getRawClass())) {
        setPattern("\\p{ASCII}+");
      } else if (jtype.isArrayType()
          && jtype.getContentType().getRawClass().equals(byte.class)) {
        setPattern("^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$");
      }
    }
  }

  /**
   * Instantiates a new <code>StringTypesSchema</code> object.
   *
   * @author paouelle
   *
   * @param jtype the corresponding Java type
   */
  public StringTypesSchema(JavaType jtype) {
    if (Optional.class.isAssignableFrom(jtype.getRawClass())) {
      jtype = jtype.containedType(0);
    }
    this.javaType = jtype;
    this.javaTypes = ReferenceTypesSchema.getJsonSubTypesFrom(jtype.getRawClass())
      .map(c -> SimpleType.construct(c))
      .collect(Collectors.toCollection(LinkedHashSet::new));
    if (javaTypes.isEmpty()) {
      javaTypes.add(jtype);
    }
  }

  /**
   * Gets the type or base type associated with the referenced schema.
   *
   * @author paouelle
   *
   * @return the type or base type associated with the referenced schema
   */
  public JavaType getJavaType() {
    return javaType;
  }

  /**
   * Gets the type or sub-types associated with the referenced schema.
   *
   * @author paouelle
   *
   * @return the type or sub-types associated with the referenced schema
   */
  public Set<JavaType> getJavaTypes() {
    return javaTypes;
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
    return ReflectionToStringBuilder.toString(this);
  }
}

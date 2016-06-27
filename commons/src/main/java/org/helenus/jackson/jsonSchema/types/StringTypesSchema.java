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

import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

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
        jtype = jtype.getReferencedType();
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
   * @param jtype the corresponding Java type
   */
  public StringTypesSchema(JavaType jtype) {
    if (Optional.class.isAssignableFrom(jtype.getRawClass())) {
      jtype = jtype.getReferencedType();
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

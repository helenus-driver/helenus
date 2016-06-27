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

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.type.SimpleType;
import com.fasterxml.jackson.module.jsonSchema.types.ObjectSchema;

/**
 * The <code>ObjectTypesSchema</code> class extends the {@link ObjectSchema}
 * class to provide additional information about the Java type being defined.
 *
 * @copyright 2015-2016 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jun 24, 2016 - paouelle - Creation
 *
 * @since 1.0
 */
public class ObjectTypesSchema extends ObjectSchema {
  /**
   * Holds the type or base type associated with the defined schema.
   *
   * @author paouelle
   */
  @JsonIgnore
  private JavaType javaType;

  /**
   * Holds the type or sub-types associated with the defined schema.
   *
   * @author paouelle
   */
  @JsonIgnore
  private final Set<JavaType> javaTypes;

  /**
   * Instantiates a new <code>ObjectTypesSchema</code> object.
   *
   * @author paouelle
   *
   * @param schema the object types schema to shallow copy
   */
  public ObjectTypesSchema(ObjectTypesSchema schema) {
    // ObjectTypesSchema
    this.javaType = schema.javaType;
    this.javaTypes = schema.javaTypes;
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
    // ContainerTypeSchema
    setEnums(schema.getEnums());
    setOneOf(schema.getOneOf());
    // ObjectSchema
    setAdditionalProperties(schema.getAdditionalProperties());
    setDependencies(schema.getDependencies());
    setPatternProperties(schema.getPatternProperties());
    setProperties(schema.getProperties());
  }

  /**
   * Instantiates a new <code>ObjectTypesSchema</code> object.
   *
   * @author paouelle
   */
  public ObjectTypesSchema() {
    this.javaTypes = new LinkedHashSet<>(16);
  }

  /**
   * Set the type or sub-types defined by this schema.
   *
   * @author <a href="mailto:paouelle@enlightedinc.com">paouelle</a>
   *
   * @param jtype the Java type defined by this schema
   */
  public void setJavaTypesFor(JavaType jtype) {
    if (Optional.class.isAssignableFrom(jtype.getRawClass())) {
      jtype = jtype.getReferencedType();
    }
    this.javaType = jtype;
    javaTypes.clear();
    ReferenceTypesSchema.getJsonSubTypesFrom(jtype.getRawClass())
      .map(c -> SimpleType.construct(c))
      .forEach(t -> javaTypes.add(t));
    if (javaTypes.isEmpty()) {
      javaTypes.add(jtype);
    }
  }

  /**
   * Gets the type or base type associated with the defined schema.
   *
   * @author paouelle
   *
   * @return the type or base type associated with the defined schema
   */
  public JavaType getJavaType() {
    return javaType;
  }

  /**
   * Gets the type or sub-types associated with the defined schema.
   *
   * @author paouelle
   *
   * @return the type or sub-types associated with the defined schema
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

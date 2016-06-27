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
import java.util.stream.Stream;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.type.SimpleType;
import com.fasterxml.jackson.module.jsonSchema.types.ReferenceSchema;

import org.helenus.commons.lang3.reflect.ReflectionUtils;

/**
 * The <code>ReferenceTypesSchema</code> class extends the {@link ReferenceSchema}
 * class to provide additional information about the Java type being referenced.
 *
 * @copyright 2015-2016 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jun 24, 2016 - paouelle - Creation
 *
 * @since 1.0
 */
public class ReferenceTypesSchema extends ReferenceSchema {
  /**
   * Gets Json sub types from the specified class annotated with {@link JsonSubTypes}.
   *
   * @author paouelle
   *
   * @param  clazz the class for which to find the sub types
   * @return a stream of all sub type classes
   */
  @SuppressWarnings({"cast", "unchecked"})
  static <T> Stream<Class<? extends T>> getJsonSubTypesFrom(Class<T> clazz) {
    final JsonSubTypes jst = ReflectionUtils.findFirstAnnotation(clazz, JsonSubTypes.class);

    if (jst != null) {
      return Stream.of(jst.value())
        .map(t -> t.value())
        .filter(c -> clazz.isAssignableFrom(c))
        .map(c -> (Class<? extends T>)c);
    }
    return Stream.empty();
  }

  /**
   * Holds the type or sub-types associated with the referenced schema.
   *
   * @author paouelle
   */
  @JsonIgnore
  private final Set<JavaType> types;

  /**
   * Instantiates a new <code>ReferenceTypesSchema</code> object.
   *
   * @author paouelle
   *
   * @param ref the reference urn
   * @param jtype the corresponding Java type
   */
  public ReferenceTypesSchema(String ref, JavaType jtype) {
    super(ref);
    if (Optional.class.isAssignableFrom(jtype.getRawClass())) {
      jtype = jtype.getReferencedType();
    }
    this.types = ReferenceTypesSchema.getJsonSubTypesFrom(jtype.getRawClass())
      .map(c -> SimpleType.construct(c))
      .collect(Collectors.toCollection(LinkedHashSet::new));
    if (types.isEmpty()) {
      types.add(jtype);
    }
  }

  /**
   * Instantiates a new <code>ReferenceTypesSchema</code> object.
   *
   * @author paouelle
   *
   * @param schema the schema for which to create a reference one
   */
  public ReferenceTypesSchema(ObjectTypesSchema schema) {
    super(schema.getId());
    this.types = new LinkedHashSet<>(schema.getTypes());
  }

  /**
   * Gets the type or sub-types associated with the referenced schema.
   *
   * @author paouelle
   *
   * @return the type or sub-types associated with the referenced schema
   */
  public Set<JavaType> getTypes() {
    return types;
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

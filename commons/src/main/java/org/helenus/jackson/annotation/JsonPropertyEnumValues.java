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
package org.helenus.jackson.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import java.util.Locale;

import java.time.ZoneId;

import com.fasterxml.jackson.annotation.JacksonAnnotation;

/**
 * The <code>JsonPropertyEnumValues</code> annotation is used to provide a set
 * of all possible values that are valid for the annotated object, primitive,
 * <code>byte[]</code>, array, collection, or map property.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Oct 29, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
@Target({ElementType.ANNOTATION_TYPE, ElementType.FIELD, ElementType.METHOD, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@JacksonAnnotation
public @interface JsonPropertyEnumValues {
  /**
   * Defines all possible values that are valid for the annotated property or
   * its contained values.
   *
   * @author paouelle
   *
   * @return all possible values that are valid for the property or its
   *         contained values
   */
  String[] value() default {};

  /**
   * Defines all possible values that are valid for the annotated property or
   * its contained values from the Json sub types of the specified class.
   * <p>
   * <i>Note:</i> Only defined one class.
   *
   * @author paouelle
   *
   * @return the class from which to get all sub-types for all possible values
   *         that are valid for the property or its contained values
   */
  Class<?>[] valueSubTypesOf() default {};

  /**
   * Defines all possible values that are valid for the annotated property or
   * its contained values from the specified class' available values. Supported
   * classes are {@link Locale}, {@link ZoneId}, and all enum classes.
   * <p>
   * <i>Note:</i> Only defined one class.
   *
   * @author paouelle
   *
   * @return the class from which to get all sub-types for all possible values
   *         that are valid for the property or its contained values
   */
  Class<?>[] valueAvailablesOf() default {};

  /**
   * Set of enum values to exclude from the schema for the annotated property or
   * its contained values.
   *
   * @author paouelle
   *
   * @return a set of enum values to exclude from the schema for the property
   *         or its contained values
   */
  String[] valueExclude() default {};

  /**
   * Defines all possible values that are valid for the annotated property
   * contained keys.
   *
   * @author paouelle
   *
   * @return all possible values that are valid for the property contained keys
   */
  String[] key() default {};

  /**
   * Defines all possible values that are valid for the annotated property
   * contained keys from the Json sub types of the specified class.
   * <p>
   * <i>Note:</i> Only defined one class.
   *
   * @author paouelle
   *
   * @return the class from which to get all sub-types for all possible values
   *         that are valid for the property contained keys
   */
  Class<?>[] keySubTypesOf() default {};

  /**
   * Defines all possible values that are valid for the annotated property
   * contained keys from the specified class' available values. Supported
   * classes are {@link Locale}, {@link ZoneId}, and all enum classes.
   * <p>
   * <i>Note:</i> Only defined one class.
   *
   * @author paouelle
   *
   * @return the class from which to get all available possible values
   *         that are valid for the property contained keys
   */
  Class<?>[] keyAvailablesOf() default {};

  /**
   * Set of enum values to exclude from the schema for the annotated property
   * contained keys.
   *
   * @author paouelle
   *
   * @return a set of enum values to exclude from the schema for the property
   *         contained keys
   */
  String[] keyExclude() default {};
}

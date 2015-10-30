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

import com.fasterxml.jackson.annotation.JacksonAnnotation;

/**
 * The <code>JsonPropertyOneOfDoubleValues</code> annotation is used to provide a set
 * of all possible values that are valid for the annotated object, array,
 * collection, or map property.
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
public @interface JsonPropertyOneOfDoubleValues {
  /**
   * Defines all possible values that are valid for the annotated property or
   * its contained values.
   *
   * @author paouelle
   *
   * @return all possible values that are valid for the property or its contained
   *         values
   */
  double[] value() default {};

  /**
   * Defines all possible values that are valid for the annotated property
   * contained keys.
   *
   * @author paouelle
   *
   * @return all possible values that are valid for the property contained keys
   */
  double[] key() default {};
}

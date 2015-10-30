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
 * The <code>JsonPropertyLength</code> annotation is used to provide length
 * information for the annotated string, <code>byte[]</code>, array, or
 * collection property.
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
public @interface JsonPropertyLength {
  /**
   * Defines the length for the annotated property or its contained values.
   *
   * @author paouelle
   *
   * @return the length for the annotated property or its contained values
   */
  int value() default -1;

  /**
   * Defines the minimum length for the annotated property or its contained
   * values.
   * <p>
   * <i>Note:</i> Ignored if {@link #value} specified.
   *
   * @author paouelle
   *
   * @return the minimum length for the annotated property or its contained
   *         values
   */
  int valueMin() default -1;

  /**
   * Defines the maximum length for the annotated property or its contained
   * values.
   * <p>
   * <i>Note:</i> Ignored if {@link #value} specified.
   *
   * @author paouelle
   *
   * @return the maximum length for the annotated property or its contained
   *         values
   */
  int valueMax() default -1;

  /**
   * Defines the length for the annotated property contained keys.
   *
   * @author paouelle
   *
   * @return the length for the annotated property contained keys
   */
  int key() default -1;

  /**
   * Defines the minimum length for the annotated property contained keys.
   * <p>
   * <i>Note:</i> Ignored if {@link #key} specified.
   *
   * @author paouelle
   *
   * @return the minimum length for the annotated property contained keys
   */
  int keyMin() default -1;

  /**
   * Defines the maximum length for the annotated property contained keys.
   * <p>
   * <i>Note:</i> Ignored if {@link #key} specified.
   *
   * @author paouelle
   *
   * @return the maximum length for the annotated property contained keys
   */
  int keyMax() default -1;
}

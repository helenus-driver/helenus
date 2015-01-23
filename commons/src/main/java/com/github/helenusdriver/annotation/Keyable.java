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
package com.github.helenusdriver.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.AnnotatedElement;

import com.github.helenusdriver.commons.lang3.reflect.ReflectionUtils;

/**
 * The <code>Keyable</code> annotation is used to annotate an annotation that
 * can be keyed. This can be useful with repeatable annotations where each of
 * the repeated annotations can be keyed differently. The method
 * {@link ReflectionUtils#getAnnotationsByType(Class, Class, AnnotatedElement)}
 * can be used to retrieved those as a map.
 * <p>
 * The value defined must correspond to an element in the annotated annotation
 * which holds the unique key for the annotation.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 15, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.ANNOTATION_TYPE)
public @interface Keyable {
  /**
   * Specifies the key element name in the annotated annotation.
   *
   * @author paouelle
   */
  String value();
}

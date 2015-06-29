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
package org.helenus.driver.junit.util;

import java.lang.annotation.Annotation;
import java.lang.reflect.Array;
import java.lang.reflect.Method;

import java.util.ArrayList;
import java.util.List;

/**
 * The <code>ReflectionJUnitUtils</code> class provides reflection utilities
 * specific to JUnit.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 15, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
public class ReflectionJUnitUtils {
  /**
   * Gets all annotations of a specified type for the test method and class given
   * a test description. This method will return an array of all found annotations
   * in the order corresponding to the annotations from the test method first
   * followed by the annotations of the test class next.
   *
   * @author paouelle
   *
   * @param <A> the type of annotation to search for
   *
   * @param  method the test method for which to find all annotations of a given
   *         type
   * @param  annotationClass the annotation to find
   * @return a non-<code>null</code> array of all found annotations
   * @throws NullPointerException if <code>description</code> or
   *         <code>annotationClass</code> is <code>null</code>
   */
  @SuppressWarnings("unchecked")
  public static <A extends Annotation> A[] getAnnotationsByType(
    Method method, Class<A> annotationClass
  ) {
    org.apache.commons.lang3.Validate.notNull(method, "invalid null method");
    final List<A> annotations = new ArrayList<>(8);

    for (final A a: method.getAnnotationsByType(annotationClass)) {
      annotations.add(a);
    }
    for (final A a: method.getDeclaringClass().getAnnotationsByType(annotationClass)) {
      annotations.add(a);
    }
    final A[] as = (A[])Array.newInstance(annotationClass, annotations.size());

    return annotations.toArray(as);
  }

  /**
   * Prevents instantiation of a new <code>class</code> object.
   *
   * @author paouelle
   *
   * @throws IllegalStateException always thrown
   */
  private ReflectionJUnitUtils() {
    throw new IllegalStateException("invalid constructor called");
  }
}

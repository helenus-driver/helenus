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
package org.helenus.driver.junit;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * The <code>SuffixKeyValues</code> annotation allows a test method or class to
 * define a specific suffix key values to be used when creating schemas for pojos.
 * <p>
 * When defining suffix key values, one can set them on either the test class or
 * the test method. Suffix key values defined on the test methods will override
 * any the values defined for the same suffix type defined on the class. The order
 * of the annotations provided also indicates a priority. That is that the first
 * annotation that matches will override all others found on the same method or
 * class.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jun 28, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(SuffixKeyValuess.class)
@Inherited
@Documented
public @interface SuffixKeyValues {
  /**
   * The type for the suffix key.
   *
   * @author paouelle
   *
   * @return the type for the suffix key
   */
  String type();

  /**
   * The values for the suffix key.
   *
   * @author paouelle
   *
   * @return the values for the suffix key
   */
  String[] values();

  /**
   * The optional pojo classes that these suffix key values are defined for.
   * <p>
   * <i>Note:</i> The suffix key values will apply to all pojo classes if both
   * {@link #classes} and {@link #packages} are omitted. If both are specified
   * then the pojo class only needs to match one of them to have the suffix key
   * values be applied to its schema definition.
   *
   * @author paouelle
   *
   * @return the optional pojo classes that these suffix key values are defined for
   */
  Class<?>[] classes() default {};

  /**
   * The optional pojo package names that these suffix key values are defined for.
   * <p>
   * <i>Note:</i> The suffix key values will apply to all pojo classes if both
   * {@link #classes} and {@link #packages} are omitted. If both are specified
   * then the pojo class only needs to match one of them to have the suffix key
   * values be applied to its schema definition.
   *
   * @author paouelle
   *
   * @return the optional pojo package names that these suffix key values are
   *         defined for
   */
  String[] packages() default {};
}

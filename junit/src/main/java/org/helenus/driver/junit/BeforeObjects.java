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
package org.helenus.driver.junit;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import java.util.Collection;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.stream.Stream;

/**
 * The <code>BeforeObjects</code> annotation can be used to identify a method
 * that will be invoked before the test methods in order to pre-populate the
 * database with the pojos returned by the method.
 * <p>
 * The referenced method must be public. It can return an array, a {@link Collection},
 * an {@link Iterable}, an {@link Iterator}, an {@link Enumeration}, or a
 * {@link Stream} of pojo objects or a single object to insert in the database.
 * Finally, if it is declared with a single parameter of type
 * <code>Map&lt;String, String&gt;</code> the Helenus JUnit framework will provide
 * access to each keyspace key values based on the corresponding keyspace key types. It
 * is therefore possible that the method be called multiple times; once per set
 * of keyspace key values. The keyspace key values are computed from the
 * {@link PartitionKeyValues} annotation provided for the test method and class.
 *
 * @copyright 2015-2016 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 *
 * @version 1 - Jun 28, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@Documented
public @interface BeforeObjects {
  /**
   * Specifies the names of the only test methods for which to invoke the
   * annotated method. If left blank then the annotated method is called for
   * all test methods.
   *
   * @author paouelle
   *
   * @return the names of optional test methods for which to call the annotated
   *         method
   */
  String[] value() default {};
}

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
package org.helenus.driver.persistence;

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
 * The <code>InitialObjects</code> annotation can be used to identify a static
 * method to invoke at the time the table for an associate entity class
 * is created in order to pre-populate the table with the pojos returned by the
 * method. It can also be used with an object created class which can be used
 * alongside with the Helenus tool to insert objects in the database.
 * <p>
 * The referenced method must be public and static. It can return an array, a
 * {@link Collection}, an {@link Iterable}, an {@link Iterator}, an
 * {@link Enumeration}, or a {@link Stream} of pojo objects or a single object
 * to insert in the database. Finally, it must not take any parameters unless
 * the pojo class entity is defined with keyspace keys in which case it must expect
 * a single parameter of type <code>Map&lt;String, String&gt;</code> which will
 * be used to provide access to each keyspace key values based on the corresponding
 * keyspace key types.
 *
 * @copyright 2015-2016 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 *
 * @version 1 - Jan 15, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@Documented
public @interface InitialObjects {
  /**
   * Specifies dependent creator classes which must be explored and their provided
   * objects inserted into the database first before invoking this creator's
   * initial method.
   * <p>
   * <i>Note:</i> This attribute only make sense when the annotation is used
   * as an object creator via the Helenus tool.
   *
   * @author paouelle
   *
   * @return the dependent creator classes
   */
  Class<?>[] dependsOn() default {};
}

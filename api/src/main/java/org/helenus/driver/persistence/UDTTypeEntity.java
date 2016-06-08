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
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * The <code>UDTTypeEntity</code> annotation must be used on all subclasses of a
 * base class annotated with {@link UDTRootEntity} in order to identify their
 * specific type name. This type name is used to uniquely identify the specific
 * type of the base class persisted to Cassandra. A type class cannot define
 * any type keys.
 *
 * @copyright 2015-2016 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jun 6, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface UDTTypeEntity {
  /**
   * The name for the user-defined type. The name must be unique within a given
   * UDT root entity. Valid table names are strings of alpha-numeric characters
   * and underscores, and must begin with a letter.
   * <p>
   * Reserved names such as byte, smallint, complex, enum, date, interval,
   * macaddr, and bitstring are not allowed.
   *
   * @author paouelle
   *
   * @return the name for the user-defined type
   */
  String name();
}

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
 * The <code>UDTRootEntity</code> annotation must be used on all UDT classes
 * defining a base class for other UDT classes. This is similar to the
 * {@link UDTEntity} annotation with the difference that it applies only to an
 * abstract base class and all the columns of that class gets persisted to
 * Cassandra. In addition, if two different UDT type entities uses the same
 * column name, it must be persisted the same way (i.e. same Cassandra type).
 * <p>
 * All subclasses must be annotated with the {@link UDTTypeEntity} annotation in
 * order to indicate the specific UDT type of this base class they represent.
 * <p>
 * The base class annotated with {@link UDTRootEntity} must include a single column
 * as an attribute type {@link String} annotated with the {@link TypeKey} annotation.
 * This attribute will be used to hold the UDT entity type and used to identify the
 * class that will be re-created at the time an object is retrieved from Cassandra.
 * <p>
 * The root UDT entity class must defined must defined the keyspace and suffix
 * keys.
 *
 * @copyright 2015-2016 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jun 6, 2016 - paouelle - Creation
 *
 * @since 1.0
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface UDTRootEntity {
  /**
   * The name for this root user-defined type. Valid table names are strings of
   * alpha-numeric characters and underscores, and must begin with a letter.
   * <p>
   * Reserved names such as byte, smallint, complex, enum, date, interval,
   * macaddr, and bitstring are not allowed.
   *
   * @author paouelle
   *
   * @return the name for the too user-defined type
   */
  String name();

  /**
   * List of supported types for this UDT root entity.
   *
   * @author paouelle
   *
   * @return the list of supported types
   */
  Class<?>[] types();
}

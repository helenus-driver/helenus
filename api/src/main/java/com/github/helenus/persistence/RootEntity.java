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
package com.github.helenus.persistence;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * The <code>RootEntity</code> annotation must be used on all classes defining
 * a base class for other classes. This is similar to the {@link Entity}
 * annotation with the difference that it applies only to an abstract base class
 * and all the columns of that class gets persisted to Cassandra. In addition,
 * all columns of all defined types are also persisted with the restriction that
 * if two different type entities uses the same column name, it must be
 * persisted the same way (i.e. same Cassandra type).
 * <p>
 * All subclasses must be annotated with the {@link TypeEntity} annotation in
 * order to indicate the specific type of this base class they represent.
 * <p>
 * The base class annotated with {@link RootEntity} must include a single column
 * as an attribute type {@link String} annotated with the {@typeKey} annotation.
 * This attribute will be used to hold the entity type and used to identify the
 * class that will be re-created at the time an object is retrieved from Cassandra.
 * <p>
 * The root entity class must defined all partition and /or clustering keys and
 * must defined the keyspace and all tables.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 15, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface RootEntity {
  /**
   * List of supported types for this root entity.
   *
   * @author paouelle
   */
  Class<?>[] types();
}

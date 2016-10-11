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
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * The <code>KeyspaceKey</code> annotation specifies a keyspace key to the database
 * keyspace name defined as a suffix to this name that is parameterized by an
 * object attribute's value.
 * <p>
 * Attributes annotated with this annotation will be loaded with the corresponding
 * part of the keyspace name when reloaded from the database later.
 * <p>
 * When defining a user-defined type where the associated keyspace is defined
 * with keyspace keys, this annotation should be place on the class itself to provide
 * a mapping between a keyspace key type and a "fake" column name such that the keyspace
 * key could be specified in the CREATE TYPE statement.
 *
 * @copyright 2015-2016 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 15, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
@Target({ElementType.FIELD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(KeyspaceKeys.class)
@Inherited
@Documented
public @interface KeyspaceKey {
  /**
   * The name for the keyspace key. If the same field is used both as a column
   * and a keyspace key, then this value should match what is defined in the
   * {@link Column} annotation. This is the name used in "WHERE" clause when
   * building statements.
   *
   * @author paouelle
   *
   * @return the name of the keyspace key
   */
  String name();

  /**
   * The key type is to identify similar keyspace keys across many POJOs
   * (e.g. a customer acronym, a region name, ...).
   *
   * @author paouelle
   *
   * @return the key type
   */
  String type();

  /**
   * The keyspace key values to exclude.
   *
   * @author paouelle
   *
   * @return a list of keyspace key values that should be excluded (or for which
   *         no keyspace would be created)
   */
  String[] exclude() default {};
}

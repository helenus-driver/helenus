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
package com.github.helenusdriver.persistence;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * The <code>SuffixKey</code> annotation specifies a suffix key to the database
 * keyspace name that is parameterized by an object attribute's value.
 * <p>
 * Attributes annotated with this annotation will be loaded with the corresponding
 * part of the keyspace name when reloaded from the database later.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 15, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface SuffixKey {
  /**
   * The name for the suffix key. If the same field is used both as a column and
   * a suffix key, then this value should match what is defined in the
   * {@link Column} annotation. This is the name used in "WHERE" clause when
   * building statements.
   *
   * @author paouelle
   *
   * @return the name of the suffix key
   */
  String name();

  /**
   * The suffix type is to identify similar suffixes across many POJOs (e.g. a
   * customer suffix).
   *
   * @author paouelle
   *
   * @return the suffix type
   */
  String type();
}

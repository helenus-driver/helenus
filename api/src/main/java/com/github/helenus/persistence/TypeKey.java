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

import com.github.helenus.annotation.Keyable;

/**
 * The <code>TypeKey</code> annotation indicates the column must be defined in
 * a root element base class to hold the type of the pojo. It is used such that
 * it be persisted to Cassandra. Such a column can also be mark as a partition
 * or clustering key. However, the field annotated with {@link TypeKey} must be
 * of type {@link String}.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 15, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
@Target({ElementType.METHOD, ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Keyable("table")
@Documented
public @interface TypeKey {
  /**
   * The name of the table as defined in {@link Table#name} this type key is
   * associated with.
   * <p>
   * By default, if none specified, the column will be considered a type
   * key for all tables defined by the root entity.
   *
   * @author paouelle
   */
  String table() default Table.ALL;
}

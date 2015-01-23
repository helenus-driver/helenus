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
import java.lang.annotation.Inherited;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.github.helenusdriver.annotation.Keyable;

/**
 * The <code>Table</code> annotation specifies table information for the annotated
 * object.
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
@Repeatable(Tables.class)
@Keyable("name")
@Inherited
@Documented
public @interface Table {
  /**
   * Special constants used in the {@link Column}, {@PartitionKey}, and
   * {@ClusteringKey} annotations to indicate the annotation applies to
   * all tables defined for the entity.
   *
   * @author paouelle
   */
  public static final String ALL = "@all";

  /**
   * The name for the table. Valid table names are strings of alpha-numeric
   * characters and underscores, and must begin with a letter.
   *
   * @author paouelle
   */
  String name();

  /**
   * Optional list of ordered partition keys. By default, partition keys are
   * ordered based on the order they are defined in the class. However, the
   * order can be tweaked by specifying a list of column names. Any keys not
   * listed will be added at the end of the provided list.
   *
   * @author paouelle
   */
  String[] partition() default {};

  /**
   * Optional list of ordered clustering keys. By default, clustering keys are
   * ordered based on the order they are defined in the class. However, the
   * order can be tweaked by specifying a list of column names. Any keys not
   * listed will be added at the end of the provided list.
   *
   * @author paouelle
   */
  String[] clustering() default {};
}

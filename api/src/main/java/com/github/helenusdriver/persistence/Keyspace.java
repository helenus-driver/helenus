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
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * The <code>Keyspace</code> annotation specifies keyspace information for the
 * annotated object.
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
@Inherited
@Documented
public @interface Keyspace {
  /**
   * Constant for the "global" keyspace.
   *
   * @author paouelle
   */
  public final static String GLOBAL = "global";

  /**
   * The name for the keyspace. Valid keyspace names are strings of alpha-numeric
   * characters and underscores, and must begin with a letter. No keyspace name
   * can be referenced unless there is at least one suffix defined.
   *
   * @author paouelle
   */
  String name() default "";

  /**
   * Optional list of suffix types to incorporate into the final keyspace name.
   * These suffix key values are resolved at runtime from the value of the
   * attribute or property tagged with a {@link SuffixKey} annotation which
   * identifies the same suffix type.
   *
   * @author paouelle
   */
  String[] suffixes() default {};

  /**
   * The optional placement strategy for the keyspace. Defaults to
   * {@link StrategyClass#SIMPLE}.
   *
   * @author paouelle
   */
  StrategyClass strategy() default StrategyClass.SIMPLE;

  /**
   * The default replication factor to use along with the simple placement
   * strategy. Defaults to 0 indicating the value should be provided by the
   * Helenus tool at the time the keyspace is created otherwise a replication
   * factor of 2 will be used.
   *
   * @author paouelle
   */
  int replicationFactor() default 0;

  // TODO: for network topology, we might need to specify a properties file which will define the data centers and their respective replication factor. If omitted there, we can then use the above one as a default value

  /**
   * When set to false, data written to the keyspace bypasses the commit log.
   * Be careful using this option because you risk losing data. Do not set this
   * attribute on a keyspace using the {@link StrategyClass#SIMPLE}. Defaults
   * to true.
   *
   * @author paouelle
   */
  boolean durableWrites() default true;
}

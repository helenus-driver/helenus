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

/**
 * The <code>Keyspace</code> annotation specifies keyspace information for the
 * annotated object.
 *
 * @copyright 2015-2016 The Helenus Driver Project Authors
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
   * characters and underscores, and must begin with a letter. An empty keyspace
   * name cannot be referenced unless there is at least one keyspace key defined.
   *
   * @author paouelle
   *
   * @return the name of the keyspace
   */
  String name() default "";

  /**
   * Optional list of keyspace key types to incorporate into the final keyspace name.
   * These keyspace key values are resolved at runtime from the value of the
   * attribute or property tagged with a {@link KeyspaceKey} annotation which
   * identifies the same keyspace key type.
   *
   * @author paouelle
   *
   * @return the optional list of keyspace key types
   */
  String[] keys() default {};

  /**
   * The optional placement strategy for the keyspace. Defaults to
   * {@link PlacementClass#NETWORK_TOPOLOGY}.
   *
   * @author paouelle
   *
   * @return the optional placement strategy class
   */
  PlacementClass placement() default PlacementClass.NETWORK_TOPOLOGY;

  /**
   * The default replication factor to use along with the simple placement
   * strategy or with the corresponding indexed data center with the network
   * topology strategy. Defaults to 0 indicating the value should be provided
   * by the Helenus tool at the time the keyspace is created otherwise a
   * replication factor of 2 will be used.
   *
   * @author paouelle
   *
   * @return the default replication factor to use with the simple placement
   *         strategy or with the corresponding indexed data centers with the
   *         network topology strategy
   */
  int[] replicationFactor() default 0;

  /**
   * The data centers where to replicate the data to use along with the network
   * topology strategy. The corresponding indexed replication factor will be
   * used for the number of replicas for each data center specified. Defaults
   * to none which means the set of data centers should be provided by the
   * Helenus tool at the time the keyspace is created otherwise it will fallback
   * to a simple strategy.
   *
   * @author paouelle
   *
   * @return the default data centers to replicate the data to use along with
   *         the network topology strategy with the corresponding indexed
   *         replication factor
   */
  String[] dataCenter() default {};

  /**
   * When set to false, data written to the keyspace bypasses the commit log.
   * Be careful using this option because you risk losing data. Do not set this
   * attribute on a keyspace using the {@link PlacementClass#SIMPLE}. Defaults
   * to true.
   *
   * @author paouelle
   *
   * @return flag indicating whether to use durable writes or not
   */
  boolean durableWrites() default true;
}

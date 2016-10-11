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
package org.helenus.driver.junit;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * The <code>PartitionKeyValues</code> annotation allows a test method or class to
 * define specific keyspace key values to be used when creating schemas for pojos.
 * <p>
 * When defining keyspace key values, one can set them on either the test class or
 * the test method. Partition key values defined on the test methods will override
 * any the values defined for the same keyspace key type on the class. The order of
 * the annotations provided also indicates a priority. That is that the first
 * annotation that matches will override all others found on the same method or
 * class.
 *
 * @copyright 2015-2016 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jun 28, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(PartitionKeyValuess.class)
@Inherited
@Documented
public @interface PartitionKeyValues {
  /**
   * The type for the keyspace key.
   *
   * @author paouelle
   *
   * @return the type for the keyspace key
   */
  String type();

  /**
   * The values for the keyspace key.
   *
   * @author paouelle
   *
   * @return the values for the keyspace key
   */
  String[] values();
}

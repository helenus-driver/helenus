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
package org.helenus.driver.persistence;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

/**
 * The <code>UDTEntity</code> annotation must be used on all classes defining
 * a user-defined type (i.e. UDT) entity.
 * <p>
 * <i>Note:</i> Classes that implements the {@link List}, {@link Set},
 * {@link SortedMap}, or {@link Map} interface will automatically have their
 * collection content stored in a special column in Cassandra. The data type
 * of the elements will be inferred from the arguments of the superclass. This
 * will allow, for a example, a UDT pojo class to extend <code>LinkedHashSet&lt;String&gt;</code>
 * which would automatically infer the argument type for a set as a string. If
 * this is not possible, then the class should be annotated with {@link UDTEntity.Data}
 * in order to specify directly the arguments and skip the automatic inference.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Mar 3, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface UDTEntity {
  /**
   * The name for the user-defined type. Valid table names are strings of
   * alpha-numeric characters and underscores, and must begin with a letter.
   * <p>
   * Reserved names such as byte, smallint, complex, enum, date, interval,
   * macaddr, and bitstring are not allowed.
   *
   * @author paouelle
   *
   * @return the name for the table
   */
  String name();

  /**
   * The <code>Data</code> annotation is used for user-defined types that extends
   * {@link List}, {@link Set}, and {@link Map} to override the default mapping
   * of arguments from the class' elements or keys/values with specified one.
   *
   * @copyright 2015-2015 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Nov 13, 2015 - paouelle - Creation
   *
   * @since 1.0
   */
  @Target(ElementType.TYPE)
  @Retention(RetentionPolicy.RUNTIME)
  @Documented
  public @interface Data {
    /**
     * Indicates the argument types for a collection user-defined type.
     *
     * @author paouelle
     *
     * @return the argument types for a collection user-defined type
     */
    DataType[] arguments();
  }
}

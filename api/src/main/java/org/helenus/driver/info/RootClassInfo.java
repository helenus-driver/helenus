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
package org.helenus.driver.info;

import java.util.stream.Stream;

/**
 * The <code>RootClassInfo</code> interface extends on the {@link ClassInfo}
 * interface to provide addition information for root entity POJO class.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Mar 11, 2015 - paouelle - Creation
 *
 * @param <T> The type of POJO represented by this class
 *
 * @since 1.0
 */
public interface RootClassInfo<T> extends ClassInfo<T> {
  /**
   * Gets the class info for the specified type entity defined from this root
   * entity.
   *
   * @author paouelle
   *
   * @param <S> the type of POJO for the type entity
   *
   * @param  clazz the POJO class of the type entity to retrieve its info
   * @return the corresponding type entity POJO class information or <code>null</code>
   *         if none defined for the given class
   */
  public <S extends T> TypeClassInfo<S> getType(Class<S> clazz);

  /**
   * Gets the class info for the specified type entity defined from this root
   * entity.
   *
   * @author paouelle
   *
   * @param  name the name of the type entity to retrieve its info
   * @return the corresponding type entity POJO class information or <code>null</code>
   *         if none defined for the given name
   */
  public TypeClassInfo<? extends T> getType(String name);

  /**
   * Gets all type entities defined from this root entity.
   *
   * @author paouelle
   *
   * @return a stream of all type entities defined from this root entity
   */
  public Stream<TypeClassInfo<? extends T>> types();

  /**
   * Gets the number of type entities defined from this root entity.
   *
   * @author paouelle
   *
   * @return the number of type entities defined from this root entity
   */
  public int getNumTypes();
}

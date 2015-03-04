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
package com.github.helenusdriver.driver.impl;

import java.lang.reflect.Modifier;

import java.util.Collection;
import java.util.Collections;

import org.apache.commons.lang3.ArrayUtils;

import com.github.helenusdriver.persistence.UDTEntity;

/**
 * The <code>UDTClassInfoImpl</code> class provides information about a
 * particular POJO class.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Mar 3, 2015 - paouelle - Creation
 *
 * @param <T> The type of POJO represented by this class
 *
 * @since 2.0
 */
@lombok.ToString(callSuper=true)
@lombok.EqualsAndHashCode(callSuper=true)
public class UDTClassInfoImpl<T> extends ClassInfoImpl<T> {
  /**
   * Holds the reserved user-defined type names.
   *
   * @author paouelle
   */
  private final static String[] RESERVED_UDT_NAMES = {
    "byte",
    "smallint",
    "complex",
    "enum",
    "date",
    "interval",
    "macaddr",
    "bitstring"
  };

  /**
   * Holds the name for the user-defined type represented by this class.
   *
   * @author <a href="mailto:paouelle@enlightedinc.com">paouelle</a>
   */
  private final String name;

  /**
   * Instantiates a new <code>TypeClassInfoImpl</code> object.
   *
   * @author paouelle
   *
   * @param  mgr the non-<code>null</code> statement manager
   * @param  clazz the class of POJO for which to get a class info object for
   * @throws NullPointerException if <code>clazz</code> is <code>null</code>
   * @throws IllegalArgumentException if <code>clazz</code> doesn't represent
   *         a valid POJO class
   */
  UDTClassInfoImpl(StatementManagerImpl mgr, Class<T> clazz) {
    super(mgr, clazz, UDTEntity.class);
    org.apache.commons.lang3.Validate.isTrue(
      !Modifier.isAbstract(clazz.getModifiers()),
      "type entity class '%s', cannot be abstract", clazz.getSimpleName()
    );
    this.name = findName();
  }

  /**
   * Finds the annotated type name for this POJO class.
   *
   * @author paouelle
   *
   * @return the non-<code>null</code> annotated type name
   * @throws IllegalArgumentException if the POJO class is improperly annotated
   */
  private String findName() {
    final UDTEntity ue = clazz.getAnnotation(UDTEntity.class);

    org.apache.commons.lang3.Validate.isTrue(
      ue != null,
      "class '%s' is not annotated with @UDTEntity", clazz.getSimpleName()
    );
    org.apache.commons.lang3.Validate.isTrue(
      !ArrayUtils.contains(UDTClassInfoImpl.RESERVED_UDT_NAMES, ue.name()),
      "user-defined type name cannot be a reserved type name: %s", ue.name()
    );
    return ue.name();
  }

  /**
   * Gets the name for the user-defined type represented by this class.
   *
   * @author paouelle
   *
   * @return the non-<code>null</code> name for the user-defined type represented
   *         by this class
   */
  public String getName() {
    return name;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.impl.ClassInfoImpl#supportsTablesAndIndexes()
   */
  @Override
  public boolean supportsTablesAndIndexes() {
    return false;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.impl.ClassInfoImpl#getTableImpl(java.lang.String)
   */
  @Override
  public TableInfoImpl<T> getTableImpl(String name) {
    throw new IllegalArgumentException("user-defined types do not define tables");
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.impl.ClassInfoImpl#getNumTables()
   */
  @Override
  public int getNumTables() {
    return 0;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.impl.ClassInfoImpl#getTables()
   */
  @Override
  public Collection<TableInfoImpl<T>> getTables() {
    return Collections.emptyList();
  }
}

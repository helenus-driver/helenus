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
package org.helenus.driver.impl;

import java.lang.reflect.Modifier;

import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.Row;

import org.helenus.commons.lang3.reflect.ReflectionUtils;
import org.helenus.driver.ObjectConversionException;
import org.helenus.driver.info.UDTTypeClassInfo;
import org.helenus.driver.persistence.Keyspace;
import org.helenus.driver.persistence.Table;

/**
 * The <code>UDTSubClassInfoImpl</code> class provides information about a
 * particular UDT sub-root element POJO class.
 *
 * @copyright 2015-2016 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jun 6, 2016 - paouelle - Creation
 *
 * @param <T> The type of POJO represented by this class
 *
 * @since 2.0
 */
@lombok.ToString(callSuper=true)
@lombok.EqualsAndHashCode(callSuper=true, exclude="rinfo")
public class UDTSubClassInfoImpl<T> extends UDTRootClassInfoImpl<T> {
  /**
   * Holds the class info for the UDT root entity this POJO is a type.
   *
   * @author paouelle
   */
  private final UDTRootClassInfoImpl<? super T> rinfo;

  /**
   * Instantiates a new <code>UDTSubClassInfoImpl</code> object.
   *
   * @author paouelle
   *
   * @param  rinfo the class info for the UDT root entity this POJO is a type
   * @param  clazz the subclass of POJO for which to get a class info object for
   * @throws NullPointerException if <code>clazz</code> is <code>null</code>
   * @throws IllegalArgumentException if <code>clazz</code> doesn't represent
   *         a valid POJO class
   */
  UDTSubClassInfoImpl(UDTRootClassInfoImpl<? super T> rinfo, Class<T> clazz) {
    super(rinfo, clazz);
    org.apache.commons.lang3.Validate.isTrue(
      Modifier.isAbstract(clazz.getModifiers()),
      "UDT root entity subclass '%s', must be abstract", clazz.getSimpleName()
    );
    this.rinfo = rinfo;
    // validate the POJO subclass
    validate(rinfo.getObjectClass());
  }

  /**
   * Validates this UDT entity subclass.
   *
   * @author paouelle
   *
   * @param  rclazz the non-<code>null</code> class of POJO for the UDT root element
   * @throws IllegalArgumentException if the POJO subclass is improperly annotated
   */
  private void validate(Class<? super T> rclazz) {
    // check keyspace
    org.apache.commons.lang3.Validate.isTrue(
      ReflectionUtils.findFirstClassAnnotatedWith(
        clazz, Keyspace.class
      ).isAssignableFrom(rclazz),
      "@Keyspace annotation is not defined on UDT root element class '%s' for subclass: %s",
      rclazz.getSimpleName(), clazz.getSimpleName()
    );
    // check table
    org.apache.commons.lang3.Validate.isTrue(
      ReflectionUtils.findFirstClassAnnotatedWith(
        clazz, Table.class
      ).isAssignableFrom(rclazz),
      "@Table annotation is not defined on UDT root element class '%s' for subclass: %s",
      rclazz.getSimpleName(), clazz.getSimpleName()
    );
    // check type key
    getTableImpl().getTypeKey().ifPresent(
      f -> {
        org.apache.commons.lang3.Validate.isTrue(
          f.getDeclaringClass().isAssignableFrom(rclazz),
          "@TypeKey annotation with name '%s' is not defined on UDT root element class '%s' for subclass: %s",
          f.getColumnName(), rclazz.getSimpleName(), clazz.getSimpleName()
        );
      }
    );
  }

  /**
   * Gets the class info for the UDT root entity defined for this subclass.
   *
   * @author paouelle
   *
   * @return the non-<code>null</code> UDT root entity POJO class info defined for
   *         this type entity
   */
  public UDTRootClassInfoImpl<? super T> getRoot() {
    return rinfo;
  }

  /**
   * Creates a new context for this subclass info with the given POJO object.
   *
   * @author paouelle
   *
   * @param  object the POJO object
   * @return a non-<code>null</code> newly created context for this subclass info
   * @throws NullPointerException if <code>object</code> is <code>null</code>
   * @throws IllegalArgumentException if <code>object</code> is not of the
   *         appropriate class
   */
  public POJOContext newContextFromRoot(Object object) {
    try {
      return newContext(clazz.cast(object));
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(e.getMessage(), e);
    }
  }

  /**
   * Converts the specified result row into a POJO object defined by this
   * class information and keyspace key map.
   *
   * @author paouelle
   *
   * @param  row the result row to convert into a POJO
   * @param  type the POJO type extracted from the specified row
   * @param  kkeys a map of keyspace key values to report back into the created
   *         POJO
   * @return the POJO object corresponding to the given result row or <code>null</code>
   *         if the type doesn't match this type entity name
   * @throws NullPointerException if <code>type</code> or <code>kkeys</code>
   *         is <code>null</code>
   * @throws ObjectConversionException if unable to convert to a POJO
   */
  public T getObject(Row row, String type, Map<String, Object> kkeys) {
    if ((row == null)
        || !clazz.isAssignableFrom(super.getType(type).getObjectClass())) {
      return null;
    }
    return super.getObject(row, kkeys);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.UDTRootClassInfoImpl#getObject(com.datastax.driver.core.Row, java.util.Map)
   */
  @Override
  public T getObject(Row row, Map<String, Object> kkeys) {
    if (row == null) {
      return null;
    }
    final ColumnDefinitions cdefs = row.getColumnDefinitions();
    // extract the type so we know which object we are creating
    final TableInfoImpl<T> table = getTableImpl();
    final FieldInfoImpl<T> type = table.getTypeKey().orElse(null);

    if (type != null) {
      final int i = cdefs.getIndexOf(type.getColumnName());

      if ((i != -1) && table.getName().equals(cdefs.getTable(i))) {
        return getObject(
          row, Objects.toString(type.decodeValue(row), null), kkeys
        );
      }
    }
    throw new ObjectConversionException(clazz, row, "missing POJO type column");
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.UDTRootClassInfoImpl#getType(java.lang.Class)
   */
  @Override
  public <S extends T> UDTTypeClassInfoImpl<S> getType(Class<S> clazz) {
    if (this.clazz.isAssignableFrom(clazz)) {
      return super.getType(clazz);
    }
    return null;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.UDTRootClassInfoImpl#getType(java.lang.String)
   */
  @Override
  public UDTTypeClassInfoImpl<? extends T> getType(String name) {
    final UDTTypeClassInfoImpl<? extends T> tinfo = super.getType(name);

    if (clazz.isAssignableFrom(tinfo.getObjectClass())) {
      return tinfo;
    }
    return null;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.UDTRootClassInfoImpl#types()
   */
  @Override
  public Stream<UDTTypeClassInfo<? extends T>> types() {
    return super.types()
      .filter(t -> clazz.isAssignableFrom(t.getObjectClass()));
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.UDTRootClassInfoImpl#typeImpls()
   */
  @Override
  public Stream<UDTTypeClassInfoImpl<? extends T>> typeImpls() {
    return super.typeImpls()
      .filter(t -> clazz.isAssignableFrom(t.getObjectClass()));
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.UDTRootClassInfoImpl#getNumTypes()
   */
  @Override
  public int getNumTypes() {
    return (int)types().count();
  }
}

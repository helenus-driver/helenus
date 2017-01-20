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

import com.datastax.driver.core.UDTValue;

import org.helenus.commons.lang3.reflect.ReflectionUtils;
import org.helenus.driver.ObjectConversionException;
import org.helenus.driver.info.UDTTypeClassInfo;
import org.helenus.driver.persistence.Keyspace;
import org.helenus.driver.persistence.UDTRootEntity;

/**
 * The <code>UDTTypeClassInfoImpl</code> class provides information about a
 * particular POJO class.
 *
 * @copyright 2015-2016 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jun 6, 2015 - paouelle - Creation
 *
 * @param <T> The type of POJO represented by this class
 *
 * @since 2.0
 */
public class UDTTypeClassInfoImpl<T>
  extends UDTClassInfoImpl<T> implements UDTTypeClassInfo<T> {
  /**
   * Holds the class info for the UDT root entity this POJO is a type.
   *
   * @author paouelle
   */
  private final UDTRootClassInfoImpl<? super T> rinfo;

  /**
   * Holds a flag indicating if this type was known to the root entity via the
   * @UDTRootEntity annotation or if it was dynamically added later.
   *
   * @author paouelle
   */
  private final boolean dynamic;

  /**
   * Instantiates a new <code>UDTTypeClassInfoImpl</code> object.
   *
   * @author paouelle
   *
   * @param  mgr the non-<code>null</code> statement manager
   * @param  rinfo the class info for the UDT root entity this POJO is a type
   * @param  clazz the class of POJO for which to get a class info object for
   * @param  dynamic <code>true</code> if this type is dynamically being added
   *         to the root; <code>false</code> if it was known to the root via
   *         the @UDTRootEnitty annotation
   * @throws NullPointerException if <code>rinfo</code> is <code>null</code>
   * @throws IllegalArgumentException if <code>clazz</code> doesn't represent
   *         a valid POJO class
   */
  UDTTypeClassInfoImpl(
    StatementManagerImpl mgr,
    UDTRootClassInfoImpl<? super T> rinfo,
    Class<T> clazz,
    boolean dynamic
  ) {
    super(mgr, clazz, UDTRootEntity.class); // search for ctor starting at root
    org.apache.commons.lang3.Validate.isTrue(
      !Modifier.isAbstract(clazz.getModifiers()),
      "UDT type entity class '%s', cannot be abstract", clazz.getSimpleName()
    );
    this.rinfo = rinfo;
    this.dynamic = dynamic;
    // validate the UDT type entity POJO class
    validate(rinfo.getObjectClass());
  }

  /**
   * Validates this UDT type entity class.
   *
   * @author paouelle
   *
   * @param  rclazz the non-<code>null</code> class of POJO for the UDT root element
   * @throws IllegalArgumentException if the POJO class is improperly annotated
   */
  private void validate(Class<? super T> rclazz) {
   // check all tables
   tablesImpl().forEach(
     t -> {
       // check keyspace
       org.apache.commons.lang3.Validate.isTrue(
         ReflectionUtils.findFirstClassAnnotatedWith(
           clazz, Keyspace.class
         ).isAssignableFrom(rclazz),
         "@Keyspace annotation is not defined in UDT root element class '%s' for type class: %s",
         rclazz.getSimpleName(),
         clazz.getSimpleName()
       );
       // check type key
       t.getTypeKey().ifPresent(
         f -> {
           org.apache.commons.lang3.Validate.isTrue(
             f.getDeclaringClass().isAssignableFrom(rclazz),
             "@TypeKey annotation with name '%s' is not defined in UDT root element class '%s' for type class: %s",
             f.getColumnName(),
             rclazz.getSimpleName(),
             clazz.getSimpleName()
           );
         }
       );
     }
   );
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.info.TypeClassInfo#getRoot()
   */
  @Override
  @SuppressWarnings("unchecked")
  public UDTRootClassInfoImpl<? super T> getRoot() {
    return rinfo;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.info.UDTTypeClassInfo#getType()
   */
  @Override
  public String getType() {
    return getName();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.info.TypeClassInfo#isDynamic()
   */
  @Override
  public boolean isDynamic() {
    return dynamic;
  }

  /**
   * Creates a new context for this class info with the given POJO object.
   *
   * @author paouelle
   *
   * @param  object the POJO object
   * @return a non-<code>null</code> newly created context for this class info
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
   * Converts the specified UDT value into a POJO object defined by this
   * class information.
   *
   * @author paouelle
   *
   * @param  uval the UDT value to convert into a POJO
   * @param  type the POJO type extracted from the specified UDT value
   * @return the POJO object corresponding to the given result row or <code>null</code>
   *         if the type doesn't match this type entity name
   * @throws NullPointerException if <code>type</code> is <code>null</code>
   * @throws ObjectConversionException if unable to convert to a POJO
   */
  @SuppressWarnings("unchecked")
  public T getObject(UDTValue uval, String type) {
    if (uval != null) {
      final T t;

      if (getType().equals(type)) { // it is our type
        t = super.getObject(uval);
        // set the type key manually
        getTableImpl().getTypeKey().ifPresent(f -> f.setValue(t, getType()));
        return t;
      }
      final UDTTypeClassInfoImpl<?> tinfo = rinfo.getType(type);

      if (clazz.isAssignableFrom(tinfo.getObjectClass())) {
        // delegate to this sub type info class
        return (T)tinfo.getObject(uval, type);
      }
    }
    return null;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.UDTClassInfoImpl#getObject(com.datastax.driver.core.UDTValue)
   */
  @Override
  public T getObject(UDTValue uval) {
    if (uval == null) {
      return null;
    }
    // create as an instance of this type (ignore any type column persisted - in fact there should be none)
    final T t = super.getObject(uval);

    // set the type key manually
    getTableImpl().getTypeKey().ifPresent(f -> f.setValue(t, getType()));
    return t;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.UDTClassInfoImpl#getObject(java.lang.String, java.util.Map)
   */
  @Override
  public T getObject(String keyspace, Map<String, String> values) {
    if (values == null) {
      return null;
    }
    // create as an instance of this type (ignore any type column persisted - in fact there should be none)
    final T t = super.getObject(keyspace, values);

    // set the type key manually
    getTableImpl().getTypeKey().ifPresent(f -> f.setValue(t, getType()));
    return t;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return (
      getClass().getSimpleName()
      + "[name=" + getName()
      + ",dynamic=" + dynamic
      + ",clazz=" + clazz
      + ",keyspace=" + getKeyspace()
      + ",columns=" + getColumns()
      + ",table=" + getTableImpl()
      + "]"
    );
  }
}

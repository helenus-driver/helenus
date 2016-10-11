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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;

import org.helenus.driver.ObjectConversionException;
import org.helenus.driver.info.FieldInfo;
import org.helenus.driver.info.UDTRootClassInfo;
import org.helenus.driver.info.UDTTypeClassInfo;
import org.helenus.driver.persistence.CQLDataType;
import org.helenus.driver.persistence.UDTRootEntity;

/**
 * The <code>UDTRootClassInfoImpl</code> class provides information about a
 * particular root element POJO class.
 *
 * @copyright 2015-2016 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jun 6, 2016 - paouelle - Creation
 *
 * @param <T> The type of POJO represented by this root class
 *
 * @since 1.0
 */
@lombok.ToString(callSuper=true, of={"ntypes"})
@lombok.EqualsAndHashCode(callSuper=true)
public class UDTRootClassInfoImpl<T>
  extends UDTClassInfoImpl<T>
  implements UDTRootClassInfo<T> {
  /**
   * The <code>Context</code> class extends the {@link ClassInfoImpl.Context}.
   *
   * @copyright 2015-2016 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jun 6, 2016 - paouelle - Creation
   *
   * @since 1.0
   */
  public class Context extends ClassInfoImpl<T>.Context {
    /**
     * Holds the contexts for all defined types.
     *
     * @author paouelle
     */
    private final Map<Class<? extends T>, UDTTypeClassInfoImpl<? extends T>.Context> contexts;

    /**
     * Instantiates a new <code>Context</code> object.
     *
     * @author paouelle
     */
    @SuppressWarnings("synthetic-access")
    Context() {
      this.contexts = ctypes.values().stream()
        .collect(
          Collectors.toMap(
            tcinfo -> tcinfo.getObjectClass(),
            tcinfo -> tcinfo.newContext()
          )
        );
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClassInfoImpl.Context#addKeyspaceKey(java.lang.String, java.lang.Object)
     */
    @Override
    public void addKeyspaceKey(String name, Object value) {
      super.addKeyspaceKey(name, value);
      contexts.values().forEach(
        tc -> tc.addKeyspaceKey(name, value)
      );
    }
  }

  /**
   * The <code>POJOContext</code> class provides a specific context for the POJO
   * as referenced while building an insert or update statement.
   *
   * @copyright 2015-2016 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jun 6, 2016 - paouelle - Creation
   *
   * @since 1.0
   */
  public class POJOContext extends UDTClassInfoImpl<T>.POJOContext {
    /**
     * Holds the corresponding type context for the POJO.
     *
     * @author paouelle
     */
    private final UDTTypeClassInfoImpl<? extends T>.POJOContext tcontext;

    /**
     * Instantiates a new <code>POJOContext</code> object.
     *
     * @author paouelle
     *
     * @param  object the POJO object
     * @throws NullPointerException if <code>object</code> is <code>null</code>
     * @throws IllegalArgumentException if <code>object</code> is not of the
     *         appropriate class
     */
    @SuppressWarnings("synthetic-access")
    public POJOContext(T object) {
      super(object);
      final UDTTypeClassInfoImpl<? extends T> tcinfo = ctypes.get(object.getClass());

      org.apache.commons.lang3.Validate.isTrue(
        tcinfo != null,
        "invalid POJO class '%s'; expecting one of %s",
        object.getClass().getName(), ctypes.keySet()
      );
      this.tcontext = tcinfo.newContextFromRoot(object);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClassInfoImpl.POJOContext#getKeyspaceKeyValues()
     */
    @Override
    public Map<String, Pair<Object, CQLDataType>> getKeyspaceKeyValues() {
      return tcontext.getKeyspaceKeyValues();
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClassInfoImpl.Context#getKeyspace()
     */
    @Override
    public String getKeyspace() {
      return tcontext.getKeyspace();
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClassInfoImpl.Context#addKeyspaceKey(java.lang.String, java.lang.Object)
     */
    @Override
    public void addKeyspaceKey(String name, Object value) {
      tcontext.addKeyspaceKey(name, value);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClassInfoImpl.Context#getObject(com.datastax.driver.core.Row)
     */
    @Override
    public T getObject(Row row) {
      return tcontext.getObject(row);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClassInfoImpl.Context#getInitialObjects()
     */
    @Override
    public Collection<T> getInitialObjects() {
      return tcontext.getInitialObjects().stream().collect(Collectors.toList());
    }
  }

  /**
   * Holds all the type element subclasses for this root element base class
   * keyed by their type class.
   *
   * @author paouelle
   */
  private final Map<Class<? extends T>, UDTTypeClassInfoImpl<? extends T>> ctypes;

  /**
   * Holds all the type element subclasses for this root element base class
   * keyed by their type names.
   *
   * @author paouelle
   */
  private final Map<String, UDTTypeClassInfoImpl<? extends T>> ntypes;

  /**
   * Instantiates a new <code>UDTRootClassInfoImpl</code> object.
   *
   * @author paouelle
   *
   * @param  mgr the non-<code>null</code> statement manager
   * @param  clazz the root class of POJO for which to get a class info object for
   * @throws NullPointerException if <code>clazz</code> is <code>null</code>
   * @throws IllegalArgumentException if <code>clazz</code> or any of its type
   *         classes don't represent valid POJO classes
   */
  UDTRootClassInfoImpl(StatementManagerImpl mgr, Class<T> clazz) {
    super(mgr, clazz, UDTRootEntity.class);
    // first make sure the class is abstract
    org.apache.commons.lang3.Validate.isTrue(
      Modifier.isAbstract(clazz.getModifiers()),
      "UDT root entity class '%s', must be abstract", clazz.getSimpleName()
    );
    this.ctypes = findTypeInfos(mgr);
    this.ntypes = ctypes.values().stream()
      .collect(Collectors.toMap(tcinfo -> tcinfo.getType(), tcinfo -> tcinfo));
    validateAndComplementSchema();
  }

  /**
   * Instantiates a new <code>UDTRootClassInfoImpl</code> object.
   *
   * @author paouelle
   *
   * @param  rinfo the class info for the root entity this POJO is a subclass
   *         and for which we will be linking to
   * @param  clazz the subclass of POJO for which to link the root class info object for
   * @throws NullPointerException if <code>clazz</code> is <code>null</code>
   * @throws IllegalArgumentException if <code>clazz</code> or any of its type
   *         classes don't represent valid POJO classes
   */
  @SuppressWarnings({"cast", "unchecked", "rawtypes"})
  UDTRootClassInfoImpl(UDTRootClassInfoImpl<? super T> rinfo, Class<T> clazz) {
    super((UDTClassInfoImpl<T>)rinfo, clazz);
    // first make sure the class is abstract
    org.apache.commons.lang3.Validate.isTrue(
      Modifier.isAbstract(clazz.getModifiers()),
      "UDT root entity class '%s', must be abstract", clazz.getSimpleName()
    );
    this.ctypes = (Map<Class<? extends T>, UDTTypeClassInfoImpl<? extends T>>)(Map)rinfo.ctypes; // reference the same map
    this.ntypes = (Map<String, UDTTypeClassInfoImpl<? extends T>>)(Map)rinfo.ntypes; // reference the same map
  }

  /**
   * Finds the class info for all type classes for this UDT root element class.
   *
   * @author paouelle
   *
   * @param  mgr the non-<code>null</code> statement manager
   * @return the non-<code>null</code> map of class info for all types
   * @throws IllegalArgumentException if any of the type classes are invalid
   */
  private Map<Class<? extends T>, UDTTypeClassInfoImpl<? extends T>> findTypeInfos(
    StatementManagerImpl mgr
  ) {
    org.apache.commons.lang3.Validate.notNull(clazz, "invalid null UDT root POJO class");
    final UDTRootEntity re = clazz.getAnnotation(UDTRootEntity.class);
    final int hsize = re.types().length * 3 / 2;
    final Map<Class<? extends T>, UDTTypeClassInfoImpl<? extends T>> types
      = new HashMap<>(hsize);
    final Set<String> names = new HashSet<>(hsize);

    for (final Class<?> type: re.types()) {
      org.apache.commons.lang3.Validate.isTrue(
        clazz.isAssignableFrom(type),
        "type class '%s' must extends UDT root element class: %s",
        type.getName(), clazz.getName()
      );
      @SuppressWarnings("unchecked") // tested above!
      final UDTTypeClassInfoImpl<? extends T> tcinfo = new UDTTypeClassInfoImpl<>(
        mgr, (UDTRootClassInfoImpl<? super T>)this, (Class<? extends T>)type, false
       );

      org.apache.commons.lang3.Validate.isTrue(
        types.put(tcinfo.getObjectClass(), tcinfo) == null,
        "duplicate type element class '%s' defined for UDT root element class '%s'",
        type.getSimpleName(), clazz.getSimpleName()
      );
      org.apache.commons.lang3.Validate.isTrue(
        names.add(tcinfo.getType()),
        "duplicate type name '%s' defined by class '%s' for UDT root element class '%s'",
        tcinfo.getType(), type.getSimpleName(), clazz.getSimpleName()
      );
    }
    return types;
  }

  /**
   * Validates this UDT root entity class and complement its schema with the
   * additional columns defined by the type entities.
   *
   * @author paouelle
   *
   * @throws IllegalArgumentException if the POJO class is improperly annotated
   */
  private void validateAndComplementSchema() {
    final TableInfoImpl<T> table = getTableImpl();

    // check type key
    org.apache.commons.lang3.Validate.isTrue(
      table.getTypeKey().isPresent(),
      "%s must annotate one field as a type key for table '%s'",
      clazz.getSimpleName(), table.getName()
    );
    ctypes.values().forEach(
      tcinfo -> {
        tcinfo.getTableImpl().getNonPrimaryKeys().stream()
          .forEach(c -> table.addNonPrimaryColumn(c));
      }
    );
    // now that we are done, make sure that all added columns defined in subclasses
    // that are marked mandatory are as such for all subclasses, if not
    // then we need to downgrade them to non-mandatory
    getTableImpl().getColumnsImpl().forEach(c -> {
      if (c.isMandatory()) {
        if (!ctypes.values().stream()
              .allMatch(tcinfo -> tcinfo.getTableImpl().getColumn(c.getColumnName())
                .map(FieldInfo::isMandatory)
                .orElse(false))) {
          table.forceNonPrimaryColumnToNotBeMandatory(c);
        }
      }
    });
  }

  /**
   * Adds a new type POJO class to this UDT root entity.
   *
   * @author paouelle
   *
   * @param  mgr the non-<code>null</code> statement manager
   * @param  type the type POJO class
   * @return the non-<code>null</code> class info for the specified type
   * @throws NullPointerException if <code>clazz</code> is <code>null</code>
   * @throws IllegalArgumentException if the type class is invalid
   */
  UDTTypeClassInfoImpl<? extends T> addType(StatementManagerImpl mgr, Class<?> type) {
    org.apache.commons.lang3.Validate.isTrue(
      clazz.isAssignableFrom(type),
      "type class '%s' must extends UDT root element class: %s",
      type.getName(), clazz.getName()
    );
    @SuppressWarnings("unchecked") // tested above!
    final UDTTypeClassInfoImpl<? extends T> tcinfo
      = new UDTTypeClassInfoImpl<>(mgr, this, (Class<? extends T>)type, true);

    org.apache.commons.lang3.Validate.isTrue(
      ctypes.put(tcinfo.getObjectClass(), tcinfo) == null,
      "duplicate type element class '%s' defined for UDT root element class '%s'",
      type.getSimpleName(), clazz.getSimpleName()
    );
    org.apache.commons.lang3.Validate.isTrue(
      ntypes.put(tcinfo.getType(), tcinfo) == null,
      "duplicate type name '%s' defined by class '%s' for UDT root element class '%s'",
      tcinfo.getType(), type.getSimpleName(), clazz.getSimpleName()
    );
    // make sure this type doesn't define any new columns or different type of columns
    // make sure there are no new non-primary columns and make sure that for
    tcinfo.getTableImpl().getNonPrimaryKeys().stream().forEach(c -> {
      final FieldInfoImpl<? extends T> rc = getTableImpl().getColumnImpl(c.getColumnName());

      org.apache.commons.lang3.Validate.isTrue(
        rc != null,
        "UDT root element '%s' doesn't define column '%s' in pojo '%s'",
        clazz.getSimpleName(),
        c.getColumnName(),
        type.getSimpleName()
      );
      // already defined so skip it but not before making sure it is compatible
      if (rc.getDeclaringClass().equals(c.getDeclaringClass())) {
        // same exact field so we are good
        return;
      }
      // check data type
      org.apache.commons.lang3.Validate.isTrue(
        rc.getDataType().getMainType() == c.getDataType().getMainType(),
        "incompatible type columns '%s.%s' of type '%s' and '%s.%s' of type '%s' in pojo '%s'",
        c.getDeclaringClass().getSimpleName(),
        c.getName(),
        c.getDataType().getMainType(),
        rc.getDeclaringClass().getSimpleName(),
        rc.getName(),
        rc.getDataType().getMainType(),
        type.getSimpleName()
      );
    });
    return tcinfo;
  }

  /**
   * Creates a new POJO subclass to this UDT root entity.
   *
   * @author paouelle
   *
   * @param  mgr the non-<code>null</code> statement manager
   * @param  type the POJO subclass
   * @return the non-<code>null</code> class info for the specified subclass
   * @throws NullPointerException if <code>clazz</code> is <code>null</code>
   * @throws IllegalArgumentException if the subclass is invalid
   */
  UDTSubClassInfoImpl<? extends T> newSubClass(
    StatementManagerImpl mgr, Class<?> type
  ) {
    org.apache.commons.lang3.Validate.isTrue(
      clazz.isAssignableFrom(type),
      "subclass '%s' must extends root element class: %s",
      type.getName(), clazz.getName()
    );
    @SuppressWarnings("unchecked") // tested above!
    final UDTSubClassInfoImpl<? extends T> tcinfo
      = new UDTSubClassInfoImpl<>(this, (Class<? extends T>)type);

    org.apache.commons.lang3.Validate.isTrue(
      !ctypes.containsKey(tcinfo.getObjectClass()),
      "a type element class '%s' is already defined for UDT root element class '%s'",
      type.getSimpleName(), clazz.getSimpleName()
    );
    // make sure this type doesn't define any new columns or different type of columns
    // make sure the table is defined in root
    final TableInfoImpl<? extends T> table = tcinfo.getTableImpl();
    final TableInfoImpl<? extends T> rt = getTableImpl();

    // make sure there are no new non-primary columns and make sure that for
    // those defined they are of the same type
    table.getNonPrimaryKeys().stream().forEach(c -> {
      final FieldInfoImpl<? extends T> rc = rt.getColumnImpl(c.getColumnName());

      org.apache.commons.lang3.Validate.isTrue(
        rc != null,
        "UDT root element '%s' doesn't define column '%s' in pojo '%s'",
        clazz.getSimpleName(),
        c.getColumnName(),
        type.getSimpleName()
      );
      // already defined so skip it but not before making sure it is compatible
      if (rc.getDeclaringClass().equals(c.getDeclaringClass())) {
        // same exact field so we are good
        return;
      }
      // check data type
      org.apache.commons.lang3.Validate.isTrue(
        rc.getDataType().getMainType() == c.getDataType().getMainType(),
        "incompatible type columns '%s.%s' of type '%s' and '%s.%s' of type '%s' in subclass '%s'",
        c.getDeclaringClass().getSimpleName(),
        c.getName(),
        c.getDataType().getMainType(),
        rc.getDeclaringClass().getSimpleName(),
        rc.getName(),
        rc.getDataType().getMainType(),
        type.getSimpleName()
      );
    });
    return tcinfo;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.ClassInfoImpl#objectClasses()
   */
  @Override
  public Stream<Class<? extends T>> objectClasses() {
    return Stream.concat(
      Stream.of(clazz),
      ctypes.values().stream()
        .sequential()
        .map(t -> t.getObjectClass())
    ).sequential();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.UDTClassInfoImpl#classInfos()
   */
  @Override
  public Stream<ClassInfoImpl<? extends T>> classInfos() {
    return Stream.concat(
      Stream.of((ClassInfoImpl<T>)this), ctypes.values().stream().sequential()
    ).sequential();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.ClassInfoImpl#newContext()
   */
  @Override
  public Context newContext() {
    return new Context();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.ClassInfoImpl#newContext(java.lang.Object)
   */
  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public POJOContext newContext(T object) {
    return new POJOContext(object);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.info.UDTRootClassInfo#getType(java.lang.Class)
   */
  @Override
  @SuppressWarnings("unchecked")
  public <S extends T> UDTTypeClassInfoImpl<S> getType(Class<S> clazz) {
    return (UDTTypeClassInfoImpl<S>)ctypes.get(clazz);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.info.UDTRootClassInfo#getType(java.lang.String)
   */
  @Override
  public UDTTypeClassInfoImpl<? extends T> getType(String name) {
    return ntypes.get(name);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.info.UDTRootClassInfo#types()
   */
  @SuppressWarnings({"cast", "rawtypes", "unchecked"})
  @Override
  public Stream<UDTTypeClassInfo<? extends T>> types() {
    return (Stream<UDTTypeClassInfo<? extends T>>)(Stream)ntypes.values().stream();
  }

  /**
   * Gets all type entities defined from this root entity.
   *
   * @author paouelle
   *
   * @return a stream of all type entities defined from this root entity
   */
  public Stream<UDTTypeClassInfoImpl<? extends T>> typeImpls() {
   return ntypes.values().stream();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.info.UDTRootClassInfo#getNumTypes()
   */
  @Override
  public int getNumTypes() {
    return ctypes.size();
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
    // get the table for this UDT
    final TableInfoImpl<T> table = getTableImpl();

    // extract the type so we know which object we are creating
    for (final UserType.Field coldef: uval.getType()) {
      // find the field in the table for this column
      final FieldInfoImpl<T> field = table.getColumnImpl(coldef.getName());

      if ((field != null) && field.isTypeKey()) { // get the POJO type
        final String type = Objects.toString(field.decodeValue(uval), null);
        final UDTTypeClassInfoImpl<? extends T> tcinfo = ntypes.get(type);

        if (tcinfo == null) {
          throw new ObjectConversionException(
            clazz, uval, "unknown POJO type: " + type
          );
        }
        return tcinfo.getObject(uval, type);
      }
    }
    throw new ObjectConversionException(clazz, uval, "missing POJO type column");
  }
}

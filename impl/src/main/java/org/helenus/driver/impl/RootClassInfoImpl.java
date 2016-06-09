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

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.Row;

import org.helenus.driver.ObjectConversionException;
import org.helenus.driver.info.RootClassInfo;
import org.helenus.driver.info.TypeClassInfo;
import org.helenus.driver.persistence.CQLDataType;
import org.helenus.driver.persistence.RootEntity;

/**
 * The <code>RootClassInfoImpl</code> class provides information about a
 * particular root element POJO class.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 19, 2015 - paouelle - Creation
 *
 * @param <T> The type of POJO represented by this root class
 *
 * @since 1.0
 */
@lombok.ToString(callSuper=true, of={"ntypes"})
@lombok.EqualsAndHashCode(callSuper=true)
public class RootClassInfoImpl<T>
  extends ClassInfoImpl<T>
  implements RootClassInfo<T> {
  /**
   * The <code>Context</code> class extends the {@link ClassInfoImpl.Context}.
   *
   * @copyright 2015-2015 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 19, 2015 - paouelle - Creation
   *
   * @since 1.0
   */
  public class Context extends ClassInfoImpl<T>.Context {
    /**
     * Holds the contexts for all defined types.
     *
     * @author paouelle
     */
    private final Map<Class<? extends T>, TypeClassInfoImpl<? extends T>.Context> contexts;

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
     * @see org.helenus.driver.impl.ClassInfoImpl.Context#addSuffix(java.lang.String, java.lang.Object)
     */
    @Override
    public void addSuffix(String suffix, Object value) {
      super.addSuffix(suffix, value);
      contexts.values().forEach(
        tc -> tc.addSuffix(suffix, value)
      );
    }
  }

  /**
   * The <code>POJOContext</code> class extends the
   * {@link ClassInfoImpl.POJOContext}.
   *
   * @copyright 2015-2015 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 19, 2015 - paouelle - Creation
   *
   * @since 1.0
   */
  public class POJOContext extends ClassInfoImpl<T>.POJOContext {
    /**
     * Holds the corresponding type context for the POJO.
     *
     * @author paouelle
     */
    private final TypeClassInfoImpl<? extends T>.POJOContext tcontext;

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
      final TypeClassInfoImpl<? extends T> tcinfo = ctypes.get(object.getClass());

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
     * @see org.helenus.driver.impl.ClassInfoImpl.POJOContext#getColumnValues(java.lang.String)
     */
    @Override
    public Map<String, Pair<Object, CQLDataType>> getColumnValues(String tname) {
      return tcontext.getColumnValues(tname);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClassInfoImpl.POJOContext#getPartitionKeyColumnValues(java.lang.String)
     */
    @Override
    public Map<String, Pair<Object, CQLDataType>> getPartitionKeyColumnValues(String tname) {
      return tcontext.getPartitionKeyColumnValues(tname);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClassInfoImpl.POJOContext#getSuffixAndPartitionKeyColumnValues(java.lang.String)
     */
    @Override
    public Map<String, Pair<Object, CQLDataType>> getSuffixAndPartitionKeyColumnValues(String tname) {
      return tcontext.getSuffixAndPartitionKeyColumnValues(tname);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClassInfoImpl.POJOContext#getPrimaryKeyColumnValues(java.lang.String)
     */
    @Override
    public Map<String, Pair<Object, CQLDataType>> getPrimaryKeyColumnValues(String tname) {
      return tcontext.getPrimaryKeyColumnValues(tname);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClassInfoImpl.POJOContext#getSuffixKeyValues()
     */
    @Override
    public Map<String, Pair<Object, CQLDataType>> getSuffixKeyValues() {
      return tcontext.getSuffixKeyValues();
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClassInfoImpl.POJOContext#getSuffixAndPrimaryKeyColumnValues(java.lang.String)
     */
    @Override
    public Map<String, Pair<Object, CQLDataType>> getSuffixAndPrimaryKeyColumnValues(String tname) {
      return tcontext.getSuffixAndPrimaryKeyColumnValues(tname);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClassInfoImpl.POJOContext#getMandatoryAndPrimaryKeyColumnValues(java.lang.String)
     */
    @Override
    public Map<String, Pair<Object, CQLDataType>> getMandatoryAndPrimaryKeyColumnValues(
      String tname
    ) {
      return tcontext.getMandatoryAndPrimaryKeyColumnValues(tname);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClassInfoImpl.POJOContext#getNonPrimaryKeyColumnNonEncodedValues(java.lang.String)
     */
    @Override
    public Map<String, Pair<Object, CQLDataType>> getNonPrimaryKeyColumnNonEncodedValues(String tname) {
      return tcontext.getNonPrimaryKeyColumnNonEncodedValues(tname);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClassInfoImpl.POJOContext#getColumnValue(java.lang.String, java.lang.CharSequence)
     */
    @Override
    public Pair<Object, CQLDataType> getColumnValue(String tname, CharSequence name) {
      return tcontext.getColumnValue(tname, name);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClassInfoImpl.POJOContext#getColumnValues(java.lang.String, java.lang.Iterable)
     */
    @Override
    public Map<String, Pair<Object, CQLDataType>> getColumnValues(
      String tname, Iterable<CharSequence> names
    ) {
      return tcontext.getColumnValues(tname, names);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClassInfoImpl.POJOContext#getColumnValues(java.lang.String, java.lang.CharSequence[])
     */
    @Override
    public Map<String, Pair<Object, CQLDataType>> getColumnValues(
      String tname, CharSequence... names
    ) {
      return tcontext.getColumnValues(tname, names);
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
     * @see org.helenus.driver.impl.ClassInfoImpl.Context#addSuffix(java.lang.String, java.lang.Object)
     */
    @Override
    public void addSuffix(String suffix, Object value) {
      tcontext.addSuffix(suffix, value);
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
  private final Map<Class<? extends T>, TypeClassInfoImpl<? extends T>> ctypes;

  /**
   * Holds all the type element subclasses for this root element base class
   * keyed by their type names.
   *
   * @author paouelle
   */
  private final Map<String, TypeClassInfoImpl<? extends T>> ntypes;

  /**
   * Instantiates a new <code>RootClassInfoImpl</code> object.
   *
   * @author paouelle
   *
   * @param  mgr the non-<code>null</code> statement manager
   * @param  clazz the root class of POJO for which to get a class info object for
   * @throws NullPointerException if <code>clazz</code> is <code>null</code>
   * @throws IllegalArgumentException if <code>clazz</code> or any of its type
   *         classes don't represent valid POJO classes
   */
  RootClassInfoImpl(StatementManagerImpl mgr, Class<T> clazz) {
    super(mgr, clazz, RootEntity.class);
    // first make sure the class is abstract
    org.apache.commons.lang3.Validate.isTrue(
      Modifier.isAbstract(clazz.getModifiers()),
      "root entity class '%s', must be abstract", clazz.getSimpleName()
    );
    this.ctypes = findTypeInfos(mgr);
    this.ntypes = ctypes.values().stream()
      .collect(Collectors.toMap(tcinfo -> tcinfo.getType(), tcinfo -> tcinfo));
    validateAndComplementSchema();
  }

  /**
   * Instantiates a new <code>RootClassInfoImpl</code> object.
   *
   * @author paouelle
   *
   * @param  rinfo the class info for the root entity this POJO is a subvlass
   *         and for which we will be linking to
   * @param  clazz the subclass of POJO for which to link the root class info object for
   * @throws NullPointerException if <code>clazz</code> is <code>null</code>
   * @throws IllegalArgumentException if <code>clazz</code> or any of its type
   *         classes don't represent valid POJO classes
   */
  @SuppressWarnings({"cast", "unchecked", "rawtypes"})
  RootClassInfoImpl(RootClassInfoImpl<? super T> rinfo, Class<T> clazz) {
    super((ClassInfoImpl<T>)rinfo, clazz);
    // first make sure the class is abstract
    org.apache.commons.lang3.Validate.isTrue(
      Modifier.isAbstract(clazz.getModifiers()),
      "root entity class '%s', must be abstract", clazz.getSimpleName()
    );
    this.ctypes = (Map<Class<? extends T>, TypeClassInfoImpl<? extends T>>)(Map)rinfo.ctypes; // reference the same map
    this.ntypes = (Map<String, TypeClassInfoImpl<? extends T>>)(Map)rinfo.ntypes; // reference the same map
  }

  /**
   * Finds the class info for all type classes for this root element class.
   *
   * @author paouelle
   *
   * @param  mgr the non-<code>null</code> statement manager
   * @return the non-<code>null</code> map of class info for all types
   * @throws IllegalArgumentException if any of the type classes are invalid
   */
  private Map<Class<? extends T>, TypeClassInfoImpl<? extends T>> findTypeInfos(
    StatementManagerImpl mgr
  ) {
    org.apache.commons.lang3.Validate.notNull(clazz, "invalid null root POJO class");
    final RootEntity re = clazz.getAnnotation(RootEntity.class);
    final int hsize = re.types().length * 3 / 2;
    final Map<Class<? extends T>, TypeClassInfoImpl<? extends T>> types
      = new HashMap<>(hsize);
    final Set<String> names = new HashSet<>(hsize);

    for (final Class<?> type: re.types()) {
      org.apache.commons.lang3.Validate.isTrue(
        clazz.isAssignableFrom(type),
        "type class '%s' must extends root element class: %s",
        type.getName(), clazz.getName()
      );
      @SuppressWarnings("unchecked") // tested above!
      final TypeClassInfoImpl<? extends T> tcinfo = new TypeClassInfoImpl<>(
        mgr, (RootClassInfoImpl<? super T>)this, (Class<? extends T>)type, false
       );

      org.apache.commons.lang3.Validate.isTrue(
        types.put(tcinfo.getObjectClass(), tcinfo) == null,
        "duplicate type element class '%s' defined for root element class '%s'",
        type.getSimpleName(), clazz.getSimpleName()
      );
      org.apache.commons.lang3.Validate.isTrue(
        names.add(tcinfo.getType()),
        "duplicate type name '%s' defined by class '%s' for root element class '%s'",
        tcinfo.getType(), type.getSimpleName(), clazz.getSimpleName()
      );
    }
    return types;
  }

  /**
   * Validates this root entity class and complement its schema with the
   * additional columns defined by the type entities.
   *
   * @author paouelle
   *
   * @throws IllegalArgumentException if the POJO class is improperly annotated
   */
  private void validateAndComplementSchema() {
    // check all tables
    tablesImpl().forEach(
      t -> {
        // check type key
        org.apache.commons.lang3.Validate.isTrue(
          t.getTypeKey().isPresent(),
          "%s must annotate one field as a type key for table '%s'",
          clazz.getSimpleName(), t.getName()
        );
        ctypes.values().forEach(
          tcinfo -> {
            tcinfo.getTableImpl(t.getName()).getNonPrimaryKeys().stream()
              .forEach(
                c -> t.addNonPrimaryColumn(c)
              );
          }
        );
      }
    );
  }

  /**
   * Adds a new type POJO class to this root entity.
   *
   * @author paouelle
   *
   * @param  mgr the non-<code>null</code> statement manager
   * @param  type the type POJO class
   * @return the non-<code>null</code> class info for the specified type
   * @throws NullPointerException if <code>clazz</code> is <code>null</code>
   * @throws IllegalArgumentException if the type class is invalid
   */
  TypeClassInfoImpl<? extends T> addType(StatementManagerImpl mgr, Class<?> type) {
    org.apache.commons.lang3.Validate.isTrue(
      clazz.isAssignableFrom(type),
      "type class '%s' must extends root element class: %s",
      type.getName(), clazz.getName()
    );
    @SuppressWarnings("unchecked") // tested above!
    final TypeClassInfoImpl<? extends T> tcinfo
      = new TypeClassInfoImpl<>(mgr, this, (Class<? extends T>)type, true);

    org.apache.commons.lang3.Validate.isTrue(
      ctypes.put(tcinfo.getObjectClass(), tcinfo) == null,
      "duplicate type element class '%s' defined for root element class '%s'",
      type.getSimpleName(), clazz.getSimpleName()
    );
    org.apache.commons.lang3.Validate.isTrue(
      ntypes.put(tcinfo.getType(), tcinfo) == null,
      "duplicate type name '%s' defined by class '%s' for root element class '%s'",
      tcinfo.getType(), type.getSimpleName(), clazz.getSimpleName()
    );
    // make sure this type doesn't define any new columns or different type of columns
    tcinfo.getTablesImpl().forEach(t -> {
      // make sure the table is defined in root
      final TableInfoImpl<? extends T> rt = getTableImpl(t.getName());

      // make sure there are no new non-primary columns and make sure that for
      // those defined they are of the same type
      t.getNonPrimaryKeys().stream().forEach(c -> {
        final FieldInfoImpl<? extends T> rc = rt.getColumnImpl(c.getColumnName());

        org.apache.commons.lang3.Validate.isTrue(
          rc != null,
          "root element '%s' doesn't define column '%s' in pojo '%s'",
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
          "incompatible type columns '%s.%s' of type '%s' and '%s.%s' of type '%s' in table '%s' in pojo '%s'",
          c.getDeclaringClass().getSimpleName(),
          c.getName(),
          c.getDataType().getMainType(),
          rc.getDeclaringClass().getSimpleName(),
          rc.getName(),
          rc.getDataType().getMainType(),
          t.getName(),
          type.getSimpleName()
        );
      });
    });
    return tcinfo;
  }

  /**
   * Creates a new POJO subclass to this root entity.
   *
   * @author paouelle
   *
   * @param  mgr the non-<code>null</code> statement manager
   * @param  type the POJO subclass
   * @return the non-<code>null</code> class info for the specified subclass
   * @throws NullPointerException if <code>clazz</code> is <code>null</code>
   * @throws IllegalArgumentException if the subclass is invalid
   */
  SubClassInfoImpl<? extends T> newSubClass(
    StatementManagerImpl mgr, Class<?> type
  ) {
    org.apache.commons.lang3.Validate.isTrue(
      clazz.isAssignableFrom(type),
      "subclass '%s' must extends root element class: %s",
      type.getName(), clazz.getName()
    );
    @SuppressWarnings("unchecked") // tested above!
    final SubClassInfoImpl<? extends T> tcinfo
      = new SubClassInfoImpl<>(this, (Class<? extends T>)type);

    org.apache.commons.lang3.Validate.isTrue(
      !ctypes.containsKey(tcinfo.getObjectClass()),
      "a type element class '%s' is already defined for root element class '%s'",
      type.getSimpleName(),
      clazz.getSimpleName()
    );
    // make sure this type doesn't define any new columns or different type of columns
    tcinfo.getTablesImpl().forEach(t -> {
      // make sure the table is defined in root
      final TableInfoImpl<? extends T> rt = getTableImpl(t.getName());

      // make sure there are no new non-primary columns and make sure that for
      // those defined they are of the same type
      t.getNonPrimaryKeys().stream().forEach(c -> {
        final FieldInfoImpl<? extends T> rc = rt.getColumnImpl(c.getColumnName());

        org.apache.commons.lang3.Validate.isTrue(
          rc != null,
          "root element '%s' doesn't define column '%s' in pojo '%s'",
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
          "incompatible type columns '%s.%s' of type '%s' and '%s.%s' of type '%s' in table '%s' in subclass '%s'",
          c.getDeclaringClass().getSimpleName(),
          c.getName(),
          c.getDataType().getMainType(),
          rc.getDeclaringClass().getSimpleName(),
          rc.getName(),
          rc.getDataType().getMainType(),
          t.getName(),
          type.getSimpleName()
        );
      });
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
   * @see org.helenus.driver.impl.ClassInfoImpl#classInfos()
   */
  @Override
  public Stream<ClassInfoImpl<? extends T>> classInfos() {
    return Stream.concat(
      Stream.of(this), ctypes.values().stream().sequential()
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
   * @see org.helenus.driver.info.RootClassInfo#getType(java.lang.Class)
   */
  @Override
  @SuppressWarnings("unchecked")
  public <S extends T> TypeClassInfoImpl<S> getType(Class<S> clazz) {
    return (TypeClassInfoImpl<S>)ctypes.get(clazz);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.info.RootClassInfo#getType(java.lang.String)
   */
  @Override
  public TypeClassInfoImpl<? extends T> getType(String name) {
    return ntypes.get(name);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.info.RootClassInfo#types()
   */
  @SuppressWarnings({"cast", "rawtypes", "unchecked"})
  @Override
  public Stream<TypeClassInfo<? extends T>> types() {
    return (Stream<TypeClassInfo<? extends T>>)(Stream)ntypes.values().stream();
  }

  /**
   * Gets all type entities defined from this root entity.
   *
   * @author paouelle
   *
   * @return a stream of all type entities defined from this root entity
   */
  public Stream<TypeClassInfoImpl<? extends T>> typeImpls() {
   return ntypes.values().stream();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.info.RootClassInfo#getNumTypes()
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
   * @see org.helenus.driver.impl.ClassInfoImpl#getObject(com.datastax.driver.core.Row, java.util.Map)
   */
  @Override
  public T getObject(Row row, Map<String, Object> suffixes) {
    if (row == null) {
      return null;
    }
    // extract the type so we know which object we are creating
    for (final ColumnDefinitions.Definition coldef: row.getColumnDefinitions()) {
      // find the table for this column
      final TableInfoImpl<T> table = (TableInfoImpl<T>)getTable(coldef.getTable());

      if (table != null) {
        // find the field in the table for this column
        final FieldInfoImpl<T> field = table.getColumnImpl(coldef.getName());

        if ((field != null) && field.isTypeKey()) { // get the POJO type
          final String type = Objects.toString(field.decodeValue(row), null);
          final TypeClassInfoImpl<? extends T> tcinfo = ntypes.get(type);

          if (tcinfo == null) {
            throw new ObjectConversionException(
              clazz, row, "unknown POJO type: " + type
            );
          }
          return tcinfo.getObject(row, type, suffixes);
        }
      }
    }
    throw new ObjectConversionException(clazz, row, "missing POJO type column");
  }
}

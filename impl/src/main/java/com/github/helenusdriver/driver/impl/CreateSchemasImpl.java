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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.github.helenusdriver.commons.lang3.reflect.ReflectionUtils;
import com.github.helenusdriver.driver.Clause;
import com.github.helenusdriver.driver.CreateSchemas;
import com.github.helenusdriver.driver.StatementBridge;
import com.github.helenusdriver.driver.VoidFuture;
import com.github.helenusdriver.driver.info.ClassInfo;
import com.github.helenusdriver.persistence.Keyspace;
import org.reflections.Reflections;

/**
 * The <code>CreateSchemasImpl</code> class provides support for a statement
 * which will create all the required elements (keyspace, tables, and indexes)
 * to support the schema for a given package of POJOs. It will take care of
 * creating the required keyspace, tables, and indexes.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 19, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
public class CreateSchemasImpl
  extends SequenceStatementImpl<Void, VoidFuture, Void>
  implements CreateSchemas {
  /**
   * Holds the package for all POJO classes for which to create schemas.
   *
   * @author paouelle
   */
  private final String pkg;

  /**
   * Flag indicating if only POJOs with keyspace names that can be computed
   * based on exactly the set of suffixes provided should be considered.
   *
   * @author paouelle
   */
  private final boolean matching;

  /**
   * Set of POJO class infos with their keyspace to be created.
   *
   * @author paouelle
   */
  private final Map<Keyspace, List<ClassInfoImpl<?>>> keyspaces;

  /**
   * Holds the cache of contexts for POJOs that will have schemas created.
   *
   * @author paouelle
   */
  private volatile List<ClassInfoImpl<?>.Context> contexts;

  /**
   * Flag indicating if the "IF NOT EXIST" option has been selected.
   *
   * @author paouelle
   */
  private volatile boolean ifNotExists;

  /**
   * Holds the where statement part.
   *
   * @author paouelle
   */
  private final WhereImpl where;

  /**
   * Instantiates a new <code>CreateSchemaImpl</code> object.
   *
   * @author paouelle
   *
   * @param  pkg the package where to find all POJO classes to create schemas for
   *         associated with this statement
   * @param  matching <code>true</code> to only consider POJOs with keyspace names
   *         that can be computed with exactly the set of suffixes provided
   * @param  mgr the non-<code>null</code> statement manager
   * @param  bridge the non-<code>null</code> statement bridge
   * @throws NullPointerException if <code>pkg</code> is <code>null</code>
   * @throws IllegalArgumentException if an @Entity or @RootEntity annotated
   *         class is missing the @Keyspace annotation or two entities defines
   *         the same keyspace with different options or an entity class doesn't
   *         represent a valid POJO class or if no entities are found
   */
  public CreateSchemasImpl(
    String pkg,
    boolean matching,
    StatementManagerImpl mgr,
    StatementBridge bridge
  ) {
    super(Void.class, (String)null, mgr, bridge);
    org.apache.commons.lang3.Validate.notNull(pkg, "invalid null package");
    this.pkg = pkg;
    this.matching = matching;
    this.keyspaces = findKeyspaces();
    this.where = new WhereImpl(this);
  }

  /**
   * Find all keyspaces.
   *
   * @author paouelle
   *
   * @return a map of all the keyspaces along with the list of POJO class info
   *         associated with them
   * @throws IllegalArgumentException if an @Entity annotated class is missing the
   *         Keyspace annotation or two entities defines the same keyspace with
   *         different options or an @Entitiy annotated class doesn't represent
   *         a valid POJO class or if no entities are found
   */
  private Map<Keyspace, List<ClassInfoImpl<?>>> findKeyspaces() {
    final Map<String, Keyspace> keyspaces = new HashMap<>(25);
    final Map<Keyspace, List<ClassInfoImpl<?>>> cinfos = new HashMap<>(25);
    final Reflections reflections = new Reflections(pkg);

    // search for all POJO annotated classes with @Entity
    for (final Class<?> clazz: reflections.getTypesAnnotatedWith(
      com.github.helenusdriver.persistence.Entity.class, true
    )) {
      // skip abstract POJO classes
      if (Modifier.isAbstract(clazz.getModifiers())) {
        continue;
      }
      final ClassInfoImpl<?> cinfo = mgr.getClassInfoImpl(clazz);
      final Keyspace k = cinfo.getKeyspace();
      final Keyspace old = keyspaces.put(k.name(), k);
      List<ClassInfoImpl<?>> cs = cinfos.get(k);

      if (cs == null) {
        cs = new ArrayList<>(25);
        cinfos.put(k, cs);
      }
      cs.add(cinfo);
      if ((old != null) && !k.equals(old)) {
        // duplicate annotation found with different attribute
        throw new IllegalArgumentException(
          "two different @Keyspace annotations found with class '"
          + clazz.getName()
          + "': "
          + old
          + " and: "
          + k
        );
      }
    }
    // search for all POJO annotated classes with @RootEntity
    for (final Class<?> clazz: reflections.getTypesAnnotatedWith(
      com.github.helenusdriver.persistence.RootEntity.class, true
    )) {
      // skip classes that are not directly annotated
      if (ReflectionUtils.findFirstClassAnnotatedWith(
           clazz, com.github.helenusdriver.persistence.RootEntity.class
         ) != clazz) {
        continue;
      }
      final ClassInfoImpl<?> cinfo = mgr.getClassInfoImpl(clazz);
      final Keyspace k = cinfo.getKeyspace();
      final Keyspace old = keyspaces.put(k.name(), k);
      List<ClassInfoImpl<?>> cs = cinfos.get(k);

      if (cs == null) {
        cs = new ArrayList<>(25);
        cinfos.put(k, cs);
      }
      cs.add(cinfo);
      if ((old != null) && !k.equals(old)) {
        // duplicate annotation found with different attribute
        throw new IllegalArgumentException(
          "two different @Keyspace annotations found with class '"
          + clazz.getName()
          + "': "
          + old
          + " and: "
          + k
        );
      }
    }
    org.apache.commons.lang3.Validate.isTrue(
      !cinfos.isEmpty(),
      "no classes annotated with @Entity or @RootEntity found in package: %s",
      pkg
    );
    return cinfos;
  }

  /**
   * Gets the contexts for all POJO classes for which we should create schemas.
   *
   * @author paouelle
   *
   * @return the list of contexts for all POJO for which we are creating schemas
   * @throws IllegalArgumentException if the value for a provided suffix doesn't
   *         match the POJO's definition for that suffix
   */
  @SuppressWarnings({"synthetic-access", "unchecked", "rawtypes"})
  private List<ClassInfoImpl<?>.Context> getContexts() {
    if (contexts == null) {
      this.contexts = new ArrayList<>(25);
      next_keyspace:
      for (final List<ClassInfoImpl<?>> cinfos: keyspaces.values()) {
        // create contexts for all the classes associated with the keyspace
        for (final ClassInfoImpl<?> cinfo: cinfos) {
          final ClassInfoImpl<?>.Context context = cinfo.newContext();
          IllegalArgumentException iae = null;
          int found = 0;

          // populate the required suffixes
          for (final Map.Entry<String, FieldInfoImpl<?>> e: (Set<Map.Entry<String, FieldInfoImpl<?>>>)(Set)cinfo.getSuffixTypes().entrySet()) {
            final String type = e.getKey();
            final FieldInfoImpl<?> finfo = e.getValue();

            if (!where.suffixes.containsKey(type)) {
              // we are missing a required suffix for this POJO and since all pojos
              // references the same @Keyspace, all of them will have the same problem
              // -- so continue with next keyspace and ignore any errors that might
              //    occurred for suffixes before that
              continue next_keyspace;
            }
            found++;
            // don't forget to convert the type into the suffix key name for the
            // associated POJO
            try {
              context.addSuffix(finfo.getSuffixKey().name(), where.suffixes.get(type));
            } catch (IllegalArgumentException ee) {
              if (iae == null) { // keep only first one
                iae = ee;
              }
            }
          }
          // if we get here then we must have found all required suffixes in the
          // where clause so whether or not we consider it depends on whether or
          // not we required the exact same number of suffixes as defined if we
          // are matching or if we are not matching (and there would potentially
          // be more suffixes defined than we needed)
          if ((found == where.suffixes.size()) || !matching) {
            // if we got an error on one of the suffix, throw it back
            if (iae != null) {
              throw iae;
            }
            contexts.add(context);
          }
        }
      }
    }
    return contexts;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.impl.StatementImpl#setDirty()
   */
  @Override
  protected void setDirty() {
    super.setDirty();
    this.contexts = null;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.impl.StatementImpl#buildQueryStrings()
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  protected StringBuilder[] buildQueryStrings() {
    // we do not want to create the same keyspace so many times for nothing
    final List<ClassInfoImpl<?>.Context> contexts = getContexts();
    final List<StringBuilder> builders = new ArrayList<>(contexts.size() * 2);
    final Set<Keyspace> keyspaces = new HashSet<>(contexts.size());

    for (final ClassInfoImpl<?>.Context context: contexts) {
      final CreateSchemaImpl cs = new CreateSchemaImpl(
        context, mgr, bridge
      );

      if (ifNotExists) {
        cs.ifNotExists();
      }
      final StringBuilder[] cbuilders = cs.buildQueryStrings(keyspaces);

      if (cbuilders != null) {
        for (final StringBuilder builder: cbuilders) {
          if (builder != null) {
            builders.add(builder);
          }
        }
      }
    }
    if (builders.isEmpty()) {
      return null;
    }
    return builders.toArray(new StringBuilder[builders.size()]);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.impl.StatementImpl#appendGroupSubType(java.lang.StringBuilder)
   */
  @Override
  protected void appendGroupSubType(StringBuilder builder) {
    builder.append(" CREATE");
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.impl.SequenceStatementImpl#appendGroupType(java.lang.StringBuilder)
   */
  @Override
  protected void appendGroupType(StringBuilder builder) {
    builder.append("SCHEMAS");
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.CreateSchemas#getObjectClasses()
   */
  @Override
  public Set<Class<?>> getObjectClasses() {
    return getContexts().stream()
      .flatMap(c -> c.getClassInfo().objectClasses())
      .collect(Collectors.toSet());
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.CreateSchemas#getClassInfos()
   */
  @Override
  public Set<ClassInfo<?>> getClassInfos() {
    return getContexts().stream()
      .flatMap(c -> c.getClassInfo().classInfos())
      .collect(Collectors.toSet());
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.CreateSchemas#getDefinedClassInfos()
   */
  @Override
  public Set<ClassInfo<?>> getDefinedClassInfos() {
    return keyspaces.values().stream()
      .flatMap(cl -> cl.stream())
      .flatMap(cl -> cl.classInfos())
      .collect(Collectors.toSet());
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.CreateSchemas#ifNotExists()
   */
  @Override
  public CreateSchemas ifNotExists() {
    this.ifNotExists = true;
    setDirty();
    return this;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.CreateSchemas#where(com.github.helenusdriver.driver.Clause)
   */
  @Override
  public Where where(Clause clause) {
    return where.and(clause);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.CreateSchemas#where()
   */
  @Override
  public Where where() {
    return where;
  }

  /**
   * The <code>WhereImpl</code> class defines a WHERE clause for the CREATE
   * SCHEMAS statement which can be used to specify suffix types used for
   * keyspace names.
   *
   * @copyright 2015-2015 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 19, 2015 - paouelle - Creation
   *
   * @since 1.0
   */
  public static class WhereImpl
    extends ForwardingStatementImpl<Void, VoidFuture, Void, CreateSchemasImpl>
    implements Where {
    /**
     * Holds the suffixes with their values.
     *
     * @author paouelle
     */
    private final Map<String, Object> suffixes = new HashMap<>(8);

    /**
     * Instantiates a new <code>WhereImpl</code> object.
     *
     * @author paouelle
     *
     * @param statement the encapsulated statement
     */
    WhereImpl(CreateSchemasImpl statement) {
      super(statement);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.github.helenusdriver.driver.CreateTable.Where#and(com.github.helenusdriver.driver.Clause)
     */
    @Override
    public Where and(Clause clause) {
      org.apache.commons.lang3.Validate.notNull(clause, "invalid null clause");
      org.apache.commons.lang3.Validate.isTrue(
        clause instanceof ClauseImpl,
        "unsupported class of clauses: %s",
        clause.getClass().getName()
      );
      org.apache.commons.lang3.Validate.isTrue(
        !(clause instanceof ClauseImpl.DelayedWithObject),
        "unsupported clause '%s' for a CREATE SCHEMA statement",
        clause
      );
      if (clause instanceof ClauseImpl.Delayed) {
        for (final Clause c: ((ClauseImpl.Delayed)clause).processWith(statement.getContext().getClassInfo())) {
          and(c); // recurse to add the processed clause
        }
      } else {
        final ClauseImpl c = (ClauseImpl)clause;

        org.apache.commons.lang3.Validate.isTrue(
          clause instanceof Clause.Equality,
          "unsupported class of clauses: %s",
          clause.getClass().getName()
        );
        suffixes.put(c.getColumnName().toString(), c.firstValue());
        setDirty();
      }
      return this;
    }
  }
}

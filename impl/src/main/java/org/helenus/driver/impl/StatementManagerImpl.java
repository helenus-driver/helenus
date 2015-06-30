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

import java.lang.reflect.Field;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.json.JsonObject;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.datastax.driver.core.CloseFuture;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

import org.helenus.commons.collections.iterators.SnapshotIterator;
import org.helenus.commons.lang3.reflect.ReflectionUtils;
import org.helenus.driver.AlterSchema;
import org.helenus.driver.AlterSchemas;
import org.helenus.driver.Assignment;
import org.helenus.driver.Batch;
import org.helenus.driver.BatchableStatement;
import org.helenus.driver.Clause;
import org.helenus.driver.CreateIndex;
import org.helenus.driver.CreateKeyspace;
import org.helenus.driver.CreateSchema;
import org.helenus.driver.CreateSchemas;
import org.helenus.driver.CreateTable;
import org.helenus.driver.CreateType;
import org.helenus.driver.Delete;
import org.helenus.driver.Delete.Builder;
import org.helenus.driver.Insert;
import org.helenus.driver.KeyspaceWith;
import org.helenus.driver.Ordering;
import org.helenus.driver.Recorder;
import org.helenus.driver.RegularStatement;
import org.helenus.driver.Select;
import org.helenus.driver.Select.Selection;
import org.helenus.driver.Sequence;
import org.helenus.driver.SequenceableStatement;
import org.helenus.driver.StatementBridge;
import org.helenus.driver.StatementManager;
import org.helenus.driver.Truncate;
import org.helenus.driver.Update;
import org.helenus.driver.Using;
import org.helenus.driver.impl.Utils.CNameSequence;
import org.helenus.driver.info.ClassInfo;
import org.helenus.driver.info.EntityFilter;
import org.helenus.driver.info.TableInfo;
import org.helenus.driver.persistence.DataType;
import org.helenus.driver.persistence.Entity;
import org.helenus.driver.persistence.RootEntity;
import org.helenus.driver.persistence.TypeEntity;
import org.helenus.driver.persistence.UDTEntity;

/**
 * The <code>StatementManagerImpl</code> class provides an implementation
 * for the {@link StatementManager}.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 19, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
public class StatementManagerImpl extends StatementManager {
  /**
   * Holds the logger.
   *
   * @author paouelle
   */
  private final static Logger logger = LogManager.getFormatterLogger(StatementManagerImpl.class);

  /**
   * Holds the bridge.
   *
   * @author paouelle
   */
  private final StatementBridge bridge;

  /**
   * Holds Cassandra's cluster object.
   *
   * @author paouelle
   */
  private final Cluster cluster;

  /**
   * Holds Cassandra's session.
   *
   * @author paouelle
   */
  private Session session;

  /**
   * Holds the default replication factor to use when creating keyspaces with
   * a SIMPLE strategy and 0 or the default replication factor has been defined
   * in the POJO.
   *
   * @author paouelle
   */
  private int defaultReplicationFactor = 2;

  /**
   * Holds the default data centers to use when creating keyspaces with
   * a NETWORK topology strategy and no data centers have been defined
   * in the POJO.
   *
   * @author paouelle
   */
  private Map<String, Integer> defaultDataCenters = null;

  /**
   * Hold the classInfoCache. This static map is used to collect and store
   * information about a given Object class file It finds out the keyspace suffix
   * field, tables suffix which help to speed up querying on these tables
   *
   * @author vasu
   */
  private final Map<Class<?>, ClassInfoImpl<?>> classInfoCache = new HashMap<>();

  /**
   * Holds the registered filters to be used when introspecting entities.
   * <p>
   * No need for sets as we do not expect to have multiple filters so simple
   * sequential search should do the trick.
   *
   * @author paouelle
   */
  private final List<EntityFilter> filters = new ArrayList<>(2);

  /**
   * Instantiates a new <code>StatementManagerImpl</code> object.
   *
   * @author paouelle
   *
   * @param  initializer the cluster initializer to use to initialize Cassandra's
   *         cluster
   * @param  connect <code>true</code> to connect to Cassandra; <code>false</code>
   *         not to connect
   * @throws NullPointerException if <code>initializer</code> is <code>null</code>
   * @throws IllegalArgumentException if the list of contact points provided
   *         by <code>initializer</code> is empty or if not all those contact
   *         points have the same port.
   * @throws NoHostAvailableException if no Cassandra host amongst the contact
   *         points can be reached
   * @throws SecurityException if the statement manager reference has already
   *         been set
   */
  public StatementManagerImpl(Cluster.Initializer initializer, boolean connect) {
    org.helenus.commons.lang3.Validate.notNull(logger, initializer, "invalid null initializer");
    this.cluster = Cluster.buildFrom(initializer);
    this.session = connect ? cluster.connect() : null;
    this.bridge = setManager(this);
  }

  /**
   * Instantiates a new <code>StatementManagerImpl</code> object.
   *
   * @author paouelle
   *
   * @param  initializer the cluster initializer to use to initialize Cassandra's
   *         cluster
   * @param  defaultReplicationFactor the default replication factor to use when
   *         POJOS are defined with the SIMPLE strategy and do not specify a factor
   * @param  connect <code>true</code> to connect to Cassandra; <code>false</code>
   *         not to connect
   * @throws NullPointerException if <code>initializer</code> is <code>null</code>
   * @throws IllegalArgumentException if the list of contact points provided
   *         by <code>initializer</code> is empty or if not all those contact
   *         points have the same port or again the default replication factor
   *         is less or equal to 0
   * @throws NoHostAvailableException if no Cassandra host amongst the contact
   *         points can be reached
   * @throws SecurityException if the statement manager reference has already
   *         been set
   */
  public StatementManagerImpl(
    Cluster.Initializer initializer, int defaultReplicationFactor, boolean connect
  ) {
    this(initializer, connect);
    setDefaultReplicationFactor(defaultReplicationFactor);
  }

  /**
   * Instantiates a new <code>StatementManagerImpl</code> object.
   *
   * @author paouelle
   *
   * @param  initializer the cluster initializer to use to initialize Cassandra's
   *         cluster
   * @param  defaultReplicationFactor the default replication factor to use when
   *         POJOS are defined with the SIMPLE strategy and do not specify a factor
   * @param  connect <code>true</code> to connect to Cassandra; <code>false</code>
   *         not to connect
   * @param  filters optional entity filters to register
   * @throws NullPointerException if <code>initializer</code> is <code>null</code>
   * @throws IllegalArgumentException if the list of contact points provided
   *         by <code>initializer</code> is empty or if not all those contact
   *         points have the same port or again the default replication factor
   *         is less or equal to 0
   * @throws NoHostAvailableException if no Cassandra host amongst the contact
   *         points can be reached
   * @throws SecurityException if the statement manager reference has already
   *         been set
   */
  public StatementManagerImpl(
    Cluster.Initializer initializer,
    int defaultReplicationFactor,
    boolean connect,
    EntityFilter... filters
  ) {
    this(initializer, connect, filters);
    setDefaultReplicationFactor(defaultReplicationFactor);
  }

  /**
   * Instantiates a new <code>StatementManagerImpl</code> object.
   *
   * @author paouelle
   *
   * @param  initializer the cluster initializer to use to initialize Cassandra's
   *         cluster
   * @param  connect <code>true</code> to connect to Cassandra; <code>false</code>
   *         not to connect
   * @param  filters optional entity filters to register
   * @throws NullPointerException if <code>initializer</code> is <code>null</code>
   * @throws IllegalArgumentException if the list of contact points provided
   *         by <code>initializer</code> is empty or if not all those contact
   *         points have the same port.
   * @throws NoHostAvailableException if no Cassandra host amongst the contact
   *         points can be reached
   * @throws SecurityException if the statement manager reference has already
   *         been set
   */
  public StatementManagerImpl(
    Cluster.Initializer initializer, boolean connect, EntityFilter... filters
  ) {
    this(initializer, connect);
    if (filters != null) {
      for (final EntityFilter filter: filters) {
        this.filters.add(filter);
      }
    }
  }

  /**
   * Instantiates a new <code>StatementManagerImpl</code> object.
   * <p>
   * <i>Note:</i> In this version of the constructor, specified filter classes
   * are instantiated using their default constructor.
   *
   * @author paouelle
   *
   * @param  initializer the cluster initializer to use to initialize Cassandra's
   *         cluster
   * @param  connect <code>true</code> to connect to Cassandra; <code>false</code>
   *         not to connect
   * @param  cnames optional entity filter class names to register
   * @throws NullPointerException if <code>initializer</code> is <code>null</code>
   * @throws IllegalArgumentException if the list of contact points provided
   *         by <code>initializer</code> is empty or if not all those contact
   *         points have the same port.
   * @throws NoHostAvailableException if no Cassandra host amongst the contact
   *         points can be reached
   * @throws SecurityException if the statement manager reference has already
   *         been set
   * @throws LinkageError if the linkage fails while loading a filter class
   * @throws ExceptionInInitializerError if the initialization provoked
   *         when loading a filter class fails
   * @throws ClassNotFoundException if a filter class cannot be located
   * @throws IllegalAccessException if a filter class or its default constructor
   *         is not accessible.
   * @throws InstantiationException if a filter class represents an abstract;
   *         or if a filter class has no default constructor; or if the
   *         instantiation fails for some other reason
   */
  public StatementManagerImpl(
    Cluster.Initializer initializer, boolean connect, String... cnames
  ) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    this(initializer, connect);
    if (cnames != null) {
      for (final String cname: cnames) {
        final Class<?> clazz = DataDecoder.findClass(cname);

        org.apache.commons.lang3.Validate.isTrue(
          EntityFilter.class.isAssignableFrom(clazz),
          "invalid entity filter class: %s", cname
        );
        this.filters.add(EntityFilter.class.cast(clazz.newInstance()));
      }
    }
  }

  /**
   * Clears the cache of pojo class info.
   *
   * @author paouelle
   */
  protected void clearCache() {
    classInfoCache.clear();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#getClassInfo(java.lang.Class)
   */
  @Override
  protected <T> ClassInfo<T> getClassInfo(Class<T> clazz) {
    return getClassInfoImpl(clazz);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#select(java.lang.Class, java.lang.CharSequence[])
   */
  @Override
  protected <T> Select.Builder<T> select(Class<T> clazz, CharSequence... columns) {
    final ClassInfoImpl<T> cinfo = getClassInfoImpl(clazz);

    org.apache.commons.lang3.Validate.isTrue(
      cinfo.supportsTablesAndIndexes(),
      "unsupported %s POJO class '%s' for a select statement",
      cinfo.getEntityAnnotationClass().getSimpleName(), clazz.getSimpleName()
    );
    return new SelectImpl.BuilderImpl<>(
      cinfo.newContext(),
      Arrays.asList((Object[])columns),
      this,
      bridge
    );
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#select(java.lang.Class)
   */
  @Override
  protected <T> Selection<T> select(Class<T> clazz) {
    final ClassInfoImpl<T> cinfo = getClassInfoImpl(clazz);

    org.apache.commons.lang3.Validate.isTrue(
      cinfo.supportsTablesAndIndexes(),
      "unsupported %s POJO class '%s' for a select statement",
      cinfo.getEntityAnnotationClass().getSimpleName(), clazz.getSimpleName()
    );
    return new SelectImpl.SelectionImpl<>(
      cinfo.newContext(),
      this,
      bridge
    );
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#insert(java.lang.Object)
   */
  @SuppressWarnings("unchecked")
  @Override
  protected <T> Insert.Builder<T> insert(T object) {
    org.apache.commons.lang3.Validate.notNull(object, "invalid null object");
    final ClassInfoImpl<T> cinfo = getClassInfoImpl((Class<T>)object.getClass());

    org.apache.commons.lang3.Validate.isTrue(
      cinfo.supportsTablesAndIndexes(),
      "unsupported %s POJO class '%s' for an insert statement",
      cinfo.getEntityAnnotationClass().getSimpleName(), object.getClass().getSimpleName()
    );
    return new InsertImpl.BuilderImpl<>(
      cinfo.newContext(object),
      this,
      bridge
    );
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#update(java.lang.Object)
   */
  @SuppressWarnings("unchecked")
  @Override
  protected <T> Update<T> update(T object) {
    org.apache.commons.lang3.Validate.notNull(object, "invalid null object");
    final ClassInfoImpl<T> cinfo = getClassInfoImpl((Class<T>)object.getClass());

    org.apache.commons.lang3.Validate.isTrue(
      cinfo.supportsTablesAndIndexes(),
      "unsupported %s POJO class '%s' for an update statement",
      cinfo.getEntityAnnotationClass().getSimpleName(), object.getClass().getSimpleName()
    );
    return new UpdateImpl<>(
      cinfo.newContext(object),
      this,
      bridge
    );
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#update(java.lang.Object, java.lang.String[])
   */
  @SuppressWarnings("unchecked")
  @Override
  protected <T> Update<T> update(T object, String... tables) {
    org.apache.commons.lang3.Validate.notNull(object, "invalid null object");
    final ClassInfoImpl<T> cinfo = getClassInfoImpl((Class<T>)object.getClass());

    org.apache.commons.lang3.Validate.isTrue(
      cinfo.supportsTablesAndIndexes(),
      "unsupported %s POJO class '%s' for an update statement",
      cinfo.getEntityAnnotationClass().getSimpleName(), object.getClass().getSimpleName()
    );
    return new UpdateImpl<>(
      cinfo.newContext(object),
      tables,
      this,
      bridge
    );
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#delete(java.lang.Object, java.lang.String[])
   */
  @SuppressWarnings("unchecked")
  @Override
  protected <T> Builder<T> delete(T object, String... columns) {
    org.apache.commons.lang3.Validate.notNull(object, "invalid null object");
    final ClassInfoImpl<T> cinfo = getClassInfoImpl((Class<T>)object.getClass());

    org.apache.commons.lang3.Validate.isTrue(
      cinfo.supportsTablesAndIndexes(),
      "unsupported %s POJO class '%s' for a delete statement",
      cinfo.getEntityAnnotationClass().getSimpleName(), object.getClass().getSimpleName()
    );
    return new DeleteImpl.BuilderImpl<>(
      cinfo.newContext(object),
      Arrays.asList((Object[])columns),
      this,
      bridge
    );
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#delete(java.lang.Object)
   */
  @SuppressWarnings("unchecked")
  @Override
  protected <T> org.helenus.driver.Delete.Selection<T> delete(T object) {
    org.apache.commons.lang3.Validate.notNull(object, "invalid null object");
    final ClassInfoImpl<T> cinfo = getClassInfoImpl((Class<T>)object.getClass());

    org.apache.commons.lang3.Validate.isTrue(
      cinfo.supportsTablesAndIndexes(),
      "unsupported %s POJO class '%s' for a delete statement",
      cinfo.getEntityAnnotationClass().getSimpleName(), object.getClass().getSimpleName()
    );
    return new DeleteImpl.SelectionImpl<>(
      cinfo.newContext(object),
      this,
      bridge
    );
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#delete(java.lang.Class, java.lang.String[])
   */
  @Override
  protected <T> Delete.Builder<T> delete(Class<T> clazz, String... columns) {
    org.apache.commons.lang3.Validate.notNull(clazz, "invalid null class");
    final ClassInfoImpl<T> cinfo = getClassInfoImpl(clazz);

    org.apache.commons.lang3.Validate.isTrue(
      cinfo.supportsTablesAndIndexes(),
      "unsupported %s POJO class '%s' for a delete statement",
      cinfo.getEntityAnnotationClass().getSimpleName(), clazz.getSimpleName()
    );
    return new DeleteImpl.BuilderImpl<>(
      cinfo.newContext(),
      Arrays.asList((Object[])columns),
      this,
      bridge
    );
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#delete(java.lang.Class)
   */
  @Override
  protected <T> Delete.Selection<T> delete(Class<T> clazz) {
    org.apache.commons.lang3.Validate.notNull(clazz, "invalid null class");
    final ClassInfoImpl<T> cinfo = getClassInfoImpl(clazz);

    org.apache.commons.lang3.Validate.isTrue(
      cinfo.supportsTablesAndIndexes(),
      "unsupported %s POJO class '%s' for a delete statement",
      cinfo.getEntityAnnotationClass().getSimpleName(), clazz.getSimpleName()
    );
    return new DeleteImpl.SelectionImpl<>(
      cinfo.newContext(),
      this,
      bridge
    );
  };

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#batch(java.util.Optional, org.helenus.driver.BatchableStatement[])
   */
  @Override
  protected Batch batch(Optional<Recorder> recorder, BatchableStatement<?, ?>... statements) {
    return new BatchImpl(recorder, statements, true, this, bridge);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#batch(java.util.Optional, java.lang.Iterable)
   */
  @Override
  protected Batch batch(Optional<Recorder> recorder, Iterable<BatchableStatement<?, ?>> statements) {
    return new BatchImpl(recorder, statements, true, this, bridge);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#unloggedBatch(java.util.Optional, org.helenus.driver.BatchableStatement[])
   */
  @Override
  protected Batch unloggedBatch(Optional<Recorder> recorder, BatchableStatement<?, ?>... statements) {
    return new BatchImpl(recorder, statements, false, this, bridge);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#unloggedBatch(java.util.Optional, java.lang.Iterable)
   */
  @Override
  protected Batch unloggedBatch(Optional<Recorder> recorder, Iterable<BatchableStatement<?, ?>> statements) {
    return new BatchImpl(recorder, statements, false, this, bridge);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#regular(com.datastax.driver.core.RegularStatement)
   */
  @Override
  protected RegularStatement regular(
    com.datastax.driver.core.RegularStatement statement
  ) {
    return new SimpleStatementImpl(
      statement,
      this,
      bridge
    );
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#createKeyspace(java.lang.Class)
   */
  @Override
  protected <T> CreateKeyspace<T> createKeyspace(Class<T> clazz) {
    return new CreateKeyspaceImpl<>(
      getClassInfoImpl(clazz).newContext(),
      this,
      bridge
    );
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#createType(java.lang.Class)
   */
  @Override
  protected <T> CreateType<T> createType(Class<T> clazz) {
    final ClassInfoImpl<T> cinfo = getClassInfoImpl(clazz);

    org.apache.commons.lang3.Validate.isTrue(
      !cinfo.supportsTablesAndIndexes(),
      "unsupported %s POJO class '%s' for a create type statement",
      cinfo.getEntityAnnotationClass().getSimpleName(), clazz.getSimpleName()
    );
    return new CreateTypeImpl<>(
      cinfo.newContext(),
      this,
      bridge
    );
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#createTable(Class)
   */
  @Override
  protected <T> CreateTable<T> createTable(Class<T> clazz) {
    final ClassInfoImpl<T> cinfo = getClassInfoImpl(clazz);

    org.apache.commons.lang3.Validate.isTrue(
      cinfo.supportsTablesAndIndexes(),
      "unsupported %s POJO class '%s' for a create table statement",
      cinfo.getEntityAnnotationClass().getSimpleName(), clazz.getSimpleName()
    );
    return new CreateTableImpl<>(
      cinfo.newContext(),
      this,
      bridge
    );
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#createTable(Class, String[])
   */
  @Override
  protected <T> CreateTable<T> createTable(Class<T> clazz, String... tables) {
    final ClassInfoImpl<T> cinfo = getClassInfoImpl(clazz);

    org.apache.commons.lang3.Validate.isTrue(
      cinfo.supportsTablesAndIndexes(),
      "unsupported %s POJO class '%s' for a create table statement",
      cinfo.getEntityAnnotationClass().getSimpleName(), clazz.getSimpleName()
    );
    return new CreateTableImpl<>(
      cinfo.newContext(),
      tables,
      this,
      bridge
    );
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#createIndex(java.lang.Class)
   */
  @Override
  protected <T> CreateIndex.Builder<T> createIndex(Class<T> clazz) {
    final ClassInfoImpl<T> cinfo = getClassInfoImpl(clazz);

    org.apache.commons.lang3.Validate.isTrue(
      cinfo.supportsTablesAndIndexes(),
      "unsupported %s POJO class '%s' for a create index statement",
      cinfo.getEntityAnnotationClass().getSimpleName(), clazz.getSimpleName()
    );
    return new CreateIndexImpl.BuilderImpl<>(
      cinfo.newContext(),
      this,
      bridge
    );
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#createSchema(java.lang.Class)
   */
  @Override
  protected <T> CreateSchema<T> createSchema(Class<T> clazz) {
    return new CreateSchemaImpl<>(
      getClassInfoImpl(clazz).newContext(),
      this,
      bridge
    );
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#createSchemas(java.lang.String)
   */
  @Override
  protected CreateSchemas createSchemas(String pkg) {
    return new CreateSchemasImpl(
      pkg,
      false,
      this,
      bridge
    );
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#createMatchingSchemas(java.lang.String)
   */
  @Override
  protected CreateSchemas createMatchingSchemas(String pkg) {
    return new CreateSchemasImpl(
      pkg,
      true,
      this,
      bridge
    );
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#alterSchema(java.lang.Class)
   */
  @Override
  protected <T> AlterSchema<T> alterSchema(Class<T> clazz) {
    return new AlterSchemaImpl<>(
      getClassInfoImpl(clazz).newContext(),
      this,
      bridge
    );
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#alterSchemas(java.lang.String)
   */
  @Override
  protected AlterSchemas alterSchemas(String pkg) {
    return new AlterSchemasImpl(
      pkg,
      false,
      this,
      bridge
    );
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#alterMatchingSchemas(java.lang.String)
   */
  @Override
  protected AlterSchemas alterMatchingSchemas(String pkg) {
    return new AlterSchemasImpl(
      pkg,
      true,
      this,
      bridge
    );
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#truncate(java.lang.Class)
   */
  @Override
  protected <T> Truncate<T> truncate(Class<T> clazz) {
    final ClassInfoImpl<T> cinfo = getClassInfoImpl(clazz);

    org.apache.commons.lang3.Validate.isTrue(
      cinfo.supportsTablesAndIndexes(),
      "unsupported %s POJO class '%s' for a truncate statement",
      cinfo.getEntityAnnotationClass().getSimpleName(), clazz.getSimpleName()
    );
    return new TruncateImpl<>(
      cinfo.newContext(),
      this,
      bridge
    );
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#truncate(Class, String[])
   */
  @Override
  protected <T> Truncate<T> truncate(Class<T> clazz, String... tables) {
    final ClassInfoImpl<T> cinfo = getClassInfoImpl(clazz);

    org.apache.commons.lang3.Validate.isTrue(
      cinfo.supportsTablesAndIndexes(),
      "unsupported %s POJO class '%s' for a truncate statement",
      cinfo.getEntityAnnotationClass().getSimpleName(), clazz.getSimpleName()
    );
    return new TruncateImpl<>(
      cinfo.newContext(),
      tables,
      this,
      bridge
    );
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#sequence(java.util.Optional, org.helenus.driver.SequenceableStatement[])
   */
  @Override
  protected Sequence sequence(Optional<Recorder> recorder, SequenceableStatement<?, ?>... statements) {
    return new SequenceImpl(
      recorder,
      statements,
      this,
      bridge
    );
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#sequence(java.util.Optional, java.lang.Iterable)
   */
  @Override
  protected Sequence sequence(Optional<Recorder> recorder, Iterable<SequenceableStatement<?, ?>> statements) {
    return new SequenceImpl(
      recorder,
      statements,
      this,
      bridge
    );
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#quote(java.lang.String)
   */
  @Override
  protected CharSequence quote(String columnName) {
    org.apache.commons.lang3.Validate.notNull(columnName, "invalid null column name");
    final StringBuilder sb = new StringBuilder(columnName.length() + 2);

    sb.append("\"");
    Utils.appendName(columnName, sb);
    return new CNameSequence(sb.toString(), columnName);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#token(java.lang.String)
   */
  @Override
  protected CharSequence token(String columnName) {
    org.apache.commons.lang3.Validate.notNull(columnName, "invalid null column name");
    final StringBuilder sb = new StringBuilder(columnName.length() + 7);

    sb.append("token(");
    Utils.appendName(columnName, sb);
    sb.append(")");
    return new CNameSequence(sb.toString(), columnName);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#token(java.lang.String[])
   */
  @Override
  protected CharSequence token(String... columnNames) {
    final StringBuilder sb = new StringBuilder();

    sb.append("token(");
    Utils.joinAndAppendNames(sb, ",", Arrays.asList((Object[])columnNames));
    sb.append(")");
    return new CNameSequence(sb.toString(), columnNames);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#isSuffixedLikeObject()
   */
  @Override
  protected Clause isSuffixedLikeObject() {
    return new ClauseImpl.IsSuffixedLikeObjectClauseImpl();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#isSuffixedLike(java.lang.Object)
   */
  @Override
  protected <T> Clause isSuffixedLike(T object) {
    return new ClauseImpl.IsSuffixedLikeClauseImpl(object);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#isPartitionedLikeObject()
   */
  @Override
  protected Clause isPartitionedLikeObject() {
    return new ClauseImpl.IsPartitionedLikeObjectClauseImpl();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#isPartitionedLike(java.lang.Object)
   */
  @Override
  protected <T> Clause isPartitionedLike(T object) {
    return new ClauseImpl.IsPartitionedLikeClauseImpl(object);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#isObject()
   */
  @Override
  protected Clause isObject() {
    return new ClauseImpl.IsObjectClauseImpl();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#is(java.lang.Object)
   */
  @Override
  protected <T> Clause is(T object) {
    return new ClauseImpl.IsClauseImpl(object);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#eq(java.lang.CharSequence, java.lang.Object)
   */
  @Override
  protected Clause.Equality eq(CharSequence name, Object value) {
    return new ClauseImpl.EqClauseImpl(name, value);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#in(java.lang.CharSequence, java.lang.Object[])
   */
  @Override
  protected Clause.In in(CharSequence name, Object... values) {
    return new ClauseImpl.InClauseImpl(name, Arrays.asList(values));
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#in(java.lang.CharSequence, java.util.Collection)
   */
  @Override
  protected Clause.In in(CharSequence name, Collection<?> values) {
    return new ClauseImpl.InClauseImpl(name, values);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#in(java.lang.CharSequence, java.util.stream.Stream)
   */
  @SuppressWarnings({"rawtypes", "cast", "unchecked"})
  @Override
  protected Clause.In in(CharSequence name, Stream<?> values) {
    return new ClauseImpl.InClauseImpl(
      name, (Collection<?>)((Stream)values).collect(Collectors.toList())
    );
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#in(java.lang.CharSequence, int, int)
   */
  @Override
  protected Clause.In in(CharSequence name, int from, int to) {
    org.apache.commons.lang3.Validate.isTrue(
      to >= from,
      "'to' value '%d' must be greater or equal to 'from' value '%d'",
      to,
      from
    );
    final List<Object> values = new ArrayList<>(to - from + 1);

    for (int i = from; i <= to; i++) {
      values.add(i);
    }
    return new ClauseImpl.InClauseImpl(name, values);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#lt(java.lang.CharSequence, java.lang.Object)
   */
  @Override
  protected Clause lt(CharSequence name, Object value) {
    return new ClauseImpl.SimpleClauseImpl(name, "<", value);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#lte(java.lang.CharSequence, java.lang.Object)
   */
  @Override
  protected Clause lte(CharSequence name, Object value) {
    return new ClauseImpl.SimpleClauseImpl(name, "<=", value);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#gt(java.lang.CharSequence, java.lang.Object)
   */
  @Override
  protected Clause gt(CharSequence name, Object value) {
    return new ClauseImpl.SimpleClauseImpl(name, ">", value);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#gte(java.lang.CharSequence, java.lang.Object)
   */
  @Override
  protected Clause gte(CharSequence name, Object value) {
    return new ClauseImpl.SimpleClauseImpl(name, ">=", value);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#asc(java.lang.CharSequence)
   */
  @Override
  protected Ordering asc(CharSequence columnName) {
    return new OrderingImpl(columnName, false);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#desc(java.lang.CharSequence)
   */
  @Override
  protected Ordering desc(CharSequence columnName) {
    return new OrderingImpl(columnName, true);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#timestamp(long)
   */
  @Override
  protected Using timestamp(long timestamp) {
    org.apache.commons.lang3.Validate.isTrue(
      timestamp >= 0L,
      "invalid timestamp, must be positive: %s", timestamp
    );
    return new UsingImpl("TIMESTAMP", timestamp);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#ttl(int)
   */
  @Override
  protected Using ttl(int ttl) {
    org.apache.commons.lang3.Validate.isTrue(
      ttl >= 0,
      "invalid ttl, must be positive: %s", ttl
    );
    return new UsingImpl("TTL", ttl);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#appendName(java.lang.String, java.lang.StringBuilder)
   */
  @Override
  protected StringBuilder appendName(String name, StringBuilder sb) {
    return Utils.appendName(name, sb);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#setFromObject(java.lang.CharSequence)
   */
  @Override
  protected Assignment setFromObject(CharSequence name) {
    return new AssignmentImpl.DelayedSetAssignmentImpl(name);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#setFrom(java.lang.Object, java.lang.CharSequence)
   */
  @Override
  protected <T> Assignment setFrom(T object, CharSequence name) {
    org.apache.commons.lang3.Validate.notNull(object, "invalid null object");
    return new AssignmentImpl.DelayedSetAssignmentImpl(object, name);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#set(java.lang.CharSequence, java.lang.Object)
   */
  @Override
  protected Assignment set(CharSequence name, Object value) {
    return new AssignmentImpl.SetAssignmentImpl(name, value);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#set(java.lang.CharSequence, java.lang.Object, java.lang.Object)
   */
  @Override
  protected Assignment set(CharSequence name, Object value, Object old) {
    // paouelle: 04/10/2015 - handling it as a normal Set causes problem in cases
    //                        where the primary key is optional as going from empty
    //                        to empty now generates an exception up
//    if (!Objects.equals(value, old)) {
      return new AssignmentImpl.ReplaceAssignmentImpl(name, value, old);
//    } // else - no change so handle as normal set
//    return new AssignmentImpl.SetAssignmentImpl(name, value);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#setAllFromObject()
   */
  @Override
  protected Assignment setAllFromObject() {
    return new AssignmentImpl.DelayedSetAllAssignmentImpl();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#setAllFrom(java.lang.Object)
   */
  @Override
  protected <T> Assignment setAllFrom(T object) {
    org.apache.commons.lang3.Validate.notNull(object, "invalid null object");
    return new AssignmentImpl.DelayedSetAllAssignmentImpl(object);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#incr(java.lang.CharSequence, long)
   */
  @Override
  protected Assignment incr(CharSequence name, long value) {
    return new AssignmentImpl.CounterAssignmentImpl(name, value, true);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#decr(java.lang.CharSequence, long)
   */
  @Override
  protected Assignment decr(CharSequence name, long value) {
    return new AssignmentImpl.CounterAssignmentImpl(name, value, false);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#prepend(java.lang.CharSequence, java.lang.Object)
   */
  @Override
  protected Assignment prepend(CharSequence name, Object value) {
    return new AssignmentImpl.ListPrependAssignmentImpl(
      name,
      Collections.singletonList(value)
    );
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#prependAll(java.lang.CharSequence, java.util.List)
   */
  @Override
  protected Assignment prependAll(CharSequence name, List<?> values) {
    return new AssignmentImpl.ListPrependAssignmentImpl(name, values);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#append(java.lang.CharSequence, java.lang.Object)
   */
  @Override
  protected Assignment append(CharSequence name, Object value) {
    return new AssignmentImpl.CollectionAssignmentImpl(
      DataType.LIST,
      name,
      Collections.singletonList(value),
      true
    );
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#appendAll(java.lang.CharSequence, java.util.List)
   */
  @Override
  protected Assignment appendAll(CharSequence name, List<?> values) {
    return new AssignmentImpl.CollectionAssignmentImpl(DataType.LIST, name, values, true);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#discard(java.lang.CharSequence, java.lang.Object)
   */
  @Override
  protected Assignment discard(CharSequence name, Object value) {
    return new AssignmentImpl.CollectionAssignmentImpl(
      DataType.LIST,
      name,
      Collections.singletonList(value),
      false
    );
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#discardAll(java.lang.CharSequence, java.util.List)
   */
  @Override
  protected Assignment discardAll(CharSequence name, List<?> values) {
    return new AssignmentImpl.CollectionAssignmentImpl(
      DataType.LIST, name, values, false
    );
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#setIdx(java.lang.CharSequence, int, java.lang.Object)
   */
  @Override
  protected Assignment setIdx(CharSequence name, int idx, Object value) {
    if (idx < 0) {
      throw new IndexOutOfBoundsException("invalid negative index: " + idx);
    }
    return new AssignmentImpl.ListSetIdxAssignmentImpl(name, idx, value);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#add(java.lang.CharSequence, java.lang.Object)
   */
  @Override
  protected Assignment add(CharSequence name, Object value) {
    return new AssignmentImpl.CollectionAssignmentImpl(
      DataType.SET,
      name,
      Collections.singleton(value),
      true
    );
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#addAll(java.lang.CharSequence, java.util.Set)
   */
  @Override
  protected Assignment addAll(CharSequence name, Set<?> values) {
    return new AssignmentImpl.CollectionAssignmentImpl(DataType.SET, name, values, true);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#remove(java.lang.CharSequence, java.lang.Object)
   */
  @Override
  protected Assignment remove(CharSequence name, Object value) {
    return new AssignmentImpl.CollectionAssignmentImpl(
      DataType.SET,
      name,
      Collections.singleton(value),
      false
    );
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#removeAll(java.lang.CharSequence, java.util.Set)
   */
  @Override
  protected Assignment removeAll(CharSequence name, Set<?> values) {
    return new AssignmentImpl.CollectionAssignmentImpl(
      DataType.SET, name, values, false
     );
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#put(java.lang.CharSequence, java.lang.Object, java.lang.Object)
   */
  @Override
  protected Assignment put(CharSequence name, Object key, Object value) {
    return new AssignmentImpl.MapPutAssignmentImpl(name, key, value);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#putAll(java.lang.CharSequence, java.util.Map)
   */
  @Override
  protected Assignment putAll(CharSequence name, Map<?, ?> mappings) {
    return new AssignmentImpl.CollectionAssignmentImpl(DataType.MAP, name, mappings, true);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#replication(javax.json.JsonObject)
   */
  @Override
  protected KeyspaceWith replication(JsonObject map) {
    return new KeyspaceWithImpl("REPLICATION", map);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#durableWrites(boolean)
   */
  @Override
  protected KeyspaceWith durableWrites(boolean value) {
    return new KeyspaceWithImpl("DURABLE_WRITES", value);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#raw(java.lang.String)
   */
  @Override
  protected Object raw(String str) {
    return new Utils.RawString(str);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#fcall(java.lang.String, java.lang.Object[])
   */
  @Override
  protected Object fcall(String name, Object... parameters) {
    return new Utils.FCall(name, parameters);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#column(java.lang.String)
   */
  @Override
  protected Object column(String name) {
    return new Utils.CName(name);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#getColumnNamesFor(java.lang.Class, java.lang.reflect.Field)
   */
  @Override
  protected <T> Set<String> getColumnNamesFor(Class<T> clazz, Field field) {
    final ClassInfoImpl<?> cinfo = getClassInfoImpl(clazz);
    final Set<String> names = new LinkedHashSet<>(cinfo.getNumTables()); // preserve order

    for (final TableInfoImpl<?> tinfo: cinfo.getTablesImpl()) {
      final FieldInfoImpl<?> column = tinfo.getColumnByField(field);

      if (column != null) {
        names.add(column.getColumnName());
      }
    }
    org.apache.commons.lang3.Validate.isTrue(
      !names.isEmpty(),
      "field '%s.%s' is not annotated as a column",
      field.getDeclaringClass().getName(),
      field.getName()
    );
    return names;
  }

  /**
   * Called after a statement's execution has been requested.
   *
   * @author paouelle
   *
   * @param statement the non-<code>null</code> statement executing
   */
  protected void executing(StatementImpl<?, ?, ?> statement) {}

  /**
   * Connects to Cassandra if not already connected.
   *
   * @author paouelle
   *
   * @throws NoHostAvailableException if no Cassandra host amongst the contact
   *         points can be reached
   */
  public synchronized void connect() {
    if (session == null) {
      this.session = cluster.connect();
    }
  }

  /**
   * Filters the specified table for a POJO class.
   *
   * @author paouelle
   *
   * @param tinfo the non-<code>null</code> info for the table of a POJO class
   *        to be filtered
   */
  public void filter(TableInfo<?> tinfo) {
    for (final Iterator<EntityFilter> i = new SnapshotIterator<>(filters.iterator()); i.hasNext(); ) {
      try {
        i.next().filter(tinfo);
      } catch (OutOfMemoryError|StackOverflowError|AssertionError|ThreadDeath e) {
        throw e;
      } catch (Throwable t) { // ignore exception
        logger.log(Level.WARN, "entity filter error", t);
      }
    }
  }

  /**
   * Gets a class info structure that defines the specified POJO class.
   *
   * @author paouelle
   *
   * @param <T> The type of POJO associated with this statement
   *
   * @param  clazz the class of POJO for which to get a class info object for
   * @return the non-<code>null</code> class info object representing the given
   *         POJO class
   * @throws NullPointerException if <code>clazz</code> is <code>null</code>
   * @throws IllegalArgumentException if <code>clazz</code> doesn't represent
   *         a valid POJO class
   */
  @SuppressWarnings("unchecked")
  public <T> ClassInfoImpl<T> getClassInfoImpl(Class<T> clazz) {
    synchronized (classInfoCache) {
      ClassInfoImpl<T> classInfo = (ClassInfoImpl<T>)classInfoCache.get(clazz);

      if (classInfo == null) {
        final Class<? super T> rclazz
          = ReflectionUtils.findFirstClassAnnotatedWith(clazz, RootEntity.class);

        if (rclazz != null) {
          org.apache.commons.lang3.Validate.isTrue(
            !clazz.isAnnotationPresent(Entity.class),
            "class '%s' cannot be annotated with both @RootEntity and @Entity",
            clazz.getSimpleName()
          );
          org.apache.commons.lang3.Validate.isTrue(
            !clazz.isAnnotationPresent(UDTEntity.class),
            "class '%s' cannot be annotated with both @RootEntity and @UDTEntity",
            clazz.getSimpleName()
          );
          if (rclazz == clazz) { // this is the root element class
            org.apache.commons.lang3.Validate.isTrue(
              !clazz.isAnnotationPresent(TypeEntity.class),
              "class '%s' cannot be annotated with both @RootEntity and @TypeEntity",
              clazz.getSimpleName()
            );
            classInfo = new RootClassInfoImpl<>(this, clazz);
          } else {
            if (clazz.isAnnotationPresent(TypeEntity.class)) {
              // for types, we get it from the root
              final RootClassInfoImpl<? super T> rcinfo
                = (RootClassInfoImpl<? super T>)getClassInfoImpl(rclazz);

              classInfo = rcinfo.getType(clazz);
            } else {
              throw new IllegalArgumentException(
                "class '" + clazz.getSimpleName() + "' is not annotated with @TypeEntity"
              );
            }
          }
        } else if (clazz.isAnnotationPresent(UDTEntity.class)) {
          org.apache.commons.lang3.Validate.isTrue(
            !clazz.isAnnotationPresent(Entity.class),
            "class '%s' cannot be annotated with both @UDTEntity and @Entity",
            clazz.getSimpleName()
          );
          classInfo = new UDTClassInfoImpl<>(this, clazz);
        } else if (clazz.isAnnotationPresent(Entity.class)) {
          classInfo = new ClassInfoImpl<>(this, clazz);
        } else {
          throw new IllegalArgumentException(
            "class '" + clazz.getSimpleName() + "' is not annotated with @Entity"
          );
        }
        classInfoCache.put(clazz, classInfo);
      }
      return classInfo;
    }
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#getSession()
   */
  @Override
  public synchronized Session getSession() {
    return session;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.StatementManager#getCluster()
   */
  @Override
  public Cluster getCluster() {
    return cluster;
  }

  /**
   * Gets the default replication factor to use when POJOS are defined with the
   * SIMPLE strategy and do not specify a factor.
   *
   * @author paouelle
   *
   * @return the default replication factor to use for the SIMPLE replication
   *         strategy
   */
  public int getDefaultReplicationFactor() {
    return defaultReplicationFactor;
  }

  /**
   * Sets the default replication factor to use when POJOS are defined with the
   * SIMPLE strategy and the pojo does not specify a factor.
   *
   * @author paouelle
   *
   * @param  defaultReplicationFactor the default replication factor to use for
   *         the SIMPLE replication strategy
   * @return this for chaining
   * @throws IllegalArgumentException if the default replication factor
   *         is less or equal to 0
   */
  public StatementManagerImpl setDefaultReplicationFactor(int defaultReplicationFactor) {
    org.apache.commons.lang3.Validate.isTrue(
      (defaultReplicationFactor > 0),
      "invalid default replication factor: %d", defaultReplicationFactor
    );
    this.defaultReplicationFactor = defaultReplicationFactor;
    return this;
  }

  /**
   * Gets the default data centers to use when POJOS are defined with the
   * NETWORK topology strategy and the pojo does not specify data centers.
   *
   * @author paouelle
   *
   * @return the default data centers to use for the NETWORK topology
   *         strategy
   */
  public Map<String, Integer> getDefaultDataCenters() {
    return defaultDataCenters;
  }

  /**
   * Sets the default data centers to use when POJOS are defined with the
   * NETWORK topology strategy and do not specify any.
   *
   * @author paouelle
   *
   * @param  defaultDataCenters the default data centers to use for
   *         the NETWORK topology strategy
   * @return this for chaining
   * @throws IllegalArgumentException if any of the replication factor
   *         is less or equal to 0
   */
  public StatementManagerImpl setDefaultDataCenters(
    Map<String, Integer> defaultDataCenters
  ) {
    if (defaultDataCenters != null) {
      defaultDataCenters.entrySet()
        .forEach(e ->
          org.apache.commons.lang3.Validate.isTrue(
            (e.getValue() != null) && (e.getValue() > 0),
            "invalid replication factor: %d for: %s", e.getValue(), e.getKey()
          )
        );
    }
    this.defaultDataCenters = defaultDataCenters;
    return this;
  }

  /**
   * Initiates a shutdown of this cluster instance.
   * <p>
   * This method is asynchronous and returns a future on the completion
   * of the shutdown process. As soon a the statement manager is shutdown, no
   * new request will be accepted, but already submitted queries are
   * allowed to complete. Shutdown closes all connections from all
   * sessions and reclaims all resources used by the statement manager.
   * <p>
   * If for some reason you wish to expedite this process, the
   * {@link CloseFuture#force} can be called on the result future.
   * <p>
   * This method has no particular effect if the statement manager was already
   * shut down (in which case the returned future will return immediately).
   *
   * @author paouelle
   *
   * @return a non-<code>null</code> future on the completion of the shutdown
   *         process
   */
  public CloseFuture closeAsync() {
    return cluster.closeAsync();
  }

  /**
   * Initiates a shutdown of this cluster instance and blocks until
   * that shutdown completes.
   * <p>
   * This method has no particular effect if the statement manager was already
   * shut down.
   *
   * @author paouelle
   */
  public void close() {
    cluster.close();
  }
}

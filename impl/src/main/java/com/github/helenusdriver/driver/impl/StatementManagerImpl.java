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
import java.util.Objects;
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

import com.github.helenusdriver.commons.collections.iterators.SnapshotIterator;
import com.github.helenusdriver.commons.lang3.reflect.ReflectionUtils;
import com.github.helenusdriver.driver.Assignment;
import com.github.helenusdriver.driver.Batch;
import com.github.helenusdriver.driver.BatchableStatement;
import com.github.helenusdriver.driver.Clause;
import com.github.helenusdriver.driver.CreateIndex;
import com.github.helenusdriver.driver.CreateKeyspace;
import com.github.helenusdriver.driver.CreateSchema;
import com.github.helenusdriver.driver.CreateSchemas;
import com.github.helenusdriver.driver.CreateTable;
import com.github.helenusdriver.driver.Delete;
import com.github.helenusdriver.driver.Insert;
import com.github.helenusdriver.driver.KeyspaceWith;
import com.github.helenusdriver.driver.Ordering;
import com.github.helenusdriver.driver.RegularStatement;
import com.github.helenusdriver.driver.Select;
import com.github.helenusdriver.driver.Sequence;
import com.github.helenusdriver.driver.SequenceableStatement;
import com.github.helenusdriver.driver.StatementBridge;
import com.github.helenusdriver.driver.StatementManager;
import com.github.helenusdriver.driver.Truncate;
import com.github.helenusdriver.driver.Update;
import com.github.helenusdriver.driver.Using;
import com.github.helenusdriver.driver.Delete.Builder;
import com.github.helenusdriver.driver.Select.Selection;
import com.github.helenusdriver.driver.impl.Utils.CNameSequence;
import com.github.helenusdriver.driver.info.ClassInfo;
import com.github.helenusdriver.driver.info.EntityFilter;
import com.github.helenusdriver.driver.info.TableInfo;
import com.github.helenusdriver.persistence.DataType;
import com.github.helenusdriver.persistence.Entity;
import com.github.helenusdriver.persistence.RootEntity;
import com.github.helenusdriver.persistence.TypeEntity;

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
   * Holds a bind marker.
   *
   * @author paouelle
   */
  static final Object BIND_MARKER = new Object() {};

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
  private final Session session;

  /**
   * Holds the default replication factor to use when creating keyspaces with
   * a SIMPLE strategy and 0 or the default replication factor has been defined
   * in the POJO.
   *
   * @author paouelle
   */
  private int defaultReplicationFactor = 2;

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
   * @throws NullPointerException if <code>initializer</code> is <code>null</code>
   * @throws IllegalArgumentException if the list of contact points provided
   *         by <code>initializer</code> is empty or if not all those contact
   *         points have the same port.
   * @throws SecurityException if the statement manager reference has already
   *         been set
   */
  public StatementManagerImpl(Cluster.Initializer initializer) {
    com.github.helenusdriver.commons.lang3.Validate.notNull(logger, initializer, "invalid null initializer");
    this.cluster = Cluster.buildFrom(initializer);
    this.session = cluster.connect();
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
   * @throws NullPointerException if <code>initializer</code> is <code>null</code>
   * @throws IllegalArgumentException if the list of contact points provided
   *         by <code>initializer</code> is empty or if not all those contact
   *         points have the same port or again the default replication factor
   *         is less or equal to 0
   * @throws SecurityException if the statement manager reference has already
   *         been set
   */
  public StatementManagerImpl(
    Cluster.Initializer initializer, int defaultReplicationFactor
  ) {
    this(initializer);
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
   * @param  filters optional entity filters to register
   * @throws NullPointerException if <code>initializer</code> is <code>null</code>
   * @throws IllegalArgumentException if the list of contact points provided
   *         by <code>initializer</code> is empty or if not all those contact
   *         points have the same port or again the default replication factor
   *         is less or equal to 0
   * @throws SecurityException if the statement manager reference has already
   *         been set
   */
  public StatementManagerImpl(
    Cluster.Initializer initializer,
    int defaultReplicationFactor,
    EntityFilter... filters
  ) {
    this(initializer, filters);
    setDefaultReplicationFactor(defaultReplicationFactor);
  }

  /**
   * Instantiates a new <code>StatementManagerImpl</code> object.
   *
   * @author paouelle
   *
   * @param  initializer the cluster initializer to use to initialize Cassandra's
   *         cluster
   * @param  filters optional entity filters to register
   * @throws NullPointerException if <code>initializer</code> is <code>null</code>
   * @throws IllegalArgumentException if the list of contact points provided
   *         by <code>initializer</code> is empty or if not all those contact
   *         points have the same port.
   * @throws SecurityException if the statement manager reference has already
   *         been set
   */
  public StatementManagerImpl(
    Cluster.Initializer initializer, EntityFilter... filters
  ) {
    this(initializer);
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
   * @param  cnames optional entity filter class names to register
   * @throws NullPointerException if <code>initializer</code> is <code>null</code>
   * @throws IllegalArgumentException if the list of contact points provided
   *         by <code>initializer</code> is empty or if not all those contact
   *         points have the same port.
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
  public StatementManagerImpl(Cluster.Initializer initializer, String... cnames)
    throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    this(initializer);
    if (cnames != null) {
      for (final String cname: cnames) {
        final Class<?> clazz = Class.forName(cname);

        org.apache.commons.lang3.Validate.isTrue(
          EntityFilter.class.isAssignableFrom(clazz),
          "invalid entity filter class: %s", cname
        );
        this.filters.add(EntityFilter.class.cast(clazz.newInstance()));
      }
    }
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.StatementManager#getClassInfo(java.lang.Class)
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
   * @see com.github.helenusdriver.driver.StatementManager#select(java.lang.Class, java.lang.CharSequence[])
   */
  @Override
  protected <T> Select.Builder<T> select(Class<T> clazz, CharSequence... columns) {
    return new SelectImpl.BuilderImpl<>(
      getClassInfoImpl(clazz).newContext(),
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
   * @see com.github.helenusdriver.driver.StatementManager#select(java.lang.Class)
   */
  @Override
  protected <T> Selection<T> select(Class<T> clazz) {
    return new SelectImpl.SelectionImpl<>(
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
   * @see com.github.helenusdriver.driver.StatementManager#insert(java.lang.Object)
   */
  @SuppressWarnings("unchecked")
  @Override
  protected <T> Insert<T> insert(T object) {
    org.apache.commons.lang3.Validate.notNull(object, "invalid null object");
    return new InsertImpl<>(
      getClassInfoImpl((Class<T>)object.getClass()).newContext(object),
      this,
      bridge
    );
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.StatementManager#update(java.lang.Object)
   */
  @SuppressWarnings("unchecked")
  @Override
  protected <T> Update<T> update(T object) {
    return new UpdateImpl<>(
      getClassInfoImpl((Class<T>)object.getClass()).newContext(object),
      this,
      bridge
    );
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.StatementManager#update(java.lang.Object, java.lang.String[])
   */
  @SuppressWarnings("unchecked")
  @Override
  protected <T> Update<T> update(T object, String... tables) {
    org.apache.commons.lang3.Validate.notNull(object, "invalid null object");
    return new UpdateImpl<>(
      getClassInfoImpl((Class<T>)object.getClass()).newContext(object),
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
   * @see com.github.helenusdriver.driver.StatementManager#delete(java.lang.Object, java.lang.String[])
   */
  @SuppressWarnings("unchecked")
  @Override
  protected <T> Builder<T> delete(T object, String... columns) {
    org.apache.commons.lang3.Validate.notNull(object, "invalid null object");
    return new DeleteImpl.BuilderImpl<>(
      getClassInfoImpl((Class<T>)object.getClass()).newContext(object),
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
   * @see com.github.helenusdriver.driver.StatementManager#delete(java.lang.Object)
   */
  @SuppressWarnings("unchecked")
  @Override
  protected <T> com.github.helenusdriver.driver.Delete.Selection<T> delete(T object) {
    org.apache.commons.lang3.Validate.notNull(object, "invalid null object");
    return new DeleteImpl.SelectionImpl<>(
      getClassInfoImpl((Class<T>)object.getClass()).newContext(object),
      this,
      bridge
    );
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.StatementManager#delete(java.lang.Class, java.lang.String[])
   */
  @Override
  protected <T> Delete.Builder<T> delete(Class<T> clazz, String... columns) {
    org.apache.commons.lang3.Validate.notNull(clazz, "invalid null class");
    return new DeleteImpl.BuilderImpl<>(
      getClassInfoImpl(clazz).newContext(),
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
   * @see com.github.helenusdriver.driver.StatementManager#delete(java.lang.Class)
   */
  @Override
  protected <T> Delete.Selection<T> delete(Class<T> clazz) {
    org.apache.commons.lang3.Validate.notNull(clazz, "invalid null class");
    return new DeleteImpl.SelectionImpl<>(
      getClassInfoImpl(clazz).newContext(),
      this,
      bridge
    );
  };

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.StatementManager#batch(com.github.helenusdriver.driver.BatchableStatement[])
   */
  @Override
  protected Batch batch(BatchableStatement<?, ?>... statements) {
    return new BatchImpl(statements, true, this, bridge);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.StatementManager#batch(java.lang.Iterable)
   */
  @Override
  protected Batch batch(Iterable<BatchableStatement<?, ?>> statements) {
    return new BatchImpl(statements, true, this, bridge);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.StatementManager#unloggedBatch(com.github.helenusdriver.driver.BatchableStatement[])
   */
  @Override
  protected Batch unloggedBatch(BatchableStatement<?, ?>... statements) {
    return new BatchImpl(statements, false, this, bridge);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.StatementManager#unloggedBatch(java.lang.Iterable)
   */
  @Override
  protected Batch unloggedBatch(Iterable<BatchableStatement<?, ?>> statements) {
    return new BatchImpl(statements, false, this, bridge);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.StatementManager#regular(com.datastax.driver.core.RegularStatement)
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
   * @see com.github.helenusdriver.driver.StatementManager#createKeyspace(java.lang.Class)
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
   * @see com.github.helenusdriver.driver.StatementManager#createTable(Class)
   */
  @Override
  protected <T> CreateTable<T> createTable(Class<T> clazz) {
    return new CreateTableImpl<>(
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
   * @see com.github.helenusdriver.driver.StatementManager#createTable(Class, String[])
   */
  @Override
  protected <T> CreateTable<T> createTable(Class<T> clazz, String... tables) {
    return new CreateTableImpl<>(
      getClassInfoImpl(clazz).newContext(),
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
   * @see com.github.helenusdriver.driver.StatementManager#createIndex(java.lang.Class)
   */
  @Override
  protected <T> CreateIndex.Builder<T> createIndex(Class<T> clazz) {
    return new CreateIndexImpl.BuilderImpl<>(
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
   * @see com.github.helenusdriver.driver.StatementManager#createSchema(java.lang.Class)
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
   * @see com.github.helenusdriver.driver.StatementManager#createSchemas(java.lang.String)
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
   * @see com.github.helenusdriver.driver.StatementManager#createMatchingSchemas(java.lang.String)
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
   * @see com.github.helenusdriver.driver.StatementManager#truncate(java.lang.Class)
   */
  @Override
  protected <T> Truncate<T> truncate(Class<T> clazz) {
    return new TruncateImpl<>(
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
   * @see com.github.helenusdriver.driver.StatementManager#truncate(Class, String[])
   */
  @Override
  protected <T> Truncate<T> truncate(Class<T> clazz, String... tables) {
    return new TruncateImpl<>(
      getClassInfoImpl(clazz).newContext(),
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
   * @see com.github.helenusdriver.driver.StatementManager#sequence(com.github.helenusdriver.driver.SequenceableStatement[])
   */
  @Override
  protected Sequence sequence(SequenceableStatement<?, ?>... statements) {
    return new SequenceImpl(
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
   * @see com.github.helenusdriver.driver.StatementManager#sequence(java.lang.Iterable)
   */
  @Override
  protected Sequence sequence(Iterable<SequenceableStatement<?, ?>> statements) {
    return new SequenceImpl(
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
   * @see com.github.helenusdriver.driver.StatementManager#quote(java.lang.String)
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
   * @see com.github.helenusdriver.driver.StatementManager#token(java.lang.String)
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
   * @see com.github.helenusdriver.driver.StatementManager#token(java.lang.String[])
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
   * @see com.github.helenusdriver.driver.StatementManager#isSuffixedLikeObject()
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
   * @see com.github.helenusdriver.driver.StatementManager#isSuffixedLike(java.lang.Object)
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
   * @see com.github.helenusdriver.driver.StatementManager#isPartitionedLikeObject()
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
   * @see com.github.helenusdriver.driver.StatementManager#isPartitionedLike(java.lang.Object)
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
   * @see com.github.helenusdriver.driver.StatementManager#isObject()
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
   * @see com.github.helenusdriver.driver.StatementManager#is(java.lang.Object)
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
   * @see com.github.helenusdriver.driver.StatementManager#eq(java.lang.CharSequence, java.lang.Object)
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
   * @see com.github.helenusdriver.driver.StatementManager#in(java.lang.CharSequence, java.lang.Object[])
   */
  @Override
  protected Clause in(CharSequence name, Object... values) {
    return new ClauseImpl.InClauseImpl(name, Arrays.asList(values));
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.StatementManager#in(java.lang.CharSequence, java.util.Collection)
   */
  @Override
  protected Clause in(CharSequence name, Collection<?> values) {
    return new ClauseImpl.InClauseImpl(name, values);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.StatementManager#in(java.lang.CharSequence, java.util.stream.Stream)
   */
  @Override
  protected Clause in(CharSequence name, Stream<?> values) {
    return new ClauseImpl.InClauseImpl(name, values.collect(Collectors.toList()));
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.StatementManager#in(java.lang.CharSequence, int, int)
   */
  @Override
  protected Clause in(CharSequence name, int from, int to) {
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
   * @see com.github.helenusdriver.driver.StatementManager#lt(java.lang.CharSequence, java.lang.Object)
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
   * @see com.github.helenusdriver.driver.StatementManager#lte(java.lang.CharSequence, java.lang.Object)
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
   * @see com.github.helenusdriver.driver.StatementManager#gt(java.lang.CharSequence, java.lang.Object)
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
   * @see com.github.helenusdriver.driver.StatementManager#gte(java.lang.CharSequence, java.lang.Object)
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
   * @see com.github.helenusdriver.driver.StatementManager#asc(java.lang.CharSequence)
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
   * @see com.github.helenusdriver.driver.StatementManager#desc(java.lang.CharSequence)
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
   * @see com.github.helenusdriver.driver.StatementManager#timestamp(long)
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
   * @see com.github.helenusdriver.driver.StatementManager#ttl(int)
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
   * @see com.github.helenusdriver.driver.StatementManager#bindMarker()
   */
  @Override
  protected Object bindMarker() {
    return StatementManagerImpl.BIND_MARKER;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.StatementManager#setFromObject(java.lang.CharSequence)
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
   * @see com.github.helenusdriver.driver.StatementManager#setFrom(java.lang.Object, java.lang.CharSequence)
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
   * @see com.github.helenusdriver.driver.StatementManager#set(java.lang.CharSequence, java.lang.Object)
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
   * @see com.github.helenusdriver.driver.StatementManager#set(java.lang.CharSequence, java.lang.Object, java.lang.Object)
   */
  @Override
  protected Assignment set(CharSequence name, Object value, Object old) {
    if (!Objects.equals(value, old)) {
      return new AssignmentImpl.ReplaceAssignmentImpl(name, value, old);
    } // else - no change so handle as normal set
    return new AssignmentImpl.SetAssignmentImpl(name, value);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.StatementManager#setAllFromObject()
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
   * @see com.github.helenusdriver.driver.StatementManager#setAllFrom(java.lang.Object)
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
   * @see com.github.helenusdriver.driver.StatementManager#incr(java.lang.CharSequence, long)
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
   * @see com.github.helenusdriver.driver.StatementManager#decr(java.lang.CharSequence, long)
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
   * @see com.github.helenusdriver.driver.StatementManager#prepend(java.lang.CharSequence, java.lang.Object)
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
   * @see com.github.helenusdriver.driver.StatementManager#prependAll(java.lang.CharSequence, java.util.List)
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
   * @see com.github.helenusdriver.driver.StatementManager#append(java.lang.CharSequence, java.lang.Object)
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
   * @see com.github.helenusdriver.driver.StatementManager#appendAll(java.lang.CharSequence, java.util.List)
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
   * @see com.github.helenusdriver.driver.StatementManager#discard(java.lang.CharSequence, java.lang.Object)
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
   * @see com.github.helenusdriver.driver.StatementManager#discardAll(java.lang.CharSequence, java.util.List)
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
   * @see com.github.helenusdriver.driver.StatementManager#setIdx(java.lang.CharSequence, int, java.lang.Object)
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
   * @see com.github.helenusdriver.driver.StatementManager#add(java.lang.CharSequence, java.lang.Object)
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
   * @see com.github.helenusdriver.driver.StatementManager#addAll(java.lang.CharSequence, java.util.Set)
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
   * @see com.github.helenusdriver.driver.StatementManager#remove(java.lang.CharSequence, java.lang.Object)
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
   * @see com.github.helenusdriver.driver.StatementManager#removeAll(java.lang.CharSequence, java.util.Set)
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
   * @see com.github.helenusdriver.driver.StatementManager#put(java.lang.CharSequence, java.lang.Object, java.lang.Object)
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
   * @see com.github.helenusdriver.driver.StatementManager#putAll(java.lang.CharSequence, java.util.Map)
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
   * @see com.github.helenusdriver.driver.StatementManager#replication(javax.json.JsonObject)
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
   * @see com.github.helenusdriver.driver.StatementManager#durableWrites(boolean)
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
   * @see com.github.helenusdriver.driver.StatementManager#raw(java.lang.String)
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
   * @see com.github.helenusdriver.driver.StatementManager#fcall(java.lang.String, java.lang.Object[])
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
   * @see com.github.helenusdriver.driver.StatementManager#column(java.lang.String)
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
   * @see com.github.helenusdriver.driver.StatementManager#getColumnNamesFor(java.lang.Class, java.lang.reflect.Field)
   */
  @Override
  protected <T> Set<String> getColumnNamesFor(Class<T> clazz, Field field) {
    final ClassInfoImpl<?> cinfo = getClassInfoImpl(clazz);
    final Set<String> names = new LinkedHashSet<>(cinfo.getNumTables()); // preserve order

    for (final TableInfoImpl<?> tinfo: cinfo.getTables()) {
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
   * @see com.github.helenusdriver.driver.StatementManager#getSession()
   */
  @Override
  public Session getSession() {
    return session;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.StatementManager#getCluster()
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
   * SIMPLE strategy and do not specify a factor.
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
  public CloseFuture close() {
    return cluster.closeAsync();
  }
}

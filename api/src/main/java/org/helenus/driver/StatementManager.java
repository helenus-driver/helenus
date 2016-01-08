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
package org.helenus.driver;

import java.lang.reflect.Field;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import javax.json.JsonObject;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import org.helenus.driver.info.ClassInfo;
import org.helenus.driver.info.TableInfo;
import org.helenus.driver.persistence.Column;

/**
 * The <code>StatementManager</code> abstract class is used to maintain and
 * manage Cassandra statements defined in the system.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 15, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
public abstract class StatementManager {
  /**
   * The <code>Context</code> interface provides statement context specific to
   * a POJO class.
   *
   * @copyright 2015-2015 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 15, 2015 - paouelle - Creation
   *
   * @param <T> The type of POJO represented by this class
   *
   * @since 1.0
   */
  public interface Context<T> {
    /**
     * Gets the class of POJO represented by this class info object.
     *
     * @author paouelle
     *
     * @return the non-<code>null</code> class of POJO represented by this context
     */
    public Class<T> getObjectClass();

    /**
     * Converts the specified result row into a POJO object defined by this
     * class information and context.
     *
     * @author paouelle
     *
     * @param  row the result row to convert into a POJO
     * @return the POJO object corresponding to the given result row or
     *         <code>null</code> if not of the appropriate type
     * @throws ObjectConversionException if unable to convert to a POJO
     */
    public T getObject(Row row);
  }

  /**
   * This field is a reference to the <code>StatementManager</code> which is
   * initialized by the manager.
   *
   * @author paouelle
   */
  private static volatile StatementManager manager = null;

  /**
   * Gets the statement manager.
   *
   * @author paouelle
   *
   * @return the statement manager installed in the system
   * @throws IllegalStateException if the statement manager reference has not
   *         yet been initialized
   */
  static final StatementManager getManager() {
    if (StatementManager.manager == null) {
      throw new IllegalStateException("statement manager is not initialized");
    }
    return StatementManager.manager;
  }

  /**
   * Sets the statement manager reference to be used by all statements and return
   * a statement bridge that can be used to access protected statement information
   * and services.
   * <p>
   * This must be done during initialization of the system statement manager.
   *
   * @author paouelle
   *
   * @param  manager a non-<code>null</code> reference to the statement manager
   *         to assign
   * @return a statement bridge providing access to protected statement information
   *         and services
   * @throws SecurityException if the statement manager reference has already
   *         been set
   */
  protected static final StatementBridge setManager(StatementManager manager)
    throws SecurityException {
    if (StatementManager.manager != null) {
      throw new SecurityException("statement manager is already defined");
    }
    StatementManager.manager = manager;
    return new StatementBridge();
  }

  /**
   * Gets a class info structure that defines the specified POJO class.
   *
   * @author paouelle
   *
   * @param <T> The type of POJO associated with this statement
   *
   * @param  clazz the non-<code>null</code> class of POJO for which to get a
   *         class info object for
   * @return the non-<code>null</code> class info object representing the given
   *         POJO class
   * @throws IllegalArgumentException if <code>clazz</code> doesn't represent
   *         a valid POJO class
   */
  protected abstract <T> ClassInfo<T> getClassInfo(Class<T> clazz);

  /**
   * Start building a new SELECT statement that selects the provided names.
   * <p>
   * <i>Note:</i> that {@code select(clazz, c1, c2)} is just a shortcut for
   * {@code select(clazz).column(c1).column(c2)}.
   *
   * @author paouelle
   *
   * @param <T> The type of POJO associated with the statement.
   *
   * @param  clazz the class of POJO associated with this statement
   * @param  columns the columns names that should be selected by the statement.
   * @return an in-construction SELECT statement (you will need to provide at
   *         least a FROM clause to complete the statement).
   * @throws NullPointerException if <code>clazz</code> is <code>null</code>
   * @throws IllegalArgumentException if <code>clazz</code> doesn't represent a
   *         valid POJO class or any of the specified columns are not defined
   *         by the POJO
   */
  protected abstract <T> Select.Builder<T> select(
    Class<T> clazz, CharSequence... columns
  );

  /**
   * Starts building a new SELECT statement.
   *
   * @author paouelle
   *
   * @param <T> The type of POJO associated with the statement.
   *
   * @param  clazz the class of POJO associated with this statement
   * @return an in-construction SELECT statement (you will need to provide a column
   *         selection and at least a FROM clause to complete the statement).
   * @throws NullPointerException if <code>clazz</code> is <code>null</code>
   * @throws IllegalArgumentException if <code>clazz</code> doesn't represent a
   *         valid POJO class
   */
  protected abstract <T> Select.Selection<T> select(Class<T> clazz);

  /**
   * Start building a new SELECT statement that selects the provided names.
   * <p>
   * <i>Note:</i> that {@code select(table, c1, c2)} is just a shortcut for
   * {@code select(table).column(c1).column(c2)}.
   *
   * @author paouelle
   *
   * @param <T> The type of POJO associated with the statement.
   *
   * @param  table the POJO table associated with this statement to select from
   * @param  columns the columns names that should be selected by the statement.
   * @return an in-construction SELECT statement
   * @throws NullPointerException if <code>table</code> is <code>null</code>
   * @throws IllegalArgumentException if <code>table</code> doesn't represent a
   *         valid POJO class or any of the specified columns are not defined
   *         by the POJO
   */
  protected abstract <T> Select<T> selectFrom(
    TableInfo<T> table, CharSequence... columns
  );

  /**
   * Starts building a new SELECT statement.
   *
   * @author paouelle
   *
   * @param <T> The type of POJO associated with the statement.
   *
   * @param  table the POJO table associated with this statement to select from
   * @return an in-construction SELECT statement
   * @throws NullPointerException if <code>table</code> is <code>null</code>
   * @throws IllegalArgumentException if <code>table</code> doesn't represent a
   *         valid POJO class
   */
  protected abstract <T> Select.TableSelection<T> selectFrom(TableInfo<T> table);

  /**
   * Starts building a new INSERT statement for the following POJO object.
   * <p>
   * <i>Note:</i> If no columns are added as part of the INSERT statement then
   * all columns and values from the POJO are automatically added.
   *
   * @author paouelle
   *
   * @param <T> The type of POJO associated with the statement.
   *
   * @param  object the POJO object to be inserted
   * @return an in-construction INSERT statement
   * @throws NullPointerException if <code>object</code> is <code>null</code>
   * @throws IllegalArgumentException if <code>object</code> is not a valid POJO
   */
  protected abstract <T> Insert.Builder<T> insert(T object);

  /**
   * Starts building a new UPDATE statement for the following POJO object to update
   * all the required tables defined by the POJO based on the assignment clauses
   * specified.
   * <p>
   * <i>Note:</i> The primary key columns are automatically added to the
   * "WHERE" part of the UPDATE statement if no clauses are added to the "WHERE"
   * part. In addition, if no assignments are specified via the "WITH" part of
   * the UPDATE statement, all non-primary columns will be automatically added.
   *
   * @author paouelle
   *
   * @param <T> The type of POJO associated with the statement.
   *
   * @param  object the POJO object to be updated
   * @return an in-construction UPDATE statement
   * @throws NullPointerException if <code>object</code> is <code>null</code>
   * @throws IllegalArgumentException if <code>object</code> is not a valid POJO
   */
  protected abstract <T> Update<T> update(T object);

  /**
   * Starts building a new UPDATE statement for the following POJO object to update
   * all the specified tables defined by the POJO based on the assignment clauses
   * specified.
   * <p>
   * <i>Note:</i> The primary key columns are automatically added to the
   * "WHERE" part of the UPDATE statement if no clauses are added to the "WHERE"
   * part. In addition, if no assignments are specified via the "WITH" part of
   * the UPDATE statement, all non-primary columns will be automatically added.
   *
   * @author paouelle
   *
   * @param <T> The type of POJO associated with the statement.
   *
   * @param  object the POJO object to be updated
   * @param  tables the tables to update
   * @return an in-construction UPDATE statement
   * @throws NullPointerException if <code>object</code> is <code>null</code>
   * @throws IllegalArgumentException if <code>object</code> is not a valid POJO
   *         or if any of the specified tables are not defined in the POJO
   */
  protected abstract <T> Update<T> update(T object, String... tables);

  /**
   * Start building a new DELETE statement that deletes the provided columns from
   * the specified POJO.
   *
   * @param <T> The type of POJO associated with the statement.
   *
   * @param  object the POJO object to delete columns from
   * @param  columns the columns names that should be deleted by the statement
   * @return an in-construction DELETE statement (At least a FROM and a WHERE
   *         clause needs to be provided to complete the statement).
   * @throws NullPointerException if <code>object</code> is <code>null</code>
   * @throws IllegalArgumentException if <code>object</code> is not a valid POJO
   *         or if any of the specified columns are not defined in the POJO
   */
  protected abstract <T> Delete.Builder<T> delete(T object, String... columns);

  /**
   * Start building a new DELETE statement for the specified POJO.
   *
   * @param <T> The type of POJO associated with the statement.
   *
   * @param  object the POJO object to delete columns from
   * @return an in-construction SELECT statement (you will need to provide a
   *         column selection and at least a FROM and a WHERE clause to complete
   *         the statement).
   * @throws NullPointerException if <code>object</code> is <code>null</code>
   * @throws IllegalArgumentException if <code>object</code> is not a valid POJO
   */
  protected abstract <T> Delete.Selection<T> delete(T object);

  /**
   * Start building a new DELETE statement that deletes the provided columns from
   * the specified POJO class.
   *
   * @param <T> The type of POJO associated with the statement.
   *
   * @param  clazz the class of POJO to delete columns for
   * @param  columns the columns names that should be deleted by the statement
   * @return an in-construction DELETE statement (At least a FROM and a WHERE
   *         clause needs to be provided to complete the statement).
   * @throws NullPointerException if <code>clazz</code> is <code>null</code>
   * @throws IllegalArgumentException if <code>clazz</code> doesn't represent a
   *         valid POJO class or any of the specified columns are not defined
   *         by the POJO
   */
  protected abstract <T> Delete.Builder<T> delete(Class<T> clazz, String... columns);

  /**
   * Start building a new DELETE statement for the specified POJO class.
   *
   * @param <T> The type of POJO associated with the statement.
   *
   * @param  clazz the class of POJO to delete columns for
   * @return an in-construction SELECT statement (you will need to provide a
   *         column selection and at least a FROM and a WHERE clause to complete
   *         the statement).
   * @throws NullPointerException if <code>clazz</code> is <code>null</code>
   * @throws IllegalArgumentException if <code>clazz</code> doesn't represent a
   *         valid POJO class
   */
  protected abstract <T> Delete.Selection<T> delete(Class<T> clazz);

  /**
   * Starts building a new BATCH statement on the provided statements.
   * <p>
   * This method will build a logged batch (this is the default in CQL3). To
   * create unlogged batches, use {@link #unloggedBatch}. Also note that
   * for convenience, if the provided statements are counter statements, this
   * method will create a COUNTER batch even though COUNTER batches are never
   * logged (so for counters, using this method is effectively equivalent to
   * using {@link #unloggedBatch}).
   * <p>
   * <i>Note:</i> This method allows one to register a recorder with the batch
   * such as to be notified whenever a pojo-specific statement is added to the
   * batch or any batches added to the returned batch recursively.
   *
   * @param  recorder the optional recorder to register
   * @param  statements the statements to batch
   * @return a new {@code BatchableStatement} that batch {@code statements}
   * @throws NullPointerException if <code>recorder</code>, <code>statement</code>,
   *         or any of the statements are <code>null</code>
   * @throws IllegalArgumentException if counter and non-counter operations
   *         are mixed
   */
  protected abstract Batch batch(
    Optional<Recorder> recorder, BatchableStatement<?, ?>... statements
  );

  /**
   * Starts building a new BATCH statement on the provided statements.
   * <p>
   * This method will build a logged batch (this is the default in CQL3). To
   * create unlogged batches, use {@link #unloggedBatch}. Also note that
   * for convenience, if the provided statements are counter statements, this
   * method will create a COUNTER batch even though COUNTER batches are never
   * logged (so for counters, using this method is effectively equivalent to
   * using {@link #unloggedBatch}).
   * <p>
   * <i>Note:</i> This method allows one to register a recorder with the batch
   * such as to be notified whenever a pojo-specific statement is added to the
   * batch or any batches added to the returned batch recursively.
   *
   * @param  recorder the optional recorder to register
   * @param  statements the statements to batch
   * @return a new {@code BatchableStatement} that batch {@code statements}
   * @throws NullPointerException if <code>recorder</code>, <code>statement</code>,
   *         or any of the statements are <code>null</code>
   * @throws IllegalArgumentException if counter and non-counter operations
   *         are mixed
   */
  protected abstract Batch batch(
    Optional<Recorder> recorder, Iterable<BatchableStatement<?, ?>> statements
  );

  /**
   * Built a new UNLOGGED BATCH statement on the provided statements.
   * <p>
   * Compared to logged batches (the default), unlogged batch don't
   * use the distributed batch log server side and as such are not
   * guaranteed to be atomic. In other words, if an unlogged batch
   * timeout, some of the batched statements may have been persisted
   * while some have not. Unlogged batch will however be slightly
   * faster than logged batch.
   * <p>
   * If the statements added to the batch are counter statements, the
   * resulting batch will be a COUNTER one.
   * <p>
   * <i>Note:</i> This method allows one to register a recorder with the batch
   * such as to be notified whenever a pojo-specific statement is added to the
   * batch or any batches added to the returned batch recursively.
   *
   * @param  recorder the optional recorder to register
   * @param  statements the statements to batch
   * @return a new {@code BatchableStatement} that batch {@code statements}
   *         without using the batch log
   * @throws NullPointerException if <code>statement</code> or any of the
   *         statements are <code>null</code>
   * @throws IllegalArgumentException if counter and non-counter operations
   *         are mixed
   */
  protected abstract Batch unloggedBatch(
    Optional<Recorder> recorder, BatchableStatement<?, ?>... statements
  );

  /**
   * Built a new UNLOGGED BATCH statement on the provided statements.
   * <p>
   * Compared to logged batches (the default), unlogged batch don't
   * use the distributed batch log server side and as such are not
   * guaranteed to be atomic. In other words, if an unlogged batch
   * timeout, some of the batched statements may have been persisted
   * while some have not. Unlogged batch will however be slightly
   * faster than logged batch.
   * <p>
   * If the statements added to the batch are counter statements, the
   * resulting batch will be a COUNTER one.
   * <p>
   * <i>Note:</i> This method allows one to register a recorder with the batch
   * such as to be notified whenever a pojo-specific statement is added to the
   * batch or any batches added to the returned batch recursively.
   *
   * @param  recorder the optional recorder to register
   * @param  statements the statements to batch
   * @return a new {@code BatchableStatement} that batch {@code statements}
   *         without using the batch log
   * @throws NullPointerException if <code>statement</code> or any of the
   *         statements are <code>null</code>
   * @throws IllegalArgumentException if counter and non-counter operations
   *         are mixed
   */
  protected abstract Batch unloggedBatch(
    Optional<Recorder> recorder, Iterable<BatchableStatement<?, ?>> statements
  );

  /**
   * Wraps a Cassandra regular statement into a statement that can be executed
   * or batched using this API.
   *
   * @author paouelle
   *
   * @param  statement the raw statement to be wrapped
   * @return a new {@code RegularStatement} that corresponds to the specified
   *         raw statement
   * @throws NullPointerException if <code>statement</code> is <code>null</code>
   */
  protected abstract RegularStatement regular(
    com.datastax.driver.core.RegularStatement statement
  );

  /**
   * Starts building a new CREATE KEYSPACE statement for the given POJO class.
   *
   * @author paouelle
   *
   * @param <T> The type of POJO associated with the statement.
   *
   * @param  clazz the class of POJO associated with this statement
   * @return a new CREATE KEYSPACE statement
   * @throws NullPointerException if <code>clazz</code> is <code>null</code>
   * @throws IllegalArgumentException if <code>clazz</code> doesn't represent a
   *         valid POJO class
   */
  protected abstract <T> CreateKeyspace<T> createKeyspace(Class<T> clazz);

  /**
   * Starts building a new CREATE TYPE statement for the given POJO class.
   * <p>
   * The column schema is automatically extracted from the POJO class definition.
   *
   * @author paouelle
   *
   * @param <T> The type of POJO associated with the statement.
   *
   * @param  clazz the class of POJO associated with this statement
   * @return a new CREATE TYPE statement
   * @throws NullPointerException if <code>clazz</code> is <code>null</code>
   * @throws IllegalArgumentException if <code>clazz</code> doesn't represent a
   *         valid user-defined type POJO class
   */
  protected abstract <T> CreateType<T> createType(Class<T> clazz);

  /**
   * Starts building a new CREATE TABLE statement for the given POJO class. This
   * might actually results in multiple CREATE TABLE statements.
   * <p>
   * <i>Note:</i> If the POJO defines multiple tables, executing this
   * statement with default values will actually create all tables. Since table creation
   * cannot be batched with Cassandra, this will result in a non-atomic creation
   * of all tables. The process will stop at first failure and will not revert back
   * the created tables if any.
   * <p>
   * The column schema is automatically extracted from the POJO class definition.
   *
   * @author paouelle
   *
   * @param <T> The type of POJO associated with the statement.
   *
   * @param  clazz the class of POJO associated with this statement
   * @return a new CREATE TABLE statement
   * @throws NullPointerException if <code>clazz</code> is <code>null</code>
   * @throws IllegalArgumentException if <code>clazz</code> doesn't represent a
   *         valid POJO class
   */
  protected abstract <T> CreateTable<T> createTable(Class<T> clazz);

  /**
   * Starts building a new CREATE TABLE statement for the given POJO class. This
   * might actually results in multiple CREATE TABLE statements.
   * <p>
   * <i>Note:</i> If multiple tables are specified, executing this statement
   * will actually create all these tables. Since table creation cannot be
   * batched with Cassandra, this will result in a non-atomic creation of all
   * tables. The process will stop at first failure and will not revert back
   * the created tables if any.
   * <p>
   * The column schema is automatically extracted from the POJO class definition.
   *
   * @author paouelle
   *
   * @param <T> The type of POJO associated with the statement.
   *
   * @param  clazz the class of POJO associated with this statement
   * @param  tables the tables to create
   * @return a new CREATE TABLE statement
   * @throws NullPointerException if <code>clazz</code> is <code>null</code>
   * @throws IllegalArgumentException if <code>clazz</code> doesn't represent a
   *         valid POJO class or if any of the specified tables are not defined
   *         in the POJO
   */
  protected abstract <T> CreateTable<T> createTable(Class<T> clazz, String... tables);

  /**
   * Starts building a new CREATE INDEX statement for the given POJO class.
   * This might actually results in multiple CREATE INDEX statements.
   * <p>
   * <i>Note:</i> If the POJO defines multiple tables and indexes, executing this
   * statement will potentially create all indexes for all tables. Since index
   * creation cannot be batched with Cassandra, this will result in a non-atomic
   * creation of all indexes. The process will stop at first failure and will
   * not revert back the created indexes if any.
   *
   * @author paouelle
   *
   * @param <T> The type of POJO associated with the statement.
   *
   * @param  clazz the class of POJO associated with this statement
   * @return an in-build construct for the CREATE INDEX statement
   * @throws NullPointerException if <code>clazz</code> is <code>null</code>
   * @throws IllegalArgumentException if <code>clazz</code> doesn't represent a
   *         valid POJO class
   */
  protected abstract <T> CreateIndex.Builder<T> createIndex(Class<T> clazz);

  /**
   * Starts building a new CREATE SCHEMA statement for the given POJO class.
   * This will create all the required elements to support the schema for a given
   * POJO. It will take care of creating the required keyspace, tables, types, and
   * indexes.
   * <p>
   * <i>Note:</i> If the POJO defines multiple tables and indexes, executing this
   * statement will potentially create the keyspace, all tables, types, and all
   * indexes for all tables. Since keyspace, table, type, and index creation
   * cannot be batched with Cassandra, this will result in a non-atomic creation
   * of everything. The process will stop at first failure and will not revert
   * back the created keyspace, tables, types, or indexes if any.
   *
   * @author paouelle
   *
   * @param <T> The type of POJO associated with the statement.
   *
   * @param  clazz the class of POJO associated with this statement
   * @return an in-build construct for the CREATE SCHEMA statement
   * @throws NullPointerException if <code>clazz</code> is <code>null</code>
   * @throws IllegalArgumentException if <code>clazz</code> doesn't represent a
   *         valid POJO class
   */
  protected abstract <T> CreateSchema<T> createSchema(Class<T> clazz);

  /**
   * Starts building a new CREATE SCHEMAS statement for all POJO classes defined
   * in a given package. This will create all the required elements to support
   * the schema for each POJO. It will take care of creating the required
   * keyspaces, tables, types, and indexes.
   * <p>
   * <i>Note:</i> This statement will create the schemas for all POJOs for which
   * the defined keyspace can be computed with any of the specified suffixes via
   * the WHERE clauses.
   * <p>
   * <i>Note:</i> Executing this statement will potentially create the keyspaces,
   * all tables, types, and all indexes for all tables. Since keyspace, table,
   * type, and index creation cannot be batched with Cassandra, this will result
   * in a non-atomic creation of everything. The process will stop at first
   * failure and will not revert back the created keyspaces, tables, types, or
   * indexes if any.
   *
   * @author paouelle
   *
   * @param  pkg the package where the POJOs for which to create schemas are
   *         defined
   * @return a new CREATE SCHEMAS statement
   * @throws NullPointerException if <code>pkg</code> is <code>null</code>
   * @throws IllegalArgumentException if two entities defines the same keyspace
   *         with different options or an @Entitiy annotated class doesn't represent
   *         a valid POJO class or if no entities are found
   */
  protected abstract CreateSchemas createSchemas(String pkg);

  /**
   * Starts building a new CREATE SCHEMAS statement for all POJO classes defined
   * in a given package. This will create all the required elements to support
   * the schema for each POJO. It will take care of creating the required
   * keyspaces, tables, types, and indexes.
   * <p>
   * <i>Note:</i> This statement will create the schemas for only the POJOs for
   * which the defined keyspace can be computed with exactly all of the specified
   * suffixes via the WHERE clauses.
   * <p>
   * <i>Note:</i> Executing this statement will potentially create the keyspaces,
   * all tables, types,  and all indexes for all tables. Since keyspace, table,
   * types, and index creation cannot be batched with Cassandra, this will result
   * in a non-atomic creation of everything. The process will stop at first
   * failure and will not revert back the created keyspaces, tables, types, or
   * indexes if any.
   *
   * @author paouelle
   *
   * @param  pkg the package where the POJOs for which to create schemas are
   *         defined
   * @return a new CREATE SCHEMAS statement
   * @throws NullPointerException if <code>pkg</code> is <code>null</code>
   * @throws IllegalArgumentException if two entities defines the same keyspace
   *         with different options or an @Entitiy annotated class doesn't represent
   *         a valid POJO class or if no entities are found
   */
  protected abstract CreateSchemas createMatchingSchemas(String pkg);

  /**
   * Starts building a new ALTER SCHEMA statement for the given POJO class.
   * This will create and/or alter all the required elements to support the
   * schema for a given POJO. It will take care of creating and/or altering the
   * required keyspace, user-defined types, tables, and indexes.
   * <p>
   * <i>Note:</i> If the POJO defines multiple tables and indexes, executing this
   * statement will potentially create and/or alter the keyspace, all tables and
   * all indexes for all tables. Since keyspace, table, and index creation and/or
   * alteration cannot be batched with Cassandra, this will result in a non-atomic
   * creation and/or alteration of everything. The process will stop at first
   * failure and will not revert back the created and/or altered keyspace, tables,
   * or indexes if any.
   *
   * @author paouelle
   *
   * @param <T> The type of POJO associated with the statement.
   *
   * @param  clazz the class of POJO associated with this statement
   * @return an in-build construct for the ALTER SCHEMA statement
   * @throws NullPointerException if <code>clazz</code> is <code>null</code>
   * @throws IllegalArgumentException if <code>clazz</code> doesn't represent a
   *         valid POJO class
   */
  protected abstract <T> AlterSchema<T> alterSchema(Class<T> clazz);

  /**
   * Starts building a new ALTER SCHEMAS statement for all POJO classes defined
   * in a given package. This will create and/or alter all the required elements
   * to support the schema for each POJO. It will take care of creating and/or
   * altering the required keyspaces, user-defined types, tables, and indexes.
   * <p>
   * <i>Note:</i> This statement will create and/or alter the schemas for all
   * POJOs for which the defined keyspace can be computed with any of the
   * specified suffixes via the WHERE clauses.
   * <p>
   * <i>Note:</i> Executing this statement will potentially create and/or alter
   * the keyspaces, all user-defined types, all tables and all indexes for all
   * tables. Since keyspace, user-defined type, table, and index creation and/or
   * alteration cannot be batched with Cassandra, this will result in a non-atomic
   * creation and/or alteration of everything. The process will stop at first
   * failure and will not revert back the created and/or altered keyspaces,
   * user-defined types, tables, or indexes if any.
   *
   * @author paouelle
   *
   * @param  pkg the package where the POJOs for which to create and/or alter
   *         schemas are defined
   * @return a new ALTER SCHEMAS statement
   * @throws NullPointerException if <code>pkg</code> is <code>null</code>
   * @throws IllegalArgumentException if two entities defines the same keyspace
   *         with different options or an entity class doesn't represent
   *         a valid POJO class or if no entities are found
   */
  protected abstract AlterSchemas alterSchemas(String pkg);

  /**
   * Starts building a new ALTER SCHEMAS statement for all POJO classes defined
   * in a given package. This will create and/or alter all the required elements
   * to support the schema for each POJO. It will take care of creating and/or
   * altering the required keyspaces, user-defined types, tables, and indexes.
   * <p>
   * <i>Note:</i> This statement will create and/or alter the schemas for only
   * the POJOs for which the defined keyspace can be computed with exactly all
   * of the specified suffixes via the WHERE clauses.
   * <p>
   * <i>Note:</i> Executing this statement will potentially create and/or alter
   * the keyspaces, all user-defined types, all tables and all indexes for all
   * tables. Since keyspace, user-defined type, table, and index creation and/or
   * alteration cannot be batched with Cassandra, this will result in a non-atomic
   * creation or alteration of everything. The process will stop at first failure
   * and will not revert back the created and/or altered keyspaces, user-defined
   * types, tables, or indexes if any.
   *
   * @author paouelle
   *
   * @param  pkg the package where the POJOs for which to create and/or alter
   *         schemas are defined
   * @return a new ALTER SCHEMAS statement
   * @throws NullPointerException if <code>pkg</code> is <code>null</code>
   * @throws IllegalArgumentException if two entities defines the same keyspace
   *         with different options or an @Entitiy annotated class doesn't represent
   *         a valid POJO class or if no entities are found
   */
  protected abstract AlterSchemas alterMatchingSchemas(String pkg);

  /**
   * Starts building a new TRUNCATE statement for the given POJO class. This
   * might actually results in multiple TRUNCATE statements.
   *
   * @author paouelle
   *
   * @param <T> The type of POJO associated with the statement.
   *
   * @param  clazz the class of POJO associated with this statement
   * @return a new TRUNCATE statement
   * @throws NullPointerException if <code>clazz</code> is <code>null</code>
   * @throws IllegalArgumentException if <code>clazz</code> doesn't represent a
   *         valid POJO class
   */
  protected abstract <T> Truncate<T> truncate(Class<T> clazz);

  /**
   * Starts building a new TRUNCATE statement for the given POJO class. This
   * might actually results in multiple TRUNCATE statements.
   *
   * @author paouelle
   *
   * @param <T> The type of POJO associated with the statement.
   *
   * @param  clazz the class of POJO associated with this statement
   * @param  tables the tables to truncate
   * @return a new TRUNCATE statement
   * @throws NullPointerException if <code>clazz</code> is <code>null</code>
   * @throws IllegalArgumentException if <code>clazz</code> doesn't represent a
   *         valid POJO class or if any of the specified tables are not defined
   *         in the POJO
   */
  protected abstract <T> Truncate<T> truncate(Class<T> clazz, String... tables);

  /**
   * Starts building a new set of statements that will execute all of them in
   * sequence one after the other.
   * <p>
   * <i>Note:</i> Executing this statement will actually execute all contained
   * statements one after the other in the given order, this will result
   * in a non-atomic execution of the statements. The process will stop
   * at first failure and will not revert back any of the previously executed
   * statements.
   * <p>
   * <i>Note:</i> This method allows one to register a recorder with the
   * sequence such as to be notified whenever a pojo-specific statement is added
   * to the sequence or any batches/sequences added to the returned sequence
   * recursively.
   *
   * @param  recorder the optional recorder to register
   * @param  statements the statements to sequence
   * @return a new {@code SequenceableStatement} that sequence {@code statements}
   * @throws NullPointerException if <code>sequence</code>, <code>statement</code>,
   *         or any of the statements are <code>null</code>
   */
  protected abstract Sequence sequence(
    Optional<Recorder> recorder, SequenceableStatement<?, ?>... statements
  );

  /**
   * Starts building a new set of statements that will execute all of them in
   * sequence one after the other.
   * <p>
   * <i>Note:</i> Executing this statement will actually execute all contained
   * statements one after the other in the given order, this will result
   * in a non-atomic execution of the statements. The process will stop
   * at first failure and will not revert back any of the previously executed
   * statements.
   * <p>
   * <i>Note:</i> This method allows one to register a recorder with the sequence
   * such as to be notified whenever a pojo-specific statement is added to the
   * sequence or any batches/sequences added to the returned sequence recursively.
   *
   * @param  recorder the optional recorder to register
   * @param  statements the statements to sequence
   * @return a new {@code SequenceableStatement} that sequence {@code statements}
   * @throws NullPointerException if <code>sequence</code>, <code>statement</code>,
   *         or any of the statements are <code>null</code>
   */
  protected abstract Sequence sequence(
    Optional<Recorder> recorder, Iterable<SequenceableStatement<?, ?>> statements
  );

  /**
   * Quotes a column name to make it case sensitive.
   *
   * @author paouelle
   *
   * @param  columnName the column name to quote.
   * @return the quoted column name.
   * @throws NullPointerException if <code>columnName</code> is <code>null</code>
   */
  protected abstract CharSequence quote(String columnName);

  /**
   * The token of a column name.
   *
   * @author paouelle
   *
   * @param  columnName the column name to take the token of.
   * @return {@code "token(" + columnName + ")"}.
   * @throws NullPointerException if <code>columnName</code> is <code>null</code>
   */
  protected abstract CharSequence token(String columnName);

  /**
   * The token of column names.
   * <p>
   * This variant is most useful when the partition key is composite.
   *
   * @author paouelle
   *
   * @param  columnNames the column names to take the token of.
   * @return a string representing the token of the provided column names.
   * @throws NullPointerException if any of the column names are <code>null</code>
   */
  protected abstract CharSequence token(String... columnNames);

  /**
   * Creates a set of "equal" where clause stating all suffix keys must be
   * equal to the POJO's used when the statement was initialized.
   *
   * @author paouelle
   *
   * @return the corresponding where clause
   */
  protected abstract Clause isSuffixedLikeObject();

  /**
   * Creates a set of "equal" where clause stating all suffix keys must be
   * equal to the provided POJO's.
   *
   * @author paouelle
   *
   * @param <T> The type of POJO.
   *
   * @param  object the POJO from which to get the suffix keys
   * @return the corresponding where clause
   * @throws NullPointerException if <code>object</code> is <code>null</code>
   */
  protected abstract <T> Clause isSuffixedLike(T object);

  /**
   * Creates a set of "equal" where clause stating all partition primary key
   * columns and suffix keys must be equal to the POJO's used when the statement
   * was initialized.
   *
   * @author paouelle
   *
   * @return the corresponding where clause
   */
  protected abstract Clause isPartitionedLikeObject();

  /**
   * Creates a set of "equal" where clause stating all partition primary key
   * columns and suffix keys must be equal to the provided POJO's.
   *
   * @author paouelle
   *
   * @param <T> The type of POJO.
   *
   * @param  object the PJO from which to get the primary and suffix keys
   * @return the corresponding where clause
   * @throws NullPointerException if <code>object</code> is <code>null</code>
   */
  protected abstract <T> Clause isPartitionedLike(T object);

  /**
   * Creates a set of "equal" where clause stating all primary key columns
   * and suffix keys must be equal to the POJO's used when the statement was
   * initialized.
   *
   * @author paouelle
   *
   * @return the corresponding where clause
   */
  protected abstract Clause isObject();

  /**
   * Creates a set of "equal" where clause stating all primary key columns
   * and suffix keys must be equal to the provided POJO's.
   *
   * @author paouelle
   *
   * @param <T> The type of POJO.
   *
   * @param  object the PJO from which to get the primary and suffix keys
   * @return the corresponding where clause
   * @throws NullPointerException if <code>object</code> is <code>null</code>
   */
  protected abstract <T> Clause is(T object);

  /**
   * Creates an "equal" where clause stating the provided column must be equal
   * to the provided value.
   * <p>
   * This clause can also be used to identify a suffix key to use as part of
   * the statement.
   *
   * @author paouelle
   *
   * @param  name the column name
   * @param  value the value
   * @return the corresponding where equality clause.
   * @throws NullPointerException if <code>columnName</code> is <code>null</code>
   */
  protected abstract Clause.Equality eq(CharSequence name, Object value);

  /**
   * Create an "in" where clause stating the provided column must be equal to
   * one of the provided values.
   * <p>
   * <i>Note:</i> Can also be used with a suffix key in order to select from
   * multiple keyspaces at the same time. In such case, the select statement
   * will actually be split into multiple select statements (one for each
   * matching keyspace) and the result will be combined as if only one select
   * statement had been executed.
   *
   * @author paouelle
   *
   * @param  name the column name
   * @param  values the values
   * @return the corresponding where clause.
   * @throws NullPointerException if <code>name</code> is <code>null</code>
   * @throws IllegalArgumentException if <code>values</code> is empty
   */
  protected abstract Clause.In in(CharSequence name, Object... values);

  /**
   * Create an "in" where clause stating the provided column must be equal to
   * one of the provided values.
   * <p>
   * <i>Note:</i> Can also be used with a suffix key in order to select from
   * multiple keyspaces at the same time. In such case, the select statement
   * will actually be split into multiple select statements (one for each
   * matching keyspace) and the result will be combined as if only one select
   * statement had been executed.
   *
   * @author paouelle
   *
   * @param  name the column name
   * @param  values the values
   * @return the corresponding where clause.
   * @throws NullPointerException if <code>name</code> is <code>null</code>
   * @throws IllegalArgumentException if <code>values</code> is empty
   */
  protected abstract Clause.In in(CharSequence name, Collection<?> values);

  /**
   * Create an "in" where clause stating the provided column must be equal to
   * one of the provided values.
   * <p>
   * <i>Note:</i> Can also be used with a suffix key in order to select from
   * multiple keyspaces at the same time. In such case, the select statement
   * will actually be split into multiple select statements (one for each
   * matching keyspace) and the result will be combined as if only one select
   * statement had been executed.
   *
   * @author paouelle
   *
   * @param  name the column name
   * @param  values the values
   * @return the corresponding where clause.
   * @throws NullPointerException if <code>name</code> is <code>null</code>
   * @throws IllegalArgumentException if <code>values</code> is empty
   */
  protected abstract Clause.In in(CharSequence name, Stream<?> values);

  /**
   * Create an "in" where clause stating the provided column must be equal to
   * a value in the provided range.
   * <p>
   * <i>Note:</i> Can also be used with a suffix key in order to select from
   * multiple keyspaces at the same time. In such case, the select statement
   * will actually be split into multiple select statements (one for each
   * matching keyspace) and the result will be combined as if only one select
   * statement had been executed.
   *
   * @author paouelle
   *
   * @param  name the column name
   * @param  from the starting value to include in the "in" clause
   * @param  to the ending value (inclusive) to include in the "in" clause
   * @return the corresponding where clause.
   * @throws NullPointerException if <code>name</code> is <code>null</code>
   * @throws IllegalArgumentException if the specified range is empty
   */
  protected abstract Clause.In in(CharSequence name, int from, int to);

  /**
   * Creates a "lesser than" where clause stating the provided column must be
   * less than the provided value.
   *
   * @author paouelle
   *
   * @param  name the column name
   * @param  value the value
   * @return the corresponding where clause.
   * @throws NullPointerException if <code>columnName</code> is <code>null</code>
   */
  protected abstract Clause lt(CharSequence name, Object value);

  /**
   * Creates a "lesser than or equal" where clause stating the provided column
   * must be lesser than or equal to the provided value.
   *
   * @author paouelle
   *
   * @param  name the column name
   * @param  value the value
   * @return the corresponding where clause.
   * @throws NullPointerException if <code>columnName</code> is <code>null</code>
   */
  protected abstract Clause lte(CharSequence name, Object value);

  /**
   * Creates a "greater than" where clause stating the provided column must be
   * greater to the provided value.
   *
   * @author paouelle
   *
   * @param  name the column name
   * @param  value the value
   * @return the corresponding where clause.
   * @throws NullPointerException if <code>columnName</code> is <code>null</code>
   */
  protected abstract Clause gt(CharSequence name, Object value);

  /**
   * Creates a "greater than or equal" where clause stating the provided column
   * must be greater than or equal to the provided value.
   *
   * @author paouelle
   *
   * @param  name the column name
   * @param  value the value
   * @return the corresponding where clause.
   * @throws NullPointerException if <code>columnName</code> is <code>null</code>
   */
  protected abstract Clause gte(CharSequence name, Object value);

  /**
   * Ascending ordering for the provided column.
   *
   * @author paouelle
   *
   * @param  columnName the column name
   * @return the corresponding ordering
   * @throws NullPointerException if <code>columnName</code> is <code>null</code>
   */
  protected abstract Ordering asc(CharSequence columnName);

  /**
   * Descending ordering for the provided column.
   *
   * @author paouelle
   *
   * @param  columnName the column name
   * @return the corresponding ordering
   * @throws NullPointerException if <code>columnName</code> is <code>null</code>
   */
  protected abstract Ordering desc(CharSequence columnName);

  /**
   * Option to set the timestamp for a modification statement (insert, update or
   * delete).
   *
   * @author paouelle
   *
   * @param  timestamp the timestamp (in microseconds) to use.
   * @return the corresponding option
   * @throws IllegalArgumentException if <code>timestamp</code> is negative
   */
  protected abstract Using timestamp(long timestamp);

  /**
   * Option to set the ttl for a modification statement (insert, update or delete).
   *
   * @author paouelle
   *
   * @param  ttl the ttl (in seconds) to use.
   * @return the corresponding option
   * @throws IllegalArgumentException if <code>ttl</code> is negative
   */
  protected abstract Using ttl(int ttl);

  /**
   * Appends the specified string as a CQL name to the given string builder.
   *
   * @author paouelle
   *
   * @param  name the name string to append as a CQL name
   * @param  sb the string builder where to append the name
   * @return <code>sb</code> for chaining
   */
  protected abstract StringBuilder appendName(String name, StringBuilder sb);

  /**
   * Simple "set" assignment of a value to a column. The value is extracted
   * directly from the POJO in play at the time the assignment is added to a
   * statement.
   * <p>
   * <code>Note:</code> This form of the <code>set()</code> cannot be used
   * with an UPDATE statement if this one was not initialized with the POJO to
   * start with.
   * <p>
   * This will generate: {@code name = value}.
   *
   * @author paouelle
   *
   * @param  name the column name
   * @return the correspond assignment (to use in an update statement)
   * @throws NullPointerException if <code>name</code> is <code>null</code>
   */
  protected abstract Assignment setFromObject(CharSequence name);

  /**
   * Simple "set" assignment of a value to a column. The value is extracted
   * directly from the POJO in play at the time the assignment is added to a
   * statement.
   * <p>
   * <code>Note:</code> This form of the <code>set()</code> cannot be used
   * with an UPDATE statement if this one was not initialized with the POJO to
   * start with.
   * <p>
   * This will generate: {@code name = value}.
   *
   * @author paouelle
   *
   * @param <T> The type of POJO.
   *
   * @param  object the POJO from which to extract all non primary key values
   * @param  name the column name
   * @return the correspond assignment (to use in an update statement)
   * @throws NullPointerException if <code>object</code> or <code>name</code>
   *         is <code>null</code>
   */
  protected abstract <T> Assignment setFrom(T object, CharSequence name);

  /**
   * Simple "set" assignment of a value to a column.
   * <p>
   * This will generate: {@code name = value}.
   *
   * @author paouelle
   *
   * @param  name the column name
   * @param  value the value to set for the column
   * @return the correspond assignment (to use in an update statement)
   * @throws NullPointerException if <code>name</code> is <code>null</code>
   */
  protected abstract Assignment set(CharSequence name, Object value);

  /**
   * Simple "set" assignment of a value to a column.
   * <p>
   * This will generate: {@code name = value}.
   * <p>
   * <i>Note:</i> This version can be useful when assigning a new value to
   * a primary key column. By specifying the old value, the statement can
   * properly generate a corresponding "delete" statement for the old record
   * using the old value before generating an "insert" statement for the new
   * record using the new primary key value.
   *
   * @author paouelle
   *
   * @param  name the column name
   * @param  value the value to set for the column
   * @param  old the old value to replace for the column
   * @return the correspond assignment (to use in an update statement)
   * @throws NullPointerException if <code>name</code> is <code>null</code>
   */
  protected abstract Assignment set(CharSequence name, Object value, Object old);

  /**
   * "set" assignment of all non primary key values to columns. The values are
   * extracted directly from the POJO in play at the time the assignment is
   * added to a statement.
   * <p>
   * <code>Note:</code> This form of the <code>setAll()</code> cannot be used
   * with an UPDATE statement if this one was not initialized with the POJO to
   * start with.
   * <p>
   * This will generate: {@code name = value} for all non primary keys.
   *
   * @author paouelle
   *
   * @return the correspond assignment (to use in an update statement)
   */
  protected abstract Assignment setAllFromObject();

  /**
   * "set" assignment of all non primary key values of the specified POJO to
   * columns.
   * <p>
   * This will generate: {@code name = value} for all non primary keys.
   *
   * @author paouelle
   *
   * @param <T> The type of POJO.
   *
   * @param  object the POJO from which to extract all non primary key values
   * @return the correspond assignment (to use in an update statement)
   * @throws NullPointerException if <code>object</code> is <code>null</code>
   */
  protected abstract <T> Assignment setAllFrom(T object);

  /**
   * Hints the statement builder to a previous value to a primary key column
   * such the statement can properly generate a corresponding "delete" statement
   * for the old record using the old value before generating an "insert" or
   * "update" statement for the new record using the new primary key value.
   * <p>
   * <i>Note:</i> The assignment returned here when added to an "update" request
   * will not count as an actual assignment which means the statement will still
   * be considered empty and continue to default to setting all columns from the
   * associated pojo
   *
   * @author paouelle
   *
   * @param  name the column name
   * @param  old the old value for the column
   * @return the correspond assignment (to use in an update statement)
   * @throws NullPointerException if <code>name</code> is <code>null</code>
   */
  protected abstract Assignment previous(CharSequence name, Object old);

  /**
   * Incrementation of a counter column by a provided value.
   * <p>
   * This will generate: {@code name = name + value}.
   *
   * @author paouelle
   *
   * @param  name the column name to increment
   * @param  value the value by which to increment
   * @return the correspond assignment (to use in an update statement)
   * @throws NullPointerException if <code>name</code> is <code>null</code>
   */
  protected abstract Assignment incr(CharSequence name, long value);

  /**
   * Decrementation of a counter column by a provided value.
   * <p>
   * This will generate: {@code name = name - value}.
   *
   * @author paouelle
   *
   * @param  name the column name to decrement
   * @param  value the value by which to decrement
   * @return the correspond assignment (to use in an update statement)
   * @throws NullPointerException if <code>name</code> is <code>null</code>
   */
  protected abstract Assignment decr(CharSequence name, long value);

  /**
   * Prepend a value to a list column.
   * <p>
   * This will generate: {@code name = [ value ] + name}.
   *
   * @author paouelle
   *
   * @param  name the column name (must be of type list).
   * @param  value the value to prepend
   * @return the correspond assignment (to use in an update statement)
   * @throws NullPointerException if <code>name</code> is <code>null</code>
   */
  protected abstract Assignment prepend(CharSequence name, Object value);

  /**
   * Prepend a list of values to a list column.
   * <p>
   * This will generate: {@code name = list + name}.
   *
   * @author paouelle
   *
   * @param  name the column name (must be of type list).
   * @param  values the list of values to prepend
   * @return the correspond assignment (to use in an update statement)
   * @throws NullPointerException if <code>name</code> or <code>values</code> is
   *         <code>null</code>
   */
  protected abstract Assignment prependAll(CharSequence name, List<?> values);

  /**
   * Append a value to a list column.
   * <p>
   * This will generate: {@code name = name + [value]}.
   *
   * @author paouelle
   *
   * @param  name the column name (must be of type list).
   * @param  value the value to append
   * @return the correspond assignment (to use in an update statement)
   * @throws NullPointerException if <code>name</code> is <code>null</code>
   */
  protected abstract Assignment append(CharSequence name, Object value);

  /**
   * Append a list of values to a list column.
   * <p>
   * This will generate: {@code name = name + list}.
   *
   * @author paouelle
   *
   * @param  name the column name (must be of type list).
   * @param  values the list of values to append
   * @return the correspond assignment (to use in an update statement)
   * @throws NullPointerException if <code>name</code> or <code>values</code> is
   *         <code>null</code>
   */
  protected abstract Assignment appendAll(CharSequence name, List<?> values);

  /**
   * Discard a value from a list column.
   * <p>
   * This will generate: {@code name = name - [value]}.
   *
   * @author paouelle
   *
   * @param  name the column name (must be of type list).
   * @param  value the value to discard
   * @return the correspond assignment (to use in an update statement)
   * @throws NullPointerException if <code>name</code> is <code>null</code>
   */
  protected abstract Assignment discard(CharSequence name, Object value);

  /**
   * Discard a list of values to a list column.
   * <p>
   * This will generate: {@code name = name - list}.
   *
   * @author paouelle
   *
   * @param  name the column name (must be of type list).
   * @param  values the list of values to append
   * @return the correspond assignment (to use in an update statement)
   * @throws NullPointerException if <code>name</code> or <code>values</code> is
   *         <code>null</code>
   */
  protected abstract Assignment discardAll(CharSequence name, List<?> values);

  /**
   * Sets a list column value by index.
   * <p>
   * This will generate: {@code name[idx] = value}.
   *
   * @author paouelle
   *
   * @param  name the column name (must be of type list).
   * @param  idx the index to set
   * @param  value the value to set
   * @return the correspond assignment (to use in an update statement)
   * @throws NullPointerException if <code>name</code> is <code>null</code>
   * @throws IndexOutOfBoundsException if <code>idx</code> is less than 0
   */
  protected abstract Assignment setIdx(CharSequence name, int idx, Object value);

  /**
   * Adds a value to a set column.
   * <p>
   * This will generate: {@code name = name + value}}.
   *
   * @author paouelle
   *
   * @param  name the column name (must be of type set).
   * @param  value the value to add
   * @return the correspond assignment (to use in an update statement)
   * @throws NullPointerException if <code>name</code> is <code>null</code>
   */
  protected abstract Assignment add(CharSequence name, Object value);

  /**
   * Adds a set of values to a set column.
   * <p>
   * This will generate: {@code name = name + set}.
   *
   * @author paouelle
   *
   * @param  name the column name (must be of type set).
   * @param  values the set of values to add
   * @return the correspond assignment (to use in an update statement)
   * @throws NullPointerException if <code>name</code> or <code>values</code> is
   *         <code>null</code>
   */
  protected abstract Assignment addAll(CharSequence name, Set<?> values);

  /**
   * Remove a value from a set column or a mapping from a map column.
   * <p>
   * This will generate: {@code name = name - value}}.
   *
   * @author paouelle
   *
   * @param  name the column name (must be of type set).
   * @param  value the value to remove from the set or the key to remove from
   *         the map
   * @return the correspond assignment (to use in an update statement)
   * @throws NullPointerException if <code>name</code> is <code>null</code>
   */
  protected abstract Assignment remove(CharSequence name, Object value);

  /**
   * Remove a set of values from a set column or a set of mappings from a map
   * column.
   * <p>
   * This will generate: {@code name = name - set}.
   *
   * @author paouelle
   *
   * @param  name the column name (must be of type set).
   * @param  values the set of values to remove from the set or the set of keys
   *         to remove from the map
   * @return the correspond assignment (to use in an update statement)
   * @throws NullPointerException if <code>name</code> or <code>values</code> is
   *         <code>null</code>
   */
  protected abstract Assignment removeAll(CharSequence name, Set<?> values);

  /**
   * Puts a new key/value pair to a map column.
   * <p>
   * This will generate: {@code name[key] = value}.
   *
   * @author paouelle
   *
   * @param  name the column name (must be of type map).
   * @param  key the key to put
   * @param  value the value to put
   * @return the correspond assignment (to use in an update statement)
   * @throws NullPointerException if <code>name</code> is <code>null</code>
   */
  protected abstract Assignment put(CharSequence name, Object key, Object value);

  /**
   * Puts a map of new key/value pairs to a map column.
   * <p>
   * This will generate: {@code name = name + map}.
   *
   * @author paouelle
   *
   * @param  name the column name (must be of type map).
   * @param  mappings the map of key/value pairs to put
   * @return the correspond assignment (to use in an update statement)
   * @throws NullPointerException if <code>name</code> or <code>mappings</code> is
   *         <code>null</code>
   */
  protected abstract Assignment putAll(CharSequence name, Map<?, ?> mappings);

  /**
   * Puts a replication set of properties together for keyspace creation. This
   * will generate: {@code REPLICATION = map}.
   *
   * @author paouelle
   *
   * @param  map the map of replication properties in json format
   * @return the corresponding option (to use in a create keyspace statement)
   * @throws NullPointerException if <code>map</code> is <code>null</code>
   */
  protected abstract KeyspaceWith replication(JsonObject map);

  /**
   * Sets the durable write option for a keyspace. This will generate:
   * {@code DURABLE_WRITES = value}.
   *
   * @author paouelle
   *
   * @param  value the keyspace durable writes option value
   * @return the corresponding option (to use in a create keyspace statement)
   */
  protected abstract KeyspaceWith durableWrites(boolean value);

  /**
   * Protects a value from any interpretation by the query builder.
   * <p>
   * The following table exemplify the behavior of this function:
   * <table border=1 summary="Behavior">
   * <tr>
   * <th>Code</th>
   * <th>Resulting query string</th>
   * </tr>
   * <tr>
   * <td>{@code select().from("t").where(eq("c", "C'est la vie!")); }</td>
   * <td>{@code "SELECT * FROM t WHERE c='C''est la vie!';"}</td>
   * </tr>
   * <tr>
   * <td>{@code select().from("t").where(eq("c", raw("C'est la vie!"))); }</td>
   * <td>{@code "SELECT * FROM t WHERE c=C'est la vie!;"}</td>
   * </tr>
   * <tr>
   * <td>{@code select().from("t").where(eq("c", raw("'C'est la vie!'"))); }</td>
   * <td>{@code "SELECT * FROM t WHERE c='C'est la vie!';"}</td>
   * </tr>
   * <tr>
   * <td>{@code select().from("t").where(eq("c", "now()")); }</td>
   * <td>{@code "SELECT * FROM t WHERE c='now()';"}</td>
   * </tr>
   * <tr>
   * <td>{@code select().from("t").where(eq("c", raw("now()"))); }</td>
   * <td>{@code "SELECT * FROM t WHERE c=now();"}</td>
   * </tr>
   * </table>
   * <i>Note: the 2nd and 3rd examples in this table are not a valid CQL3
   * queries.</i>
   * <p>
   * The use of that method is generally discouraged since it lead to security
   * risks. However, if you know what you are doing, it allows to escape the
   * interpretations done by the StatementBuilder.
   *
   * @author paouelle
   *
   * @param  str the raw value to use as a string
   * @return the value but protected from being interpreted/escaped by the query
   *         builder.
   */
  protected abstract Object raw(String str);

  /**
   * Creates a function call.
   *
   * @author paouelle
   *
   * @param  name the name of the function to call.
   * @param  parameters the parameters for the function.
   * @return the function call.
   * @throws NullPointerException if <code>name</code> is <code>null</code>
   */
  protected abstract Object fcall(String name, Object... parameters);

  /**
   * Declares that the name in argument should be treated as a column name.
   * <p>
   * This mainly meant for use with {@link Select.Selection#fcall} when a
   * function should apply to a column name, not a string value.
   *
   * @author paouelle
   *
   * @param  name the name of the column.
   * @return the name as a column name.
   * @throws NullPointerException if <code>name</code> is <code>null</code>
   */
  protected abstract Object column(String name);

  /**
   * Gets all the column names defined for a given field based on the specified
   * POJO class. The field must be annotated with {@link Column} and be part of
   * a class hierarchy annotated as an entity.
   *
   * @author paouelle
   *
   * @param <T> The type of POJO associated with the request.
   *
   * @param  clazz the non-<code>null</code> POJO class to get all the column
   *         names for
   * @param  field the non-<code>null</code> field to get all the column names
   *         for
   * @return a non-<code>null</code> set of all the column names the specified
   *         field is annotated with
   * @throws IllegalArgumentException if the field or its class are not properly
   *         annotated or the field is not in a class that is the same or a base
   *         class of the specified class
   */
  protected abstract <T> Set<String> getColumnNamesFor(Class<T> clazz, Field field);

  /**
   * Gets the Cassandra session.
   *
   * @author paouelle
   *
   * @return the non-<code>null</code> Cassandra session
   */
  public abstract Session getSession();

  /**
   * Gets the Cassandra cluster.
   *
   * @author paouelle
   *
   * @return the non-<code>null</code> Cassandra cluster
   */
  public abstract Cluster getCluster();
}

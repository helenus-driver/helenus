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
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.json.JsonObject;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import org.helenus.driver.info.ClassInfo;
import org.helenus.driver.info.TableInfo;
import org.helenus.driver.persistence.Column;

/**
 * The <code>StatementBuilder</code> class extends the functionality of Cassandra's
 * {@link com.datastax.driver.core.querybuilder.QueryBuilder} class in order to
 * provide support for POJOs.
 * <p>
 * The Helenus driver will take advantage of the {@link Thread#getContextClassLoader}
 * method to retrieve a class loader to use when it needs to find classes. If not
 * available, it will fallback to using {@link Class#forName(String)} directly.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 15, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
public final class StatementBuilder {
  /**
   * Prevents instantiation
   *
   * @author paouelle
   */
  private StatementBuilder() {}

  /**
   * Gets a stream of the remaining rows for the specified result set.
   *
   * @author paouelle
   *
   * @param  set the result set for which to get a stream of rows
   * @return a stream of all remaining rows in the specified result set
   * @throws NullPointerException if <code>set</code> is <code>null</code>
   */
  public static Stream<Row> stream(ResultSet set) {
    return StreamSupport.stream(Spliterators.spliteratorUnknownSize(
      set.iterator(),
      Spliterator.ORDERED | Spliterator.IMMUTABLE | Spliterator.NONNULL
    ), false);
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
  public static <T> ClassInfo<T> getClassInfo(Class<T> clazz) {
    org.apache.commons.lang3.Validate.notNull(clazz, "invalid null POJO class");
    return StatementManager.getManager().getClassInfo(clazz);
  }

  /**
   * Gets a root class info structure that defines the specified POJO class. The
   * standard class info is returned if the specified class does not represent
   * a type entity.
   *
   * @author paouelle
   *
   * @param <T> The type of POJO associated with this statement
   *
   * @param  clazz the class of POJO for which to get a class info object for
   * @return the non-<code>null</code> class info object representing the given
   *         POJO class or its root class info if <Code>clazz</code> represents
   *         a type entity
   * @throws NullPointerException if <code>clazz</code> is <code>null</code>
   * @throws IllegalArgumentException if <code>clazz</code> doesn't represent
   *         a valid POJO class
   */
  public static <T> ClassInfo<? super T> getRootClassInfo(Class<T> clazz) {
    org.apache.commons.lang3.Validate.notNull(clazz, "invalid null POJO class");
    return StatementManager.getManager().getRootClassInfo(clazz);
  }

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
  public static <T> Select.Builder<T> select(
    Class<T> clazz, CharSequence... columns
  ) {
    return StatementManager.getManager().select(clazz, columns);
  }

  /**
   * Starts building a new SELECT statement.
   *
   * @author paouelle
   *
   * @param <T> The type of POJO associated with the statement
   *
   * @param  clazz the class of POJO associated with this statement
   * @return an in-construction SELECT statement (you will need to provide a column
   *         selection and at least a FROM clause to complete the statement).
   * @throws NullPointerException if <code>clazz</code> is <code>null</code>
   * @throws IllegalArgumentException if <code>clazz</code> doesn't represent a
   *         valid POJO class
   */
  public static <T> Select.Selection<T> select(Class<T> clazz) {
    return StatementManager.getManager().select(clazz);
  }

  /**
   * Start building a new SELECT statement that selects the provided names.
   * <p>
   * <i>Note:</i> that {@code selectFrom(table, c1, c2)} is just a shortcut for
   * {@code selectFrom(table).column(c1).column(c2)}.
   *
   * @author paouelle
   *
   * @param <T> The type of POJO associated with the statement
   *
   * @param  table the POJO table associated with this statement to select from
   * @param  columns the columns names that should be selected by the statement
   * @return the SELECT statement
   * @throws NullPointerException if <code>table</code> is <code>null</code>
   * @throws IllegalArgumentException if <code>table</code> doesn't represent a
   *         valid POJO class or any of the specified columns are not defined
   *         by the POJO
   */
  public static <T> Select<T> selectFrom(
    TableInfo<T> table, CharSequence... columns
  ) {
    return StatementManager.getManager().selectFrom(table, columns);
  }

  /**
   * Starts building a new SELECT statement.
   *
   * @author paouelle
   *
   * @param <T> The type of POJO associated with the statement
   *
   * @param  table the POJO table associated with this statement to select from
   * @return an in-construction SELECT statement (you will need to provide a column
   *         selection).
   * @throws NullPointerException if <code>table</code> is <code>null</code>
   * @throws IllegalArgumentException if <code>table</code> doesn't represent a
   *         valid POJO class
   */
  public static <T> Select.TableSelection<T> selectFrom(TableInfo<T> table) {
    return StatementManager.getManager().selectFrom(table);
  }

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
  public static <T> Insert.Builder<T> insert(T object) {
    return StatementManager.getManager().insert(object);
  }

  /**
   * Starts building a new UPDATE statement for the following POJO object to update
   * all the required tables defined by the POJO based on the assignment clauses
   * specified.
   * <p>
   * <i>Note:</i> The primary key columns are automatically added to the
   * "WHERE" part of the UPDATE statement if no clauses are added to the "WHERE"
   * part. In addition, if no assignments are specified via the "WITH" part of
   * the UPDATE statement, all non-primary columns will be automatically added.
   * <p>
   * If it is discovered that a primary key is part of the set of assignments,
   * the "update" will be translated into a full "insert" of the POJO object
   * without regards to the assignments specified with the "update. This is to
   * ensure that the whole POJO is persisted to the DB into a new row. If a
   * primary key is assigned using the {@link #set(CharSequence, Object, Object)}
   * then a "delete" statement will also be generated for each tables affected
   * by the primary key column before a full "insert" is generated.
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
  public static <T> Update<T> update(T object) {
    org.apache.commons.lang3.Validate.notNull(object, "invalid null object");
    return StatementManager.getManager().update(object);
  }

  /**
   * Starts building a new UPDATE statement for the following POJO object to update
   * all the specified tables defined by the POJO based on the assignment clauses
   * specified.
   * <p>
   * <i>Note:</i> The primary key columns are automatically added to the
   * "WHERE" part of the UPDATE statement if no clauses are added to the "WHERE"
   * part. In addition, if no assignments are specified via the "WITH" part of
   * the UPDATE statement, all non-primary columns will be automatically added.
   * <p>
   * If it is discovered that a primary key is part of the set of assignments,
   * the "update" will be translated into a full "insert" of the POJO object
   * without regards to the assignments specified with the "update. This is to
   * ensure that the whole POJO is persisted to the DB into a new row. If a
   * primary key is assigned using the {@link #set(CharSequence, Object, Object)}
   * then a "delete" statement will also be generated for each tables affected
   * by the primary key column before a full "insert" is generated.
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
  public static <T> Update<T> update(T object, String... tables) {
    return StatementManager.getManager().update(object, tables);
  }

  /**
   * Starts building a new UPDATE statement for the following POJO object to update
   * all the specified tables defined by the POJO based on the assignment clauses
   * specified.
   * <p>
   * <i>Note:</i> The primary key columns are automatically added to the
   * "WHERE" part of the UPDATE statement if no clauses are added to the "WHERE"
   * part. In addition, if no assignments are specified via the "WITH" part of
   * the UPDATE statement, all non-primary columns will be automatically added.
   * <p>
   * If it is discovered that a primary key is part of the set of assignments,
   * the "update" will be translated into a full "insert" of the POJO object
   * without regards to the assignments specified with the "update. This is to
   * ensure that the whole POJO is persisted to the DB into a new row. If a
   * primary key is assigned using the {@link #set(CharSequence, Object, Object)}
   * then a "delete" statement will also be generated for each tables affected
   * by the primary key column before a full "insert" is generated.
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
  public static <T> Update<T> update(T object, Stream<String> tables) {
    return StatementManager.getManager().update(object, tables.toArray(String[]::new));
  }

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
  public static <T> Delete.Builder<T> delete(T object, String... columns) {
    return StatementManager.getManager().delete(object, columns);
  }

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
  public static <T> Delete.Selection<T> delete(T object) {
    return StatementManager.getManager().delete(object);
  }

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
  public static <T> Delete.Builder<T> delete(Class<T> clazz, String... columns) {
    return StatementManager.getManager().delete(clazz, columns);
  }

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
  public static <T> Delete.Selection<T> delete(Class<T> clazz) {
    return StatementManager.getManager().delete(clazz);
  }

  /**
   * Starts building a new BATCH statement on the provided statements.
   * <p>
   * This method will build a logged batch (this is the default in CQL3). To
   * create unlogged batches, use {@link #unloggedBatch}. Also note that
   * for convenience, if the provided statements are counter statements, this
   * method will create a COUNTER batch even though COUNTER batches are never
   * logged (so for counters, using this method is effectively equivalent to
   * using {@link #unloggedBatch}).
   *
   * @param  statements the statements to batch
   * @return a new {@code BatchableStatement} that batch {@code statements}
   * @throws NullPointerException if <code>statement</code> or any of the
   *         statements are <code>null</code>
   * @throws IllegalArgumentException if counter and non-counter operations
   *         are mixed
   */
  public static Batch batch(BatchableStatement<?, ?>... statements) {
    return StatementManager.getManager().batch(Optional.empty(), statements);
  }

  /**
   * Starts building a new BATCH statement on the provided statements.
   * <p>
   * This method will build a logged batch (this is the default in CQL3). To
   * create unlogged batches, use {@link #unloggedBatch}. Also note that
   * for convenience, if the provided statements are counter statements, this
   * method will create a COUNTER batch even though COUNTER batches are never
   * logged (so for counters, using this method is effectively equivalent to
   * using {@link #unloggedBatch}).
   *
   * @param  statements the statements to batch
   * @return a new {@code BatchableStatement} that batch {@code statements}
   * @throws NullPointerException if <code>statement</code> or any of the
   *         statements are <code>null</code>
   * @throws IllegalArgumentException if counter and non-counter operations
   *         are mixed
   */
  public static Batch batch(Iterable<BatchableStatement<?, ?>> statements) {
    return StatementManager.getManager().batch(Optional.empty(), statements);
  }

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
   * <i>Note:</i> This version of the <code>batch()</code> method allows one to
   * register a recorder with the batch such as to be notified whenever a
   * pojo-specific statement is added to the batch or any batches added to the
   * returned batch recursively.
   *
   * @param  recorder the recorder to register
   * @param  statements the statements to batch
   * @return a new {@code Batch} that batch {@code statements}
   * @throws NullPointerException if <code>recorder</code>, <code>statement</code>,
   *         or any of the statements are <code>null</code>
   * @throws IllegalArgumentException if counter and non-counter operations
   *         are mixed
   */
  public static Batch batch(
    Recorder recorder, BatchableStatement<?, ?>... statements
  ) {
    return StatementManager.getManager().batch(Optional.of(recorder), statements);
  }

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
   * <i>Note:</i> This version of the <code>batch()</code> method allows one to
   * register a recorder with the batch such as to be notified whenever a
   * pojo-specific statement is added to the batch or any batches added to the
   * returned batch recursively.
   *
   * @param  recorder the recorder to register
   * @param  statements the statements to batch
   * @return a new {@code Batch} that batch {@code statements}
   * @throws NullPointerException if <code>recorder</code>, <code>statement</code>,
   *         or any of the statements are <code>null</code>
   * @throws IllegalArgumentException if counter and non-counter operations
   *         are mixed
   */
  public static Batch batch(
    Recorder recorder, Iterable<BatchableStatement<?, ?>> statements
  ) {
    return StatementManager.getManager().batch(Optional.of(recorder), statements);
  }

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
   *
   * @param  statements the statements to batch
   * @return a new {@code Batch} that batch {@code statements} without using the
   *         batch log
   * @throws NullPointerException if <code>statement</code> or any of the
   *         statements are <code>null</code>
   * @throws IllegalArgumentException if counter and non-counter operations
   *         are mixed
   */
  public static Batch unloggedBatch(BatchableStatement<?, ?>... statements) {
    return StatementManager.getManager().unloggedBatch(Optional.empty(), statements);
  }

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
   *
   * @param  statements the statements to batch
   * @return a new {@code Batch} that batch {@code statements} without using the
   *         batch log
   * @throws NullPointerException if <code>statement</code> or any of the
   *         statements are <code>null</code>
   * @throws IllegalArgumentException if counter and non-counter operations
   *         are mixed
   */
  public static Batch unloggedBatch(Iterable<BatchableStatement<?, ?>> statements) {
    return StatementManager.getManager().unloggedBatch(Optional.empty(), statements);
  }

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
   * <i>Note:</i> This version of the <code>unloggedBatch()</code> method allows
   * one to register a recorder with the batch such as to be notified whenever a
   * pojo-specific statement is added to the batch or any batches added to the
   * returned batch recursively.
   *
   * @param  recorder the recorder to register
   * @param  statements the statements to batch
   * @return a new {@code Batch} that batch {@code statements} without using the
   *         batch log
   * @throws NullPointerException if <code>recorder</code>, <code>statement</code>,
   *         or any of the statements are <code>null</code>
   * @throws IllegalArgumentException if counter and non-counter operations
   *         are mixed
   */
  public static Batch unloggedBatch(
    Recorder recorder, BatchableStatement<?, ?>... statements
  ) {
    return StatementManager.getManager().unloggedBatch(Optional.of(recorder), statements);
  }

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
   * <i>Note:</i> This version of the <code>unloggedBatch()</code> method allows
   * one to register a recorder with the batch such as to be notified whenever a
   * pojo-specific statement is added to the batch or any batches added to the
   * returned batch recursively.
   *
   * @param  recorder the recorder to register
   * @param  statements the statements to batch
   * @return a new {@code Batch} that batch {@code statements} without using the
   *         batch log
   * @throws NullPointerException if <code>recorder</code>, <code>statement</code>,
   *         or any of the statements are <code>null</code>
   * @throws IllegalArgumentException if counter and non-counter operations
   *         are mixed or if any statement represents a "select" statement or a
   *         "batch" statement
   */
  public static Batch unloggedBatch(
    Recorder recorder, Iterable<BatchableStatement<?, ?>> statements
  ) {
    return StatementManager.getManager().unloggedBatch(Optional.of(recorder), statements);
  }

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
  public static RegularStatement regular(
    com.datastax.driver.core.RegularStatement statement
  ) {
    return StatementManager.getManager().regular(statement);
  }

  /**
   * Starts building a new CREATE KEYSPACE statement for the given POJO class.
   * <p>
   * <i>Note:</i> By default, it will create the keyspace defined as part of
   * the POJO along with the options set in the POJO @keyspace annotation.
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
  public static <T> CreateKeyspace<T> createKeyspace(Class<T> clazz) {
    return StatementManager.getManager().createKeyspace(clazz);
  }

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
  public static <T> CreateType<T> createType(Class<T> clazz) {
    return StatementManager.getManager().createType(clazz);
  }

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
  public static <T> CreateTable<T> createTable(Class<T> clazz) {
    return StatementManager.getManager().createTable(clazz);
  }

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
  public static <T> CreateTable<T> createTable(Class<T> clazz, String... tables) {
    return StatementManager.getManager().createTable(clazz, tables);
  }

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
  public static <T> CreateIndex.Builder<T> createIndex(Class<T> clazz) {
    return StatementManager.getManager().createIndex(clazz);
  }

  /**
   * Starts building a new CREATE SCHEMA statement for the given POJO class.
   * This will create all the required elements to support the schema for a given
   * POJO. It will take care of creating the required keyspace, user-defined types,
   * tables, and indexes.
   * <p>
   * <i>Note:</i> If the POJO defines multiple tables and indexes, executing this
   * statement will potentially create the keyspace, all tables and all indexes
   * for all tables. Since keyspace, table, and index creation cannot be batched
   * with Cassandra, this will result in a non-atomic creation of everything.
   * The process will stop at first failure and will not revert back the created
   * keyspace, tables, or indexes if any.
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
  public static <T> CreateSchema<T> createSchema(Class<T> clazz) {
    return StatementManager.getManager().createSchema(clazz);
  }

  /**
   * Starts building a new CREATE SCHEMAS statement for all POJO classes defined
   * in the given packages and/or classes. This will create all the required
   * elements to support the schema for each POJO. It will take care of creating
   * the required keyspaces, user-defined types, tables, and indexes.
   * <p>
   * <i>Note:</i> This statement will create the schemas for all POJOs for which
   * the defined keyspace can be computed with any of the specified suffixes via
   * the WHERE clauses.
   * <p>
   * <i>Note:</i> Executing this statement will potentially create the keyspaces,
   * all user-defined types, all tables and all indexes for all tables. Since
   * keyspace, user-defined type, table, and index creation cannot be batched
   * with Cassandra, this will result in a non-atomic creation of everything.
   * The process will stop at first failure and will not revert back the created
   * keyspaces, user-defined types, tables, or indexes if any.
   *
   * @author paouelle
   *
   * @param  pkgs the packages and/or classes where the POJOs for which to create
   *         schemas are defined
   * @return a new CREATE SCHEMAS statement
   * @throws NullPointerException if <code>pkgs</code> is <code>null</code>
   * @throws IllegalArgumentException if two entities defines the same keyspace
   *         with different options or an entity class doesn't represent
   *         a valid POJO class or if no entities are found
   */
  public static CreateSchemas createSchemas(String... pkgs) {
    return StatementManager.getManager().createSchemas(pkgs);
  }

  /**
   * Starts building a new CREATE SCHEMAS statement for all POJO classes defined
   * in the given packages and/or classes. This will create all the required
   * elements to support the schema for each POJO. It will take care of creating
   * the required keyspaces, user-defined types, tables, and indexes.
   * <p>
   * <i>Note:</i> This statement will create the schemas for only the POJOs for
   * which the defined keyspace can be computed with exactly all of the specified
   * suffixes via the WHERE clauses.
   * <p>
   * <i>Note:</i> Executing this statement will potentially create the keyspaces,
   * all user-defined types, all tables and all indexes for all tables. Since
   * keyspace, user-defined type, table, and index creation cannot be batched
   * with Cassandra, this will result in a non-atomic creation of everything.
   * The process will stop at first failure and will not revert back the created
   * keyspaces, user-defined types, tables, or indexes if any.
   *
   * @author paouelle
   *
   * @param  pkgs the packages and/or classes where the POJOs for which to create
   *         schemas are defined
   * @return a new CREATE SCHEMAS statement
   * @throws NullPointerException if <code>pkgs</code> is <code>null</code>
   * @throws IllegalArgumentException if two entities defines the same keyspace
   *         with different options or an @Entitiy annotated class doesn't represent
   *         a valid POJO class or if no entities are found
   */
  public static CreateSchemas createMatchingSchemas(String... pkgs) {
    return StatementManager.getManager().createMatchingSchemas(pkgs);
  }

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
  public static <T> AlterSchema<T> alterSchema(Class<T> clazz) {
    return StatementManager.getManager().alterSchema(clazz);
  }

  /**
   * Starts building a new ALTER SCHEMAS statement for all POJO classes defined
   * in the given packages and/or classes. This will create and/or alter all the
   * required elements to support the schema for each POJO. It will take care of
   * creating and/or altering the required keyspaces, user-defined types, tables,
   * and indexes.
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
   * @param  pkgs the packages and/or classes where the POJOs for which to create
   *         and/or alter schemas are defined
   * @return a new ALTER SCHEMAS statement
   * @throws NullPointerException if <code>pkgs</code> is <code>null</code>
   * @throws IllegalArgumentException if two entities defines the same keyspace
   *         with different options or an entity class doesn't represent
   *         a valid POJO class or if no entities are found
   */
  public static AlterSchemas alterSchemas(String... pkgs) {
    return StatementManager.getManager().alterSchemas(pkgs);
  }

  /**
   * Starts building a new ALTER SCHEMAS statement for all POJO classes defined
   * in the given packages and/or classes. This will create and/or alter all the
   * required elements to support the schema for each POJO. It will take care of
   * creating and/or altering the required keyspaces, user-defined types, tables,
   * and indexes.
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
   * @param  pkgs the packages and/or classes where the POJOs for which to create
   *         and/or alter schemas are defined
   * @return a new ALTER SCHEMAS statement
   * @throws NullPointerException if <code>pkgs</code> is <code>null</code>
   * @throws IllegalArgumentException if two entities defines the same keyspace
   *         with different options or an @Entitiy annotated class doesn't represent
   *         a valid POJO class or if no entities are found
   */
  public static AlterSchemas alterMatchingSchemas(String... pkgs) {
    return StatementManager.getManager().alterMatchingSchemas(pkgs);
  }

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
  public static <T> Truncate<T> truncate(Class<T> clazz) {
    return StatementManager.getManager().truncate(clazz);
  }

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
  public static <T> Truncate<T> truncate(Class<T> clazz, String... tables) {
    return StatementManager.getManager().truncate(clazz, tables);
  }

  /**
   * Starts building a new set of statements that will execute all of them in
   * sequence one after the other.
   * <p>
   * <i>Note:</i> Executing this statement will actually execute all contained
   * statements one after the other in the given order, this will result
   * in a non-atomic execution of the statements. The process will stop
   * at first failure and will not revert back any of the previously executed
   * statements.
   *
   * @param  statements the statements to sequence
   * @return a new {@code Sequence} that sequence {@code statements}
   * @throws NullPointerException if <code>statement</code> or any of the
   *         statements are <code>null</code>
   */
  public static Sequence sequence(SequenceableStatement<?, ?>... statements) {
    return StatementManager.getManager().sequence(Optional.empty(), statements);
  }

  /**
   * Starts building a new set of statements that will execute all of them in
   * sequence one after the other.
   * <p>
   * <i>Note:</i> Executing this statement will actually execute all contained
   * statements one after the other in the given order, this will result
   * in a non-atomic execution of the statements. The process will stop
   * at first failure and will not revert back any of the previously executed
   * statements.
   *
   * @param  statements the statements to sequence
   * @return a new {@code Sequence} that sequence {@code statements}
   * @throws NullPointerException if <code>statement</code> or any of the
   *         statements are <code>null</code>
   */
  public static Sequence sequence(Iterable<SequenceableStatement<?, ?>> statements) {
    return StatementManager.getManager().sequence(Optional.empty(), statements);
  }

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
   * <i>Note:</i> This version of the <code>sequence()</code> method allows one to
   * register a recorder with the sequence such as to be notified whenever a
   * pojo-specific statement is added to the sequence or any batches/sequences
   * added to the returned sequence recursively.
   *
   * @param  recorder the recorder to register
   * @param  statements the statements to sequence
   * @return a new {@code Sequence} that sequence {@code statements}
   * @throws NullPointerException if <code>recorder</code>, <code>statement</code>,
   *         or any of the statements are <code>null</code>
   */
  public static Sequence sequence(
    Recorder recorder, SequenceableStatement<?, ?>... statements
  ) {
    return StatementManager.getManager().sequence(Optional.of(recorder), statements);
  }

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
   * <i>Note:</i> This version of the <code>sequence()</code> method allows one to
   * register a recorder with the sequence such as to be notified whenever a
   * pojo-specific statement is added to the sequence or any batches/sequences
   * added to the returned sequence recursively.
   *
   * @param  recorder the recorder to register
   * @param  statements the statements to sequence
   * @return a new {@code Sequence} that sequence {@code statements}
   * @throws NullPointerException if <code>recorder</code>, <code>statement</code>,
   *         or any of the statements are <code>null</code>
   */
  public static Sequence sequence(
    Recorder recorder, Iterable<SequenceableStatement<?, ?>> statements
  ) {
    return StatementManager.getManager().sequence(Optional.of(recorder), statements);
  }

  /**
   * Starts building a new group of statements that will execute them in parallel
   * subsets where each subsets will be executed one after the other.
   * <p>
   * <i>Note:</i> Executing this statement will attempt to group together
   * statements in the given order up to either the parallel factor or until
   * a sequence statement is reached. It will then wait for all statements to
   * in this subset to have completed before proceeding. This will result in a
   * non-atomic execution of the statements. The process will stop at first
   * failure encountered will executing a subset of statements and will not
   * revert back any of the previously executed statements.
   *
   * @param  statements the statements to group
   * @return a new {@code Group} that groups {@code statements}
   * @throws NullPointerException if <code>statement</code> or any of the
   *         statements are <code>null</code>
   */
  @SafeVarargs
  public static Group group(GroupableStatement<?, ?>... statements) {
    return StatementManager.getManager().group(Optional.empty(), statements);
  }

  /**
   * Starts building a new group of statements that will execute them in parallel
   * subsets where each subsets will be executed one after the other.
   * <p>
   * <i>Note:</i> Executing this statement will attempt to group together
   * statements in the given order up to either the parallel factor or until
   * a sequence statement is reached. It will then wait for all statements to
   * in this subset to have completed before proceeding. This will result in a
   * non-atomic execution of the statements. The process will stop at first
   * failure encountered will executing a subset of statements and will not
   * revert back any of the previously executed statements.
   *
   * @param  statements the statements to group
   * @return a new {@code Group} that groups {@code statements}
   * @throws NullPointerException if <code>statement</code> or any of the
   *         statements are <code>null</code>
   */
  public static Group group(Iterable<GroupableStatement<?, ?>> statements) {
    return StatementManager.getManager().group(Optional.empty(), statements);
  }

  /**
   * Starts building a new group of statements that will execute them in parallel
   * subsets where each subsets will be executed one after the other.
   * <p>
   * <i>Note:</i> Executing this statement will attempt to group together
   * statements in the given order up to either the parallel factor or until
   * a sequence statement is reached. It will then wait for all statements to
   * in this subset to have completed before proceeding. This will result in a
   * non-atomic execution of the statements. The process will stop at first
   * failure encountered will executing a subset of statements and will not
   * revert back any of the previously executed statements.
   * <p>
   * <i>Note:</i> This version of the <code>group()</code> method allows one to
   * register a recorder with the group such as to be notified whenever a
   * pojo-specific statement is added to the group or any batches/sequences/groups
   * added to the returned group recursively.
   *
   * @param  recorder the recorder to register
   * @param  statements the statements to group
   * @return a new {@code Group} that groups {@code statements}
   * @throws NullPointerException if <code>recorder</code>, <code>statement</code>,
   *         or any of the statements are <code>null</code>
   */
  @SafeVarargs
  public static Group group(Recorder recorder, GroupableStatement<?, ?>... statements) {
    return StatementManager.getManager().group(Optional.of(recorder), statements);
  }

  /**
   * Starts building a new group of statements that will execute them in parallel
   * subsets where each subsets will be executed one after the other.
   * <p>
   * <i>Note:</i> Executing this statement will attempt to group together
   * statements in the given order up to either the parallel factor or until
   * a sequence statement is reached. It will then wait for all statements to
   * in this subset to have completed before proceeding. This will result in a
   * non-atomic execution of the statements. The process will stop at first
   * failure encountered will executing a subset of statements and will not
   * revert back any of the previously executed statements.
   * <p>
   * <i>Note:</i> This version of the <code>group()</code> method allows one to
   * register a recorder with the group such as to be notified whenever a
   * pojo-specific statement is added to the group or any batches/sequences/groups
   * added to the returned group recursively.
   *
   * @param  recorder the recorder to register
   * @param  statements the statements to group
   * @return a new {@code Group} that groups {@code statements}
   * @throws NullPointerException if <code>recorder</code>, <code>statement</code>,
   *         or any of the statements are <code>null</code>
   */
  public static Group group(Recorder recorder, Iterable<GroupableStatement<?, ?>> statements) {
    return StatementManager.getManager().group(Optional.of(recorder), statements);
  }

  /**
   * Quotes a column name to make it case sensitive.
   *
   * @author paouelle
   *
   * @param  columnName the column name to quote.
   * @return the quoted column name.
   * @throws NullPointerException if <code>columnName</code> is <code>null</code>
   */
  public static CharSequence quote(String columnName) {
    return StatementManager.getManager().quote(columnName);
  }

  /**
   * The token of a column name.
   *
   * @author paouelle
   *
   * @param  columnName the column name to take the token of.
   * @return {@code "token(" + columnName + ")"}.
   * @throws NullPointerException if <code>columnName</code> is <code>null</code>
   */
  public static CharSequence token(String columnName) {
    return StatementManager.getManager().token(columnName);
  }

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
  public static CharSequence token(String... columnNames) {
    return StatementManager.getManager().token(columnNames);
  }

  /**
   * Creates a set of "equal" where clause stating all suffix keys must be
   * equal to the POJO's used when the statement was initialized.
   *
   * @author paouelle
   *
   * @return the corresponding where clause
   */
  public static Clause isSuffixedLikeObject() {
    return StatementManager.getManager().isSuffixedLikeObject();
  }

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
  public static <T> Clause isSuffixedLike(T object) {
    return StatementManager.getManager().isSuffixedLike(object);
  }

  /**
   * Creates a set of "equal" where clause stating all partition primary key
   * columns and suffix keys must be equal to the POJO's used when the statement
   * was initialized.
   *
   * @author paouelle
   *
   * @return the corresponding where clause
   */
  public static Clause isPartitionedLikeObject() {
    return StatementManager.getManager().isPartitionedLikeObject();
  }

  /**
   * Creates a set of "equal" where clause stating all partition primary key
   * columns and suffix keys must be equal to the provided POJO's.
   *
   * @author paouelle
   *
   * @param <T> The type of POJO.
   *
   * @param  object the POJO from which to get the primary and suffix keys
   * @return the corresponding where clause
   * @throws NullPointerException if <code>object</code> is <code>null</code>
   */
  public static <T> Clause isPartitionedLike(T object) {
    return StatementManager.getManager().isPartitionedLike(object);
  }

  /**
   * Creates a set of "equal" where clause stating all primary key columns
   * and suffix keys must be equal to the POJO's used when the statement was
   * initialized.
   *
   * @author paouelle
   *
   * @return the corresponding where clause
   */
  public static Clause isObject() {
    return StatementManager.getManager().isObject();
  }

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
  public static <T> Clause is(T object) {
    return StatementManager.getManager().is(object);
  }

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
  public static Clause.Equality eq(CharSequence name, Object value) {
    return StatementManager.getManager().eq(name, value);
  }

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
  public static Clause.In in(CharSequence name, Object... values) {
    return StatementManager.getManager().in(name, values);
  }

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
  public static Clause.In in(CharSequence name, Collection<?> values) {
    return StatementManager.getManager().in(name, values);
  }

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
  public static Clause.In in(CharSequence name, Stream<?> values) {
    return StatementManager.getManager().in(name, values);
  }

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
  public static Clause.In in(CharSequence name, int from, int to) {
    return StatementManager.getManager().in(name, from, to);
  }

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
  public static Clause lt(CharSequence name, Object value) {
    return StatementManager.getManager().lt(name, value);
  }

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
  public static Clause lte(CharSequence name, Object value) {
    return StatementManager.getManager().lte(name, value);
  }

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
  public static Clause gt(CharSequence name, Object value) {
    return StatementManager.getManager().gt(name, value);
  }

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
  public static Clause gte(CharSequence name, Object value) {
    return StatementManager.getManager().gte(name, value);
  }

  /**
   * Ascending ordering for the provided column.
   *
   * @author paouelle
   *
   * @param  columnName the column name
   * @return the corresponding ordering
   * @throws NullPointerException if <code>columnName</code> is <code>null</code>
   */
  public static Ordering asc(CharSequence columnName) {
    return StatementManager.getManager().asc(columnName);
  }

  /**
   * Descending ordering for the provided column.
   *
   * @author paouelle
   *
   * @param  columnName the column name
   * @return the corresponding ordering
   * @throws NullPointerException if <code>columnName</code> is <code>null</code>
   */
  public static Ordering desc(CharSequence columnName) {
    return StatementManager.getManager().desc(columnName);
  }

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
  public static Using<Long> timestamp(long timestamp) {
    return StatementManager.getManager().timestamp(timestamp);
  }

  /**
   * Option to set the timestamp for a modification statement (insert, update or
   * delete).
   *
   * @author paouelle
   *
   * @param  timestamp the timestamp (in milliseconds) to use.
   * @return the corresponding option
   * @throws IllegalArgumentException if <code>timestamp</code> is negative
   */
  public static Using<Long> timestampInMS(long timestamp) {
    return StatementManager.getManager().timestamp(timestamp * 1000L);
  }

  /**
   * Option to set the ttl for a modification statement (insert, update or delete).
   *
   * @author paouelle
   *
   * @param  ttl the ttl (in seconds) to use.
   * @return the corresponding option
   * @throws IllegalArgumentException if <code>ttl</code> is negative
   */
  public static Using<Integer> ttl(int ttl) {
    return StatementManager.getManager().ttl(ttl);
  }

  /**
   * An object representing a bind marker (a question mark).
   * <p>
   * This can be used wherever a value is expected. For instance, one can do:
   *
   * <pre>
   * {@code
   *   Insert i = StatementBuilder.insertInto("test").value("k", 0)
   *                                                 .value("c", StatementBuilder.bindMarker());
   *   PreparedState p = session.prepare(i.toString());
   * }
   * </pre>
   *
   * @author paouelle
   *
   * @return an object representing a bind marker.
   */
  public static BindMarker bindMarker() {
    return BindMarker.ANONYMOUS;
  }

  /**
   * An object representing a named bind marker.
   * <p>
   * This can be used wherever a value is expected. For instance, one can do:
   * <pre>
   * {@code
   *     Insert i = StatementBuilder.insertInto("test").value("k", 0)
   *                                                   .value("c", StatementBuilder.bindMarker("c_val"));
   *     PreparedState p = session.prepare(i.toString());
   * }
   * </pre>
   * <p>
   * Please note that named bind makers are only supported starting with Cassandra 2.0.1.
   *
   * @author paouelle
   *
   * @param  name the name for the bind marker
   * @return an object representing a bind marker named {@code name}.
   */
  public static BindMarker bindMarker(String name) {
    return new BindMarker(name);
  }

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
  public static Assignment setFromObject(CharSequence name) {
    return StatementManager.getManager().setFromObject(name);
  }

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
  public static <T> Assignment setFrom(T object, CharSequence name) {
    return StatementManager.getManager().setFrom(object, name);
  }

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
  public static Assignment set(CharSequence name, Object value) {
    return StatementManager.getManager().set(name, value);
  }

  /**
   * Simple "set" assignment of a value to a column.
   * <p>
   * This will generate: {@code name = value}.
   * <p>
   * <i>Note:</i> This version can be useful when assigning a new value to
   * a primary key column. By specifying the old value, the statement can
   * properly generate a corresponding "delete" statement for the old record
   * using the old value before generating an "insert" or "delete" statement
   * for the new record using the new primary key value.
   *
   * @author paouelle
   *
   * @param  name the column name
   * @param  value the value to set for the column
   * @param  old the old value to replace for the column
   * @return the correspond assignment (to use in an update statement)
   * @throws NullPointerException if <code>name</code> is <code>null</code>
   */
  public static Assignment set(CharSequence name, Object value, Object old) {
    return StatementManager.getManager().set(name, value, old);
  }

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
  public static Assignment setAllFromObject() {
    return StatementManager.getManager().setAllFromObject();
  }

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
  public static <T> Assignment setAllFrom(T object) {
    return StatementManager.getManager().setAllFrom(object);
  }

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
  public static Assignment previous(CharSequence name, Object old) {
    return StatementManager.getManager().previous(name, old);
  }

  /**
   * Incrementation of a counter column.
   * <p>
   * This will generate: {@code name = name + 1}.
   *
   * @author paouelle
   *
   * @param  name the column name to increment
   * @return the correspond assignment (to use in an update statement)
   * @throws NullPointerException if <code>name</code> is <code>null</code>
   */
  public static Assignment incr(CharSequence name) {
    return StatementBuilder.incr(name, 1L);
  }

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
  public static Assignment incr(CharSequence name, long value) {
    return StatementManager.getManager().incr(name, value);
  }

  /**
   * Decrementation of a counter column.
   * <p>
   * This will generate: {@code name = name - 1}.
   *
   * @author paouelle
   *
   * @param  name the column name to decrement
   * @return the correspond assignment (to use in an update statement)
   * @throws NullPointerException if <code>name</code> is <code>null</code>
   */
  public static Assignment decr(CharSequence name) {
    return StatementBuilder.decr(name, 1L);
  }

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
  public static Assignment decr(CharSequence name, long value) {
    return StatementManager.getManager().decr(name, value);
  }

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
  public static Assignment prepend(CharSequence name, Object value) {
    return StatementManager.getManager().prepend(name, value);
  }

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
  public static Assignment prependAll(CharSequence name, List<?> values) {
    return StatementManager.getManager().prependAll(name, values);
  }

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
  public static Assignment append(CharSequence name, Object value) {
    return StatementManager.getManager().append(name, value);
  }

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
  public static Assignment appendAll(CharSequence name, List<?> values) {
    return StatementManager.getManager().appendAll(name, values);
  }

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
  public static Assignment discard(CharSequence name, Object value) {
    return StatementManager.getManager().discard(name, value);
  }

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
  public static Assignment discardAll(CharSequence name, List<?> values) {
    return StatementManager.getManager().discardAll(name, values);
  }

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
  public static Assignment setIdx(CharSequence name, int idx, Object value) {
    return StatementManager.getManager().setIdx(name, idx, value);
  }

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
  public static Assignment add(CharSequence name, Object value) {
    return StatementManager.getManager().add(name, value);
  }

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
  public static Assignment addAll(CharSequence name, Set<?> values) {
    return StatementManager.getManager().addAll(name, values);
  }

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
  public static Assignment remove(CharSequence name, Object value) {
    return StatementManager.getManager().remove(name, value);
  }

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
  public static Assignment removeAll(CharSequence name, Set<?> values) {
    return StatementManager.getManager().removeAll(name, values);
  }

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
  public static Assignment put(CharSequence name, Object key, Object value) {
    return StatementManager.getManager().put(name, key, value);
  }

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
  public static Assignment putAll(CharSequence name, Map<?, ?> mappings) {
    return StatementManager.getManager().putAll(name, mappings);
  }

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
  public static KeyspaceWith replication(JsonObject map) {
    return StatementManager.getManager().replication(map);
  }

  /**
   * Sets the durable write option for a keyspace. This will generate:
   * {@code DURABLE_WRITES = true}.
   *
   * @author paouelle
   *
   * @return the corresponding option (to use in a create keyspace statement)
   */
  public static KeyspaceWith durableWrites() {
    return StatementBuilder.durableWrites(true);
  }

  /**
   * Sets the durable write option for a keyspace. This will generate:
   * {@code DURABLE_WRITES = value}.
   *
   * @author paouelle
   *
   * @param  value the keyspace durable writes option value
   * @return the corresponding option (to use in a create keyspace statement)
   */
  public static KeyspaceWith durableWrites(boolean value) {
    return StatementManager.getManager().durableWrites(value);
  }

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
  public static Object raw(String str) {
    return StatementManager.getManager().raw(str);
  }

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
  public static Object fcall(String name, Object... parameters) {
    return StatementManager.getManager().fcall(name, parameters);
  }

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
  public static Object column(String name) {
    return StatementManager.getManager().column(name);
  }

  /**
   * Gets all the column names defined for a given field based on the specified
   * POJO class. The field must be annotated with {@link Column} and be part of
   * a class hierarchy annotated as an entity.
   *
   * @author paouelle
   *
   * @param <T> The type of POJO associated with the request.
   *
   * @param  clazz the POJO class to get all the column names for
   * @param  field the field to get all the column names for
   * @return a non-<code>null</code> set of all the column names the specified
   *         field is annotated with
   * @throws NullPointerException if <code>clazz</code> or <code>name</code>
   *         is <code>null</code>
   * @throws IllegalArgumentException if the field or its class are not properly
   *         annotated or the field is not in a class that is the same or a base
   *         class of the specified class
   */
  public static <T> Set<String> getColumnNamesFor(Class<T> clazz, Field field) {
    org.apache.commons.lang3.Validate.notNull(clazz, "invalid null class");
    org.apache.commons.lang3.Validate.notNull(field, "invalid null field name");
    org.apache.commons.lang3.Validate.isTrue(
      field.getDeclaringClass().isAssignableFrom(clazz),
      "field '%s.%s' is not defined in the class hieriarchy of: %s",
      field.getDeclaringClass().getName(),
      field.getName(),
      clazz.getClass().getName()
    );
    return StatementManager.getManager().getColumnNamesFor(clazz, field);
  }
}

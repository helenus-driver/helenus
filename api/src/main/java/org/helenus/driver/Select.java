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
package org.helenus.driver;

import org.helenus.driver.info.ClassInfo;
import org.helenus.driver.info.TableInfo;
import org.helenus.driver.persistence.DataType;


/**
 * The <code>Select</code> interface extends the functionality of Cassandra's
 * {@link com.datastax.driver.core.querybuilder.Select} class to provide support
 * for POJOs.
 *
 * @copyright 2015-2016 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 15, 2015 - paouelle - Creation
 *
 * @param <T> The type of POJO associated with this statement.
 *
 * @since 1.0
 */
public interface Select<T> extends ObjectClassStatement<T> {
  /**
   * Adds a WHERE clause to this statement.
   *
   * This is a shorter/more readable version for {@code where().and(clauses)}.
   *
   * @author paouelle
   *
   * @param  clause the clause to add.
   * @return the where clause of this query to which more clause can be added.
   * @throws IllegalArgumentException if the clause reference a
   *         column which is not a primary key or an index column in the POJO
   */
  public Where<T> where(Clause clause);

  /**
   * Gets a Where statement for this query without adding clause.
   *
   * @author paouelle
   *
   * @return the where clause of this query to which more clause can be added.
   */
  public Where<T> where();

  /**
   * Adds an ORDER BY clause to this statement.
   *
   * @author paouelle
   *
   * @param  orderings the orderings to define for this query.
   * @return this statement.
   * @throws IllegalStateException if an ORDER BY clause has already been
   *         provided.
   * @throws IllegalArgumentException if any of the column names are not defined
   *         by the POJO
   */
  public Select<T> orderBy(Ordering... orderings);

  /**
   * Adds a LIMIT clause to this statement.
   *
   * @author paouelle
   *
   * @param  limit the limit to set.
   * @return this statement.
   * @throws IllegalArgumentException if {@code limit &gte; 0}.
   * @throws IllegalStateException if a LIMIT clause has already been
   *         provided.
   */
  public Select<T> limit(int limit);

  /**
   * Adds an ALLOW FILTERING directive to this statement.
   *
   * @author paouelle
   *
   * @return this statement.
   */
  public Select<T> allowFiltering();

  /**
   * The <code>Where</code> interface defines a WHERE clause for a SELECT
   * statement.
   *
   * @copyright 2015-2017 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 15, 2015 - paouelle - Creation
   *
   * @param <T> The type of POJO associated with the statement.
   *
   * @since 1.0
   */
  public interface Where<T> extends ObjectClassStatement<T> {
    /**
     * Adds the provided clause to this WHERE clause.
     *
     * @author paouelle
     *
     * @param  clause the clause to add
     * @return this WHERE clause
     * @throws IllegalArgumentException if the clause referenced a
     *         column which is not a primary key or an index column in the POJO
     */
    public Where<T> and(Clause clause);

    /**
     * Adds an ORDER BY clause to the SELECT statement this WHERE clause if part
     * of.
     *
     * @author paouelle
     *
     * @param  orderings the orderings to add
     * @return the select statement this Where clause if part of
     * @throws IllegalStateException if an ORDER BY clause has already been
     *         provided.
     * @throws IllegalArgumentException if any of the column names are not defined
     *         by the POJO
     */
    public Select<T> orderBy(Ordering... orderings);

    /**
     * Adds a LIMIT clause to the SELECT statement this WHERE clause if part of.
     *
     * @author paouelle
     *
     * @param  limit the limit to set
     * @return the select statement this Where clause if part of
     * @throws IllegalArgumentException if {@code limit &gte; 0}
     * @throws IllegalStateException if a LIMIT clause has already been
     *         provided
     */
    public Select<T> limit(int limit);
  }

  /**
   * The <code>Builder</code> interface defines an in-construction SELECT
   * statement.
   *
   * @copyright 2015-2017 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 15, 2015 - paouelle - Creation
   *
   * @param <T> The type of POJO associated with the statement.
   *
   * @since 1.0
   */
  public interface Builder<T> {
    /**
     * Gets the POJO class associated with this statement builder.
     *
     * @author paouelle
     *
     * @return the POJO class associated with this statement builder
     */
    public Class<T> getObjectClass();

    /**
     * Gets the POJO class information associated with this statement builder.
     *
     * @author paouelle
     *
     * @return the POJO class info associated with this statement builder
     */
    public ClassInfo<T> getClassInfo();

    /**
     * Specify to select from the keyspace as defined in the POJO and the
     * specified table.
     * <p>
     * This flavor should be used when the POJO doesn't require keyspace keys to the
     * keyspace name.
     *
     * @author paouelle
     *
     * @param  table the name of the table to select from
     * @return a newly build SELECT statement
     * @throws NullPointerException if <code>table</code> is <code>null</code>
     * @throws IllegalArgumentException if the table or any of the referenced
     *         columns are not defined by the POJO
     */
    public Select<T> from(String table);

    /**
     * Specify to select from the keyspace as defined in the POJO and the
     * specified table.
     * <p>
     * This flavor should be used when the POJO doesn't require keyspace keys to the
     * keyspace name.
     *
     * @author paouelle
     *
     * @param  table the table to select from
     * @return a newly build SELECT statement
     * @throws NullPointerException if <code>table</code> is <code>null</code>
     * @throws IllegalArgumentException if the table or any of the referenced
     *         columns are not defined by the POJO
     */
    public Select<T> from(TableInfo<T> table);
  }

  /**
   * The <code>Selection</code> interface defines a selection clause for an
   * in-construction SELECT statement.
   *
   * @copyright 2015-2015 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 15, 2015 - paouelle - Creation
   *
   * @param <T> The type of POJO associated with the response from this statement.
   *
   * @since 1.0
   */
  public interface Selection<T> extends Builder<T> {
    /**
     * Selects all columns (i.e. "SELECT *  ...")
     *
     * @author paouelle
     *
     * @return an in-build SELECT statement.
     * @throws IllegalStateException if some columns had already been selected
     *         for this builder
     */
    public Builder<T> all();

    /**
     * Selects the count of all returned rows (i.e. "SELECT count(*) ...").
     *
     * @author paouelle
     *
     * @return an in-build SELECT statement.
     * @throws IllegalStateException if some columns had already been selected
     *         for this builder.
     */
    public Builder<T> countAll();

    /**
     * Selects the provided column.
     *
     * @author paouelle
     *
     * @param  name the new column name to add.
     * @return this in-build SELECT statement
     * @throws NullPointerException if <code>name</code> is <code>null</code>
     * @throws IllegalArgumentException if the specified column is not defined
     *         by the POJO
     */
    public SelectionOrAlias<T> column(Object name);

    /**
     * Selects the write time of provided column.
     * <p>
     * This is a shortcut for {@code fcall("writetime", StatementBuilder.column(name))}.
     *
     * @author paouelle
     *
     * @param  name the name of the column to select the write time of.
     * @return this in-build SELECT statement
     * @throws NullPointerException if <code>name</code> is <code>null</code>
     * @throws IllegalArgumentException if the specified column is not defined
     *         by the POJO
     */
    public SelectionOrAlias<T> writeTime(String name);

    /**
     * Selects the ttl of provided column.
     * <p>
     * This is a shortcut for {@code fcall("ttl", StatementBuilder.column(name))}.
     *
     * @author paouelle
     *
     * @param  name the name of the column to select the ttl of.
     * @return this in-build SELECT statement
     * @throws NullPointerException if <code>name</code> is <code>null</code>
     * @throws IllegalArgumentException if the specified column is not defined
     *         by the POJO
     */
    public SelectionOrAlias<T> ttl(String name);

    /**
     * Creates a function call.
     * <p>
     * Please note that the parameters are interpreted as values, and so
     * {@code fcall("textToBlob", "foo")} will generate the string
     * {@code "textToBlob('foo')"}. If you want to generate
     * {@code "textToBlob(foo)"}, i.e. if the argument must be interpreted
     * as a column name (in a select clause), you will need to use the
     * {@link StatementBuilder#column} method, and so
     * {@code fcall("textToBlob", StatementBuilder.column(foo)}.
     *
     * @author paouelle
     *
     * @param  name the name of the column to select the function for
     * @param  parameters the parameters to the function
     * @return this in-build SELECT statement
     * @throws NullPointerException if <code>name</code> is <code>null</code>
     * @throws IllegalArgumentException if any of specified column parameters are
     *         not defined by the POJO
     */
    public SelectionOrAlias<T> fcall(String name, Object... parameters);

    /**
     * Creates a cast of an expression to a given CQL type.
     *
     * @param  column the expression to cast. It can be a complex expression like a
     *         {@link StatementBuilder#fcall}
     * @param  targetType the target CQL type to cast to
     * @return this in-build SELECT statement
     */
    public SelectionOrAlias<T> cast(Object column, DataType targetType);

    /**
     * Selects the provided raw expression.
     * <p>
     * The provided string will be appended to the query as-is, without any form
     * of escaping or quoting.
     *
     * @param  rawString the raw expression to add
     * @return this in-build SELECT statement
     */
    public SelectionOrAlias<T> raw(String rawString);

    /**
     * Creates a {@code toJson()} function call. This is a shortcut for
     * {@code fcall("toJson", StatementManager.column(name))}.
     * <p>
     * Support for JSON functions has been added in Cassandra 2.2.
     * The {@code toJson()} function is similar to {@code SELECT JSON} statements,
     * but applies to a single column value instead of the entire row,
     * and produces a JSON-encoded string representing the normal Cassandra column value.
     * <p>
     * It may only be used in the selection clause of a {@code SELECT} statement.
     *
     * @param  name the column name
     * @return this in-build SELECT statement
     */
    public SelectionOrAlias<T> toJson(String name);
  }

  /**
   * The <code>SelectionOrAlias</code> interface defines a selection clause for an
   * in-construction SELECT statement that differs only in that you can add an
   * alias for the previously selected item through {@link #as}.
   *
   * @copyright 2015-2017 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 3, 2017 - paouelle - Creation
   *
   * @param <T> The type of POJO associated with the response from this statement.
   *
   * @since 1.0
   */
  public interface SelectionOrAlias<T> extends Selection<T> {
    /**
     * Adds an alias for the just selected item.
     *
     * @author paouelle
     *
     * @param  alias the name of the alias to use
     * @return this in-build SELECT statement
     */
    public Selection<T> as(String alias);
  }

  /**
   * The <code>TableSelection</code> interface defines a selection clause for an
   * in-construction SELECT statement.
   *
   * @copyright 2015-2016 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 6, 2016 - paouelle - Creation
   *
   * @param <T> The type of POJO associated with the response from this statement.
   *
   * @since 1.0
   */
  public interface TableSelection<T> {
    /**
     * Gets the POJO class associated with this statement builder.
     *
     * @author paouelle
     *
     * @return the POJO class associated with this statement builder
     */
    public Class<T> getObjectClass();

    /**
     * Gets the POJO class information associated with this statement builder.
     *
     * @author paouelle
     *
     * @return the POJO class info associated with this statement builder
     */
    public ClassInfo<T> getClassInfo();

    /**
     * Selects all columns (i.e. "SELECT *  ...")
     *
     * @author paouelle
     *
     * @return the SELECT statement.
     * @throws IllegalStateException if some columns had already been selected
     *         for this builder
     */
    public Select<T> all();

    /**
     * Selects the count of all returned rows (i.e. "SELECT count(*) ...").
     *
     * @author paouelle
     *
     * @return the SELECT statement.
     * @throws IllegalStateException if some columns had already been selected
     *         for this builder.
     */
    public Select<T> countAll();

    /**
     * Selects the provided column.
     *
     * @author paouelle
     *
     * @param  name the new column name to add.
     * @return this in-build SELECT statement
     * @throws NullPointerException if <code>name</code> is <code>null</code>
     * @throws IllegalArgumentException if the specified column is not defined
     *         by the POJO
     */
    public TableSelection<T> column(Object name);

    /**
     * Selects the write time of provided column.
     * <p>
     * This is a shortcut for {@code fcall("writetime", StatementBuilder.column(name))}.
     *
     * @author paouelle
     *
     * @param  name the name of the column to select the write time of.
     * @return this in-build SELECT statement
     * @throws NullPointerException if <code>name</code> is <code>null</code>
     * @throws IllegalArgumentException if the specified column is not defined
     *         by the POJO
     */
    public TableSelection<T> writeTime(String name);

    /**
     * Selects the ttl of provided column.
     * <p>
     * This is a shortcut for {@code fcall("ttl", StatementBuilder.column(name))}.
     *
     * @author paouelle
     *
     * @param  name the name of the column to select the ttl of.
     * @return this in-build SELECT statement
     * @throws NullPointerException if <code>name</code> is <code>null</code>
     * @throws IllegalArgumentException if the specified column is not defined
     *         by the POJO
     */
    public TableSelection<T> ttl(String name);

    /**
     * Creates a function call.
     * <p>
     * Please note that the parameters are interpreted as values, and so
     * {@code fcall("textToBlob", "foo")} will generate the string
     * {@code "textToBlob('foo')"}. If you want to generate
     * {@code "textToBlob(foo)"}, i.e. if the argument must be interpreted
     * as a column name (in a select clause), you will need to use the
     * {@link StatementBuilder#column} method, and so
     * {@code fcall("textToBlob", StatementBuilder.column(foo)}.
     *
     * @author paouelle
     *
     * @param  name the name of the column to select the function for
     * @param  parameters the parameters to the function
     * @return this in-build SELECT statement
     * @throws NullPointerException if <code>name</code> is <code>null</code>
     * @throws IllegalArgumentException if any of specified column parameters are
     *         not defined by the POJO
     */
    public TableSelection<T> fcall(String name, Object... parameters);
  }
}

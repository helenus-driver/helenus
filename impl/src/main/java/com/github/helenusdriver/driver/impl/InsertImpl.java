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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import com.github.helenusdriver.commons.collections.iterators.CombinationIterator;
import com.github.helenusdriver.driver.ColumnPersistenceException;
import com.github.helenusdriver.driver.Insert;
import com.github.helenusdriver.driver.ObjectExistException;
import com.github.helenusdriver.driver.StatementBridge;
import com.github.helenusdriver.driver.Using;
import com.github.helenusdriver.driver.VoidFuture;

/**
 * The <code>InsertImpl</code> class extends the functionality of Cassandra's
 * {@link com.datastax.driver.core.querybuilder.Insert} class to provide support
 * for POJOs.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 19, 2015 - paouelle - Creation
 *
 * @param <T> The type of POJO associated with this statement.
 *
 * @since 1.0
 */
public class InsertImpl<T>
  extends StatementImpl<Void, VoidFuture, T>
  implements Insert<T> {
  /**
   * Holds the column names being inserted.
   *
   * @author paouelle
   */
  private final Set<String> columns = new LinkedHashSet<>(32);

  /**
   * Holds the "USING" options.
   *
   * @author paouelle
   */
  private final OptionsImpl<T> usings;

  /**
   * Flag indicating if the "IF NOT EXIST" option has been selected.
   *
   * @author paouelle
   */
  private volatile boolean ifNotExists;

  /**
   * Flag indicating if all values were added already.
   *
   * @author paouelle
   */
  private volatile boolean allValuesAdded;

  /**
   * Instantiates a new <code>InsertImpl</code> object.
   *
   * @author paouelle
   *
   * @param  context the non-<code>null</code> class info context for the POJO
   *         associated with this statement
   * @param  mgr the non-<code>null</code> statement manager
   * @param  bridge the non-<code>null</code> statement bridge
   * @throws NullPointerException if <code>context</code> is <code>null</code>
   * @throws IllegalArgumentException if unable to compute the keyspace name
   *         based from the given object
   */
  public InsertImpl(
    ClassInfoImpl<T>.POJOContext context,
    StatementManagerImpl mgr,
    StatementBridge bridge
  ) {
    super(Void.class, context, mgr, bridge);
    this.usings = new OptionsImpl<>(this);
  }

  /**
   * Builds a query string for the specified table.
   *
   * @author paouelle
   *
   * @param  table the non-<code>null</code> table for which to build a query
   *         string
   * @param  builders the non-<code>null</code> list of builders where to add
   *         the query strings built
   * @throws IllegalArgumentException if the keyspace has not yet been computed
   *         and cannot be computed with the provided suffixes yet or if the
   *         POJO is missing primary, clustering, or mandatory columns defined
   *         by the specified table
   * @throws ColumnPersistenceException if unable to persist a column's value
   */
  @SuppressWarnings({"cast", "unchecked", "rawtypes"})
  void buildQueryStrings(TableInfoImpl<T> table, List<StringBuilder> builders) {
    final Map<String, Object> columns;

    if (allValuesAdded || this.columns.isEmpty()) {
      // either all columns were added so just get all of them from the table
      // or again no columns were added so fallback to all
      columns = getPOJOContext().getColumnValues(table.getName());
    } else {
      // we need to make sure all primary and mandatory columns are in there first
      final Map<String, Object> mpkcolumns
        = getPOJOContext().getMandatoryAndPrimaryKeyColumnValues(table.getName());

      columns = new LinkedHashMap<>(mpkcolumns.size() + this.columns.size());
      columns.putAll(mpkcolumns);
      // now add those that were manually added
      columns.putAll(getPOJOContext().getColumnValues(
        table.getName(), (Collection<CharSequence>)(Collection)this.columns)
      );
    }
    // check if the table has multi-keys in which case we need to iterate all
    // possible combinations/values for all keys and generate separate insert
    // statements
    final Collection<FieldInfoImpl<T>> multiKeys = table.getMultiKeys();

    if (!multiKeys.isEmpty()) {
      // prepare sets of values for all multi-keys
      final Collection<Object>[] sets = new Collection[multiKeys.size()];
      int j = -1;

      for (final FieldInfoImpl<T> finfo: multiKeys) {
        sets[++j] = (Set<Object>)columns.get(finfo.getColumnName());
      }
      // now iterate all combination of these sets
      for (final Iterator<List<Object>> i = new CombinationIterator<>(Object.class, sets); i.hasNext(); ) {
        final List<Object> ckeys = i.next();

        j = -1;
        // add all multi-key column values from this combination to the column map
        for (final FieldInfoImpl<T> finfo: multiKeys) {
          columns.put(StatementImpl.MK_PREFIX + finfo.getColumnName(), ckeys.get(++j));
        }
        // finally build the query for this combination
        buildQueryString(table, columns, builders);
      }
    } else { // only one statement to generate!
      buildQueryString(table, columns, builders);
    }
  }

  /**
   * Builds a query string for the specified table using the specified columns.
   *
   * @author paouelle
   *
   * @param  table the non-<code>null</code> table for which to build a query
   *         string
   * @param  builders the non-<code>null</code> list of builders where to add
   *         the query strings built
   * @param  columns the set of columns and values to insert
   * @throws IllegalArgumentException if the keyspace has not yet been computed
   *         and cannot be computed with the provided suffixes yet or if the
   *         POJO is missing primary, clustering, or mandatory columns defined
   *         by the specified table
   * @throws ColumnPersistenceException if unable to persist a column's value
   */
  @SuppressWarnings("synthetic-access")
  private void buildQueryString(
    TableInfoImpl<T> table, Map<String, Object> columns, List<StringBuilder> builders
  ) {
    final StringBuilder builder = new StringBuilder();

    builder.append("INSERT INTO ");
    if (getKeyspace() != null) {
      Utils.appendName(getKeyspace(), builder).append(".");
    }
    Utils.appendName(table.getName(), builder);
    builder.append("(");
    Utils.joinAndAppendNames(builder, ",", columns.keySet());
    builder.append(") VALUES (");
    Utils.joinAndAppendValues(builder, ",", columns.values());
    builder.append(")");
    if (ifNotExists) {
      builder.append(" IF NOT EXISTS");
    }
    if (!usings.usings.isEmpty()) {
      builder.append(" USING ");
      Utils.joinAndAppend(table, builder, " AND ", usings.usings);
    }
    builders.add(builder);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.impl.StatementImpl#buildQueryStrings()
   */
  @Override
  protected StringBuilder[] buildQueryStrings() {
    final Collection<TableInfoImpl<T>> tables = getPOJOContext().getClassInfo().getTables();
    final List<StringBuilder> builders = new ArrayList<>(tables.size());

    for (final TableInfoImpl<T> table: tables) {
      buildQueryStrings(table, builders);
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
   * @see com.github.helenusdriver.driver.impl.StatementImpl#appendGroupType(java.lang.StringBuilder)
   */
  @Override
  protected void appendGroupType(StringBuilder builder) {
    builder.append("BATCH");
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
    if (isCounterOp()) {
      builder.append(" COUNTER");
    }
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.Insert#valuesFromObject()
   */
  @Override
  public Insert<T> valuesFromObject() {
    if (!allValuesAdded) {
      columns.clear();
      this.allValuesAdded = true;
      // no need to keep track of the columns since we set the allValuesAdded flag
      setDirty();
    }
    return this;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.Insert#value(java.lang.String)
   */
  @Override
  public Insert<T> value(String name) {
    getPOJOContext().getClassInfo().validateColumn(name);
    if (!allValuesAdded) {
      final int size = columns.size();

      columns.add(name);
      if (size != columns.size()) { // new one was added
        setDirty();
      }
    }
    return this;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.Insert#values(java.lang.String[])
   */
  @Override
  public Insert<T> values(String... names) {
    getPOJOContext().getClassInfo().validateColumns(names);
    if (!allValuesAdded) {
      final int size = columns.size();

      columns.addAll(getPOJOContext().getClassInfo().getColumns());
      if (size != columns.size()) { // new ones were added
        setDirty();
      }
    }
    return this;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.Insert#using(com.github.helenusdriver.driver.Using)
   */
  @Override
  public Options<T> using(Using using) {
    return usings.and(using);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.Insert#ifNotExists()
   */
  @Override
  public Insert<T> ifNotExists() {
    this.ifNotExists = true;
    setDirty();
    return this;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.impl.StatementImpl#executeAsync()
   */
  @Override
  public VoidFuture executeAsync() {
    // if we have no conditions then no need for special treatment of the response
    if (!ifNotExists) {
      return super.executeAsync();
    }
    return bridge.newVoidFuture(executeAsyncRaw(), new VoidFuture.PostProcessor() {
      @Override
      public void postProcess(ResultSet result) {
        // update result set when using conditions have only one row
        // where the entry "[applied]" is a boolean indicating if the insert was
        // successful and the rest are all the conditional values specified in
        // the INSERT request

        // check if the condition was successful
        final Row row = result.one();

        if (row == null) {
          throw new ObjectExistException("no result row returned");
        }
        if (!row.getBool("[applied]")) {
          throw new ObjectExistException(row, "insert not applied");
        }
        // else all good
      }
    });
  }

  /**
   * The <code>OptionsImpl</code> class defines the options of an INSERT
   * statement.
   *
   * @copyright 2015-2015 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 19, 2015 - paouelle - Creation
   *
   * @param <T> The type of POJO associated with the statement.
   *
   * @since 1.0
   */
  public static class OptionsImpl<T>
    extends ForwardingStatementImpl<Void, VoidFuture, T, InsertImpl<T>>
    implements Options<T> {
    /**
     * Holds the list of "USINGS" options.
     *
     * @author paouelle
     */
    private final List<UsingImpl> usings = new ArrayList<>(5);

    /**
     * Instantiates a new <code>OptionsImpl</code> object.
     *
     * @author paouelle
     *
     * @param statement the non-<code>null</code> statement for which we are
     *        creating options
     */
    OptionsImpl(InsertImpl<T> statement) {
      super(statement);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.github.helenusdriver.driver.Insert.Options#and(com.github.helenusdriver.driver.Using)
     */
    @Override
    public Options<T> and(Using using) {
      org.apache.commons.lang3.Validate.notNull(using, "invalid null using");
      org.apache.commons.lang3.Validate.isTrue(
        using instanceof UsingImpl,
        "unsupported class of usings: %s",
        using.getClass().getName()
      );
      usings.add((UsingImpl)using);
      setDirty();
      return this;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.github.helenusdriver.driver.Insert.Options#value(java.lang.String)
     */
    @Override
    public Insert<T> value(String name) {
      return statement.value(name);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.github.helenusdriver.driver.Insert.Options#values(java.lang.String[])
     */
    @Override
    public Insert<T> values(String... names) {
      return statement.values(names);
    }
  }
}
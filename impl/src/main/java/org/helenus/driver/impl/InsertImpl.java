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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;

import org.helenus.commons.collections.iterators.CombinationIterator;
import org.helenus.driver.ColumnPersistenceException;
import org.helenus.driver.Insert;
import org.helenus.driver.ObjectExistException;
import org.helenus.driver.StatementBridge;
import org.helenus.driver.Using;
import org.helenus.driver.VoidFuture;
import org.helenus.driver.info.TableInfo;

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
   * Holds the tables to insert into.
   *
   * @author paouelle
   */
  private final List<TableInfoImpl<T>> tables = new ArrayList<>(8);

  /**
   * Holds the column names being inserted.
   *
   * @author paouelle
   */
  private final Set<String> columns = new LinkedHashSet<>(32);

  /**
   * Holds the column names and values being inserted.
   *
   * @author paouelle
   */
  private final Map<String, Object> values = new LinkedHashMap<>(32);

  /**
   * Holds the "USING" options.
   *
   * @author paouelle
   */
  private final OptionsImpl<T> usings;

  /**
   * Flag indicating if the "IF NOT EXISTS" option has been selected.
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
   * @param  tables the tables to insert into
   * @param  mgr the non-<code>null</code> statement manager
   * @param  bridge the non-<code>null</code> statement bridge
   * @throws NullPointerException if <code>context</code> is <code>null</code>
   * @throws IllegalArgumentException if unable to compute the keyspace name
   *         based from the given object
   */
  InsertImpl(
    ClassInfoImpl<T>.POJOContext context,
    String[] tables,
    StatementManagerImpl mgr,
    StatementBridge bridge
  ) {
    super(Void.class, context, mgr, bridge);
    if (tables != null) {
      for (final String table: tables) {
        if (table != null) {
          this.tables.add((TableInfoImpl<T>)context.getClassInfo().getTable(table));
        }
      }
    } else { // fallback to all
      this.tables.addAll(context.getClassInfo().getTablesImpl());
    }
    this.usings = new OptionsImpl<>(this);
  }

  /**
   * Instantiates a new <code>InsertImpl</code> object.
   *
   * @author paouelle
   *
   * @param  context the non-<code>null</code> class info context for the POJO
   *         associated with this statement
   * @param  table the table to insert into
   * @param  mgr the non-<code>null</code> statement manager
   * @param  bridge the non-<code>null</code> statement bridge
   * @throws NullPointerException if <code>context</code> is <code>null</code>
   * @throws IllegalArgumentException if unable to compute the keyspace name
   *         based from the given object
   */
  InsertImpl(
    ClassInfoImpl<T>.POJOContext context,
    TableInfoImpl<T> table,
    StatementManagerImpl mgr,
    StatementBridge bridge
  ) {
    super(Void.class, context, mgr, bridge);
    tables.add(table);
    this.usings = new OptionsImpl<>(this);
  }

  /**
   * Adds the specified table to the list of tables to insert into.
   *
   * @author paouelle
   *
   * @param table the non-<code>null</code> table to insert into
   */
  void into(TableInfoImpl<T> table) {
    tables.add(table);
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

    try {
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
        // finally add the specific values for this statement
        columns.putAll(values);
      }
    } catch (EmptyOptionalPrimaryKeyException e) {
      // ignore and continue without updating this table
      return;
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
   * Build query strings for each tables into the specified list.
   *
   * @author paouelle
   *
   * @param builders the list where to store all tables generated query strings
   */
  void buildQueryStrings(List<StringBuilder> builders) {
    for (final TableInfoImpl<T> table: tables) {
      buildQueryStrings(table, builders);
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
   * @see org.helenus.driver.impl.StatementImpl#simpleSize()
   */
  @Override
  protected int simpleSize() {
    if (super.simpleSize == -1) {
      if (!isEnabled()) {
        super.simpleSize = 0;
      } else {
        super.simpleSize = tables.size();
      }
    }
    return super.simpleSize;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.StatementImpl#buildQueryStrings()
   */
  @Override
  protected StringBuilder[] buildQueryStrings() {
    if (!isEnabled()) {
      return null;
    }
    final List<StringBuilder> builders = new ArrayList<>(tables.size());

    buildQueryStrings(builders);
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
   * @see org.helenus.driver.impl.StatementImpl#appendGroupType(java.lang.StringBuilder)
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
   * @see org.helenus.driver.impl.StatementImpl#appendGroupSubType(java.lang.StringBuilder)
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
   * @see org.helenus.driver.impl.StatementImpl#executeAsync0()
   */
  @Override
  protected VoidFuture executeAsync0() {
    // if we are disabled or have no conditions then no need for special treatment of the response
    if (!isEnabled() || !ifNotExists) {
      return super.executeAsync0();
    }
    return bridge.newVoidFuture(executeAsyncRaw0(), new VoidFuture.PostProcessor() {
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
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.Insert#isEmpty()
   */
  @Override
  public boolean isEmpty() {
    if (allValuesAdded || columns.isEmpty()) {
      // either is considered as if all columns were added
      return true;
    }
    return false;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.Insert#tables()
   */
  @Override
  public Stream<TableInfo<T>> tables() {
    return tables.stream().map(t -> t);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.Insert#valuesFromObject()
   */
  @Override
  public Insert<T> valuesFromObject() {
    org.apache.commons.lang3.Validate.validState(
      values.isEmpty(),
      "separate values have already been added to this statement"
    );
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
   * @see org.helenus.driver.Insert#value(java.lang.String)
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
   * @see org.helenus.driver.Insert#values(java.lang.String[])
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
   * @see org.helenus.driver.Insert#value(java.lang.String, java.lang.Object)
   */
  @Override
  public Insert<T> value(String name, Object value) {
    org.apache.commons.lang3.Validate.validState(
      !allValuesAdded,
      "all columns from the object have already been added to this statement"
    );
    if (value instanceof Optional) {
      value = ((Optional<?>)value).orElse(null);
    }
    getPOJOContext().getClassInfo().validateColumnAndValue(name, value);
    columns.remove(name);
    values.put(name, value);
    return this;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.Insert#using(org.helenus.driver.Using)
   */
  @Override
  public Options<T> using(Using<?> using) {
    return usings.and(using);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.Insert#usings()
   */
  @SuppressWarnings({"rawtypes", "cast", "unchecked", "synthetic-access"})
  @Override
  public Stream<Using<?>> usings() {
    return (Stream<Using<?>>)(Stream)usings.usings.stream();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.Insert#getUsing(java.lang.String)
   */
  @SuppressWarnings({"rawtypes", "cast", "unchecked"})
  @Override
  public <U> Optional<Using<U>> getUsing(String name) {
    return (Optional<Using<U>>)(Optional)usings()
      .filter(u -> u.getName().equals(name)).findAny();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.Insert#ifNotExists()
   */
  @Override
  public Insert<T> ifNotExists() {
    this.ifNotExists = true;
    setDirty();
    return this;
  }

  /**
   * The <code>BuilderImpl</code> class defines an in-construction INSERT statement.
   *
   * @copyright 2015-2016 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Mar 20, 2015 - paouelle - Creation
   *
   * @param <T> The type of POJO associated with the statement.
   *
   * @since 1.0
   */
  public static class BuilderImpl<T>
    extends StatementImpl<Void, VoidFuture, T> implements Builder<T> {
    /**
     * Instantiates a new <code>BuilderImpl</code> object.
     *
     * @author paouelle
     *
     * @param context the non-<code>null</code> class info context
     *        associated with this statement
     * @param mgr the non-<code>null</code> statement manager
     * @param bridge the non-<code>null</code> statement bridge
     */
    BuilderImpl(
      ClassInfoImpl<T>.POJOContext context,
      StatementManagerImpl mgr,
      StatementBridge bridge
    ) {
      super(Void.class, context, mgr, bridge);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.StatementImpl#simpleSize()
     */
    @Override
    protected int simpleSize() {
      return getContext().getClassInfo().getNumTables(); // don't know more yet!!!
    }


    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.StatementImpl#buildQueryStrings()
     */
    @Override
    protected StringBuilder[] buildQueryStrings() {
      return ((InsertImpl<T>)intoAll()).buildQueryStrings();
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.Insert.Builder#into(java.lang.String[])
     */
    @Override
    public Insert<T> into(String... tables) {
      return init(new InsertImpl<>(getPOJOContext(), tables, mgr, bridge));
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.Insert.Builder#into(java.util.stream.Stream)
     */
    @Override
    public Insert<T> into(Stream<String> tables) {
      return init(new InsertImpl<>(
        getPOJOContext(), tables.toArray(String[]::new), mgr, bridge
      ));
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.Insert.Builder#intoAll()
     */
    @Override
    public Insert<T> intoAll() {
      return init(
        new InsertImpl<>(getPOJOContext(), (String[])null, mgr, bridge)
      );
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.StatementImpl#getQueryString()
     */
    @Override
    public String getQueryString() {
      return intoAll().getQueryString();
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.StatementImpl#execute()
     */
    @Override
    public Void execute() {
      return intoAll().execute();
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.StatementImpl#executeAsync()
     */
    @Override
    public VoidFuture executeAsync() {
      return intoAll().executeAsync();
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.StatementImpl#executeRaw()
     */
    @Override
    public ResultSet executeRaw() {
      return intoAll().executeRaw();
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.StatementImpl#executeAsyncRaw()
     */
    @Override
    public ResultSetFuture executeAsyncRaw() {
      return intoAll().executeAsyncRaw();
    }
  }

  /**
   * The <code>OptionsImpl</code> class defines the options of an INSERT
   * statement.
   *
   * @copyright 2015-2016 The Helenus Driver Project Authors
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
    private final List<UsingImpl<?>> usings = new ArrayList<>(5);

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
     * @see org.helenus.driver.Insert.Options#and(org.helenus.driver.Using)
     */
    @Override
    public Options<T> and(Using<?> using) {
      org.apache.commons.lang3.Validate.notNull(using, "invalid null using");
      org.apache.commons.lang3.Validate.isTrue(
        using instanceof UsingImpl,
        "unsupported class of usings: %s",
        using.getClass().getName()
      );
      usings.add(((UsingImpl<?>)using).setStatement(statement));
      setDirty();
      return this;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.Insert.Options#value(java.lang.String)
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
     * @see org.helenus.driver.Insert.Options#values(java.lang.String[])
     */
    @Override
    public Insert<T> values(String... names) {
      return statement.values(names);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.Insert.Options#value(java.lang.String, java.lang.Object)
     */
    @Override
    public Insert<T> value(String name, Object value) {
      return statement.value(name, value);
    }
  }
}

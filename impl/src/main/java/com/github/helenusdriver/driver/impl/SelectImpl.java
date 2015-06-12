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
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import com.datastax.driver.core.CompoundResultSetFuture;
import com.datastax.driver.core.EmptyResultSetFuture;
import com.datastax.driver.core.ResultSetFuture;
import com.github.helenusdriver.commons.collections.iterators.CombinationIterator;
import com.github.helenusdriver.driver.Clause;
import com.github.helenusdriver.driver.ObjectSet;
import com.github.helenusdriver.driver.ObjectSetFuture;
import com.github.helenusdriver.driver.Ordering;
import com.github.helenusdriver.driver.Select;
import com.github.helenusdriver.driver.StatementBridge;
import com.github.helenusdriver.driver.impl.Utils.CName;

/**
 * The <code>SelectImpl</code> class extends the functionality of Cassandra's
 * {@link com.datastax.driver.core.querybuilder.Select} class to provide support
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
public class SelectImpl<T>
  extends StatementImpl<ObjectSet<T>, ObjectSetFuture<T>, T>
  implements Select<T> {
  /**
   * Holds a special list of column names used when we are counting instead of
   * querying.
   *
   * @author paouelle
   */
  static final List<Object> COUNT_ALL = Collections.<Object>singletonList(new Utils.FCall("count", new Utils.RawString("*")));

  /**
   * Holds the table info for this statement.
   *
   * @author paouelle
   */
  final TableInfoImpl<T> table;

  /**
   * Holds the where statement part.
   *
   * @author paouelle
   */
  private final WhereImpl<T> where;

  /**
   * Holds the registered ordering statement parts.
   *
   * @author paouelle
   */
  private List<OrderingImpl> orderings;

  /**
   * Holds the limit associated with this statement.
   *
   * @author paouelle
   */
  private int limit = -1;

  /**
   * Flag indicating if filtering is allowed.
   *
   * @author paouelle
   */
  private boolean allowFiltering;

  /**
   * Holds the computed keyspace name for this statement when suffixes are used
   * with an IN clause.
   *
   * @author paouelle
   */
  private String keyspace = null;

  /**
   * Holds underlying select statements for all combinations of an IN clause
   * on keyspace suffixes.
   *
   * @author paouelle
   */
  private List<SelectImpl<T>> statements = null;

  /**
   * Holds the collection of suffix values when using the IN clause.
   *
   * @author paouelle
   */
  private Map<String, Collection<?>> suffixes = null;

  /**
   * List of columns added.
   *
   * @author paouelle
   */
  protected List<Object> columnNames;

  /**
   * Flag to keep track of whether or not the special COUNT() or * has been
   * selected.
   *
   * @author paouelle
   */
  protected boolean countOrAllSelected = false;


  /**
   * Instantiates a new <code>SelectImpl</code> object.
   *
   * @author paouelle
   *
   * @param  context the non-<code>null</code> class info context for the POJO
   *         associated with this statement
   * @param  table the non-<code>null</code> table to select from
   * @param  columnNames the selected columns
   * @param  countOrAllSelected <code>true</code> if the special COUNT() or *
   *         has been selected
   * @param  mgr the non-<code>null</code> statement manager
   * @param  bridge the non-<code>null</code> statement bridge
   * @throws NullPointerException if <code>context</code> is <code>null</code>
   * @throws IllegalArgumentException if the table or any of the referenced
   *         columns are not defined by the POJO
   */
  @SuppressWarnings({"cast", "unchecked", "rawtypes"})
  SelectImpl(
    ClassInfoImpl<T>.Context context,
    String table,
    List<Object> columnNames,
    boolean countOrAllSelected,
    StatementManagerImpl mgr,
    StatementBridge bridge
  ) {
    super((Class<ObjectSet<T>>)(Class)ObjectSet.class, context, mgr, bridge);
    this.table = (TableInfoImpl<T>)context.getClassInfo().getTable(table);
    this.columnNames = columnNames;
    this.countOrAllSelected = countOrAllSelected;
    this.where = new WhereImpl<>(this);
    org.apache.commons.lang3.Validate.isTrue(
      !(!countOrAllSelected && CollectionUtils.isEmpty(columnNames)),
      "must select at least one column"
    );
    if (columnNames != null) {
      this.table.validateColumns(columnNames);
    }
  }

  /**
   * Instantiates a new <code>SelectImpl</code> object.
   *
   * @author paouelle
   *
   * @param statement the non-<code>null</code> select statement to be copied
   */
  SelectImpl(SelectImpl<T> statement) {
    super(statement, statement.getContext().getClassInfo().newContext());
    setDirty();
    clearKeyspace();
    this.table = statement.table;
    this.columnNames = statement.columnNames;
    this.countOrAllSelected = statement.countOrAllSelected;
    this.where = new WhereImpl<>(statement.where, this);
    this.limit = statement.limit;
    this.allowFiltering = statement.allowFiltering;
    this.orderings = statement.orderings;
  }

  /**
   * Gets the underlying statements when an IN clause is used with suffix keys.
   *
   * @author paouelle
   *
   * @return a stream of all underlying statements to execute
   */
  @SuppressWarnings({"synthetic-access", "cast", "unchecked", "rawtypes"})
  private Stream<SelectImpl<T>> statements() {
    // if we get here then the query was done for suffixes with an IN clause
    if (statements == null) {
      // in such case, we must generate one query for each combinations
      // and aggregate the results
      final List<String> snames = new ArrayList<>(suffixes.keySet());
      final CombinationIterator<Object> ci = new CombinationIterator<>(
        Object.class, (Collection<Collection<Object>>)(Collection)suffixes.values()
      );
      final List<SelectImpl<T>> statements = new ArrayList<>(ci.size());

      while (ci.hasNext()) {
        final List<Object> svalues = ci.next();
        // create a new select statement as a dup of this one but with
        // the suffixes from the current combination
        final SelectImpl<T> s = new SelectImpl<>(this);

        for (int j = 0; j < snames.size(); j++) {
          s.getContext().addSuffix(snames.get(j), svalues.get(j));
        }
        statements.add(s);
      }
      this.statements = statements; // cache the underlying statements
    }
    return statements.stream();
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
    return new StringBuilder[] { buildQueryString() };
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.impl.StatementImpl#buildQueryString()
   */
  @Override
  @SuppressWarnings("synthetic-access")
  protected StringBuilder buildQueryString() {
    final StringBuilder builder = new StringBuilder();

    builder.append("SELECT ");
    if (columnNames == null) {
      builder.append("*");
    } else {
      if (countOrAllSelected) {
        Utils.joinAndAppendNames(builder, ",", columnNames);
      } else {
        // prepend all mandatory and primary key columns if it is not the special COUNT()
        final Set<String> set = new HashSet<>(25);

        for (final Object name: columnNames) {
          if (name instanceof String) {
            set.add((String)name);
          } else if (name instanceof CName) {
            set.add(((CName)name).getName());
          } // else - ?????
        }
        final List<Object> names = new ArrayList<>();

        for (final String name: table.getMandatoryAndPrimaryKeyColumns()) {
          if (!set.contains(name)) {
            names.add(name);
          }
        }
        names.addAll(columnNames);
        Utils.joinAndAppendNames(builder, ",", names);
      }
    }
    builder.append(" FROM ");
    try {
      if (getKeyspace() != null) {
        Utils.appendName(getKeyspace(), builder).append(".");
      }
    } catch (ExcludedSuffixKeyException e) { // just skip this one since we were asked to skip the current keyspace suffix
      return null;
    }
    Utils.appendName(table.getName(), builder);
    if (!where.clauses.isEmpty()) {
      builder.append(" WHERE ");
      Utils.joinAndAppend(table, builder, " AND ", where.getClauses(table));
    }
    if (orderings != null) {
      builder.append(" ORDER BY ");
      Utils.joinAndAppend(table, builder, ",", orderings);
    }
    if (limit > 0) {
      builder.append(" LIMIT ").append(limit);
    }
    if (allowFiltering) {
      builder.append(" ALLOW FILTERING");
    }
    return builder;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.impl.StatementImpl#clearKeyspace()
   */
  @Override
  protected void clearKeyspace() {
    super.clearKeyspace();
    this.keyspace = null;
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
    this.keyspace = null;
    this.statements = null;
  }

  /**
   * Gets the keyspace name associated with this context.
   *
   * @author paouelle
   *
   * @return the non-<code>null</code> keyspace name associated with this
   *         context
   * @throws IllegalArgumentException if unable to compute the keyspace name
   *         based on provided suffixes
   */
  @SuppressWarnings({"synthetic-access", "cast", "unchecked", "rawtypes"})
  @Override
  public String getKeyspace() {
    if (suffixes == null) {
      return super.getKeyspace();
    }
    if (keyspace == null) {
      // if we get here then the query was done for suffixes with an IN clause
      // in such case, we must generate one query for each combinations
      // and aggregate the results
      final List<String> snames = new ArrayList<>(suffixes.keySet());
      final CombinationIterator<Object> ci = new CombinationIterator<>(
        Object.class, (Collection<Collection<Object>>)(Collection)suffixes.values()
      );
      final List<String> keyspaces = new ArrayList<>(ci.size());

      while (ci.hasNext()) {
        final List<Object> svalues = ci.next();
        // create a new select statement as a dup of this one but with
        // the suffixes from the current combination
        final SelectImpl s = new SelectImpl(this);

        for (int j = 0; j < snames.size(); j++) {
          s.getContext().addSuffix(snames.get(j), svalues.get(j));
        }
        keyspaces.add(s.getKeyspace());
      }
      this.keyspace = "(" + String.join("|", keyspaces) + ")";
    }
    return keyspace;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.impl.StatementImpl#executeAsync()
   */
  @Override
  @SuppressWarnings("unchecked")
  public ObjectSetFuture<T> executeAsync() {
    if (suffixes == null) {
      return super.executeAsync();
    }
    return new CompoundObjectSetFuture<>(
      getContext(),
      statements()
        .map(s -> s.executeAsync())
        .collect(Collectors.toList())
    );
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.impl.StatementImpl#executeAsyncRaw()
   */
  @Override
  public ResultSetFuture executeAsyncRaw() {
    if (suffixes == null) {
      return super.executeAsyncRaw();
    }
    return new CompoundResultSetFuture(
      statements()
        .map(s -> s.executeAsyncRaw())
        .filter(s -> !(s instanceof EmptyResultSetFuture))
        .collect(Collectors.toList()),
      mgr
    );
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.Select#where(com.github.helenusdriver.driver.Clause)
   */
  @Override
  public Where<T> where(Clause clause) {
    return where.and(clause);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.Select#where()
   */
  @Override
  public Where<T> where() {
    return where;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.Select#orderBy(com.github.helenusdriver.driver.Ordering[])
   */
  @Override
  public Select<T> orderBy(Ordering... orderings) {
    org.apache.commons.lang3.Validate.validState(
      this.orderings == null,
      "an ORDER BY clause has already been provided"
    );
    this.orderings = new ArrayList<>(orderings.length);
    for (final Ordering o: orderings) {
      org.apache.commons.lang3.Validate.isTrue(
        o instanceof OrderingImpl,
        "unsupported class of orderings: %s",
        o.getClass().getName()
      );
      final OrderingImpl oi = (OrderingImpl)o;

      oi.validate(table);
      // check the referenced column is a multi-key in which case we actually
      // need to treat this ordering request as if it was for the special
      // multi-key column instead
      if (table.isMultiKey(oi.getColumnName())) {
        this.orderings.add(
          new OrderingImpl(StatementImpl.MK_PREFIX + oi.getColumnName(), oi.isDescending())
        );
      } else {
        this.orderings.add(oi);
      }
    }
    setDirty();
    return this;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.Select#limit(int)
   */
  @Override
  public Select<T> limit(int limit) {
    org.apache.commons.lang3.Validate.validState(
      this.limit <= 0,
      "a LIMIT value has already been provided"
    );
    org.apache.commons.lang3.Validate.isTrue(
      limit > 0,
      "invalid negative LIMIT: %s",
      limit
    );
    this.limit = limit;
    setDirty();
    return this;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.Select#allowFiltering()
   */
  @Override
  public Select<T> allowFiltering() {
    this.allowFiltering = true;
    return this;
  }

  /**
   * The <code>WhereImpl</code> class defines a WHERE clause for a SELECT
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
  public static class WhereImpl<T>
    extends ForwardingStatementImpl<ObjectSet<T>, ObjectSetFuture<T>, T, SelectImpl<T>>
    implements Where<T> {
    /**
     * Holds the list of clauses for this statement
     *
     * @author paouelle
     */
    private final List<ClauseImpl> clauses = new ArrayList<>(10);

    /**
     * Instantiates a new <code>WhereImpl</code> object.
     *
     * @author paouelle
     *
     * @param statement the encapsulated statement
     */
    WhereImpl(SelectImpl<T> statement) {
      super(statement);
    }

    /**
     * Instantiates a new <code>WhereImpl</code> object.
     *
     * @author paouelle
     *
     * @param w the non-<code>null</code> where to copy
     * @param statement the encapsulated statement
     */
    WhereImpl(WhereImpl<T> w, SelectImpl<T> statement) {
      super(statement);
      clauses.addAll(w.clauses);
    }

    /**
     * Gets the "where" clauses while adding missing final partition keys.
     *
     * @author paouelle
     *
     * @param  table the table for which to get the "where" clauses
     * @return the "where" clauses
     */
    private List<ClauseImpl> getClauses(TableInfoImpl<T> table) {
      // check if the table defines any final primary keys
      // in which case we want to make sure to add clauses for them too
      // and also check for multi-keys as we need to convert the provided value
      // (if a set of size 1) into an EQ
      final List<ClauseImpl> clauses = new ArrayList<>(this.clauses);

      if (!table.getMultiKeys().isEmpty()) {
        // need to see if we have multi-keys in the list and if
        // so, we need to convert its value (if a set) into an "EQ"
        for (final ListIterator<ClauseImpl> i = clauses.listIterator(); i.hasNext(); ) {
          final ClauseImpl clause = i.next();

          if (table.isMultiKey(clause.getColumnName())) {
            final Set<Object> in = new LinkedHashSet<>(8); // preserve order

            for (final Object v: clause.values()) {
              if (v instanceof Set<?>) {
                for (final Object sv: (Set<?>)v) {
                  in.add(sv);
                }
              } else {
                in.add(v);
              }
            }
            // NOTE: Cassandra doesn't support using an 'IN' for a primary key
            // if as part of the columns selected, one is a collection. In our
            // case, the multi-key column is itself a collection as such, we will
            // never be able to support the 'IN' clause for that column
            if (in.size() == 1) {
              i.set(new ClauseImpl.EqClauseImpl(
                StatementImpl.MK_PREFIX + clause.getColumnName(), in.iterator().next()
              ));
            } else {
              throw new IllegalArgumentException(
                "unsupported selection of multiple values for multi-key column '"
                + clause.getColumnName()
                + "'"
               );
            }
          }
        }
      }
      for (final Map.Entry<String, Object> e: table.getFinalPrimaryKeyValues().entrySet()) {
        final String name = e.getKey();

        // check if we already have a clause for that column
        boolean found = false;

        for (final ClauseImpl c: this.clauses) {
          if (name.equals(c.getColumnName().toString())) {
            found = true;
            break;
          }
        }
        if (!found) {
          clauses.add(new ClauseImpl.EqClauseImpl(name, e.getValue()));
        }
      }
      return clauses;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.github.helenusdriver.driver.Select.Where#and(com.github.helenusdriver.driver.Clause)
     */
    @SuppressWarnings("synthetic-access")
    @Override
    public Where<T> and(Clause clause) {
      org.apache.commons.lang3.Validate.notNull(clause, "invalid null clause");
      org.apache.commons.lang3.Validate.isTrue(
        clause instanceof ClauseImpl,
        "unsupported class of clauses: %s",
        clause.getClass().getName()
      );
      org.apache.commons.lang3.Validate.isTrue(
        !(clause instanceof ClauseImpl.DelayedWithObject),
        "unsupported clause '%s' for a SELECT statement",
        clause
      );
      if (clause instanceof ClauseImpl.Delayed) {
        for (final Clause c: ((ClauseImpl.Delayed)clause).processWith(statement.table)) {
          and(c); // recurse to add the processed clause
        }
      } else {
        final ClauseImpl c = (ClauseImpl)clause;
        boolean add = true;

        if (c instanceof Clause.Equality) {
          statement.table.validateSuffixKeyOrPrimaryKeyOrIndexColumn(c.getColumnName());
          if (statement.getContext().getClassInfo().isSuffixKey(c.getColumnName().toString())) {
            statement.getContext().addSuffix(c.getColumnName().toString(), c.firstValue());
            // only add if it is a column too
            add = statement.table.hasColumn(c.getColumnName());
          }
          c.validate(statement.table);
        } else if (c instanceof Clause.In) {
          statement.table.validateSuffixKeyOrPrimaryKeyOrIndexColumn(c.getColumnName());
          if (statement.getContext().getClassInfo().isSuffixKey(c.getColumnName().toString())) {
            // verify all suffix values one after the other to validate all of them
            for (final Object v: c.values()) {
              statement.getContext().getClassInfo().validateSuffix(
                c.getColumnName().toString(), v
              );
            }
            // keep track of all suffixes so we can generate all the underlying
            // select statements later
            if (statement.suffixes == null) {
              statement.suffixes = new LinkedHashMap<>(6);
            }
            statement.suffixes.put(c.getColumnName().toString(), c.values());
            // only add if it is a column too
            add = statement.table.hasColumn(c.getColumnName());
            // don't validate the clause as we know it is not a valid one with
            // the collection of values from the in clause
            // but that is ok since we already validated them here
          } else {
            c.validate(statement.table);
          }
        } else {
          statement.table.validatePrimaryKeyOrIndexColumn(c.getColumnName());
          c.validate(statement.table);
        }
        if (add) {
          clauses.add(c);
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
     * @see com.github.helenusdriver.driver.Select.Where#orderBy(com.github.helenusdriver.driver.Ordering[])
     */
    @Override
    public Select<T> orderBy(Ordering... orderings) {
      return statement.orderBy(orderings);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.github.helenusdriver.driver.Select.Where#limit(int)
     */
    @Override
    public Select<T> limit(int limit) {
      return statement.limit(limit);
    }
  }

  /**
   * The <code>BuilderImpl</code> class defines an in-construction SELECT statement.
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
  public static class BuilderImpl<T> implements Builder<T> {
    /**
     * Holds the statement manager.
     *
     * @author paouelle
     */
    protected final StatementManagerImpl mgr;

    /**
     * Holds the statement bridge.
     *
     * @author paouelle
     */
    protected final StatementBridge bridge;

    /**
     * Holds the context associated with the POJO class for this statement.
     *
     * @author paouelle
     */
    protected final ClassInfoImpl<T>.Context context;

    /**
     * List of columns added.
     *
     * @author paouelle
     */
    protected List<Object> columnNames;

    /**
     * Flag to keep track of whether or not the special COUNT() or * has been
     * selected.
     *
     * @author paouelle
     */
    protected boolean countOrAllSelected = false;

    /**
     * Instantiates a new <code>BuilderImpl</code> object.
     *
     * @author paouelle
     *
     * @param context the non-<code>null</code> class info context for the POJO
     *        associated with this statement
     * @param mgr the non-<code>null</code> statement manager
     * @param bridge the non-<code>null</code> statement bridge
     */
    BuilderImpl(
      ClassInfoImpl<T>.Context context,
      StatementManagerImpl mgr,
      StatementBridge bridge
    ) {
      this.mgr = mgr;
      this.bridge = bridge;
      this.context = context;
    }

    /**
     * Instantiates a new <code>BuilderImpl</code> object.
     *
     * @author paouelle
     *
     * @param  context the non-<code>null</code> class info context for the POJO
     *         associated with this statement
     * @param  columnNames the column names
     * @param  mgr the non-<code>null</code> statement manager
     * @param  bridge the non-<code>null</code> statement bridge
     * @throws NullPointerException if <code>context</code> is <code>null</code>
     * @throws IllegalArgumentException if any of the specified columns are not
     *         defined by the POJO
     */
    BuilderImpl(
      ClassInfoImpl<T>.Context context,
      List<Object> columnNames,
      StatementManagerImpl mgr,
      StatementBridge bridge
    ) {
      this(context, mgr, bridge);
      context.getClassInfo().validateColumns(columnNames);
      this.columnNames = columnNames;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.github.helenusdriver.driver.Select.Builder#from(java.lang.String)
     */
    @Override
    public Select<T> from(String table) {
      return new SelectImpl<>(
        context, table, columnNames, countOrAllSelected, mgr, bridge
      );
    }
  }

  /**
   * The <code>SelectionImpl</code> class defines a selection clause for an
   * in-construction SELECT statement.
   *
   * @copyright 2015-2015 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 19, 2015 - paouelle - Creation
   *
   * @param <T> The type of POJO associated with the response from this statement.
   *
   * @since 1.0
   */
  public static class SelectionImpl<T>
    extends BuilderImpl<T>
    implements Selection<T> {
    /**
     * Instantiates a new <code>SelectionImpl</code> object.
     *
     * @author paouelle
     *
     * @param context the non-<code>null</code> class info context for the POJO
     *        associated with this statement
     * @param mgr the non-<code>null</code> statement manager
     * @param bridge the non-<code>null</code> statement bridge
     */
    SelectionImpl(
      ClassInfoImpl<T>.Context context,
      StatementManagerImpl mgr,
      StatementBridge bridge
    ) {
      super(context, mgr, bridge);
    }

    /**
     * Adds the specified column name.
     *
     * @author paouelle
     *
     * @param  name the non-<code>null</code> column name to be added
     * @return this for chaining
     * @throws NullPointerException if <code>name</code> is <code>null</code>
     * @throws IllegalArgumentException if any of the specified columns are not defined
     *         by the POJO
     */
    private Selection<T> addName(Object name) {
      context.getClassInfo().validateColumn(name);
      if (columnNames == null) {
        super.columnNames = new ArrayList<>(25);
      }
      columnNames.add(name);
      return this;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.github.helenusdriver.driver.Select.Selection#all()
     */
    @Override
    public Builder<T> all() {
      org.apache.commons.lang3.Validate.validState(
        columnNames == null,
        "some columns (%s) have already been selected",
        StringUtils.join(columnNames, ", ")
      );
      super.countOrAllSelected = true;
      return this;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.github.helenusdriver.driver.Select.Selection#countAll()
     */
    @Override
    public Builder<T> countAll() {
      org.apache.commons.lang3.Validate.validState(
        columnNames == null,
        "some columns (%s) have already been selected",
        StringUtils.join(columnNames, ", ")
      );
      super.columnNames = SelectImpl.COUNT_ALL;
      super.countOrAllSelected = true;
      return this;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.github.helenusdriver.driver.Select.Selection#column(java.lang.Object)
     */
    @Override
    public Selection<T> column(Object name) {
      return addName(name);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.github.helenusdriver.driver.Select.Selection#writeTime(java.lang.String)
     */
    @Override
    public Selection<T> writeTime(String name) {
      return addName(new Utils.FCall("writetime", new Utils.CName(name)));
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.github.helenusdriver.driver.Select.Selection#ttl(java.lang.String)
     */
    @Override
    public Selection<T> ttl(String name) {
      return addName(new Utils.FCall("ttl", new Utils.CName(name)));
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.github.helenusdriver.driver.Select.Selection#fcall(java.lang.String, java.lang.Object[])
     */
    @Override
    public Selection<T> fcall(String name, Object... parameters) {
      return addName(new Utils.FCall(name, parameters));
    }
  }
}

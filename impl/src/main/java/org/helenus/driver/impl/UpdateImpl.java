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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import org.helenus.commons.collections.iterators.CombinationIterator;
import org.helenus.driver.Assignment;
import org.helenus.driver.Clause;
import org.helenus.driver.ColumnPersistenceException;
import org.helenus.driver.StatementBridge;
import org.helenus.driver.StatementBuilder;
import org.helenus.driver.Update;
import org.helenus.driver.UpdateNotAppliedException;
import org.helenus.driver.Using;
import org.helenus.driver.VoidFuture;
import org.helenus.driver.impl.AssignmentImpl.CounterAssignmentImpl;
import org.helenus.driver.info.TableInfo;

/**
 * The <code>UpdateImpl</code> class extends the functionality of Cassandra's
 * {@link com.datastax.driver.core.querybuilder.Update} class to provide support
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
public class UpdateImpl<T>
  extends StatementImpl<Void, VoidFuture, T>
  implements Update<T> {
  /**
   * List of tables to be updated.
   *
   * @author paouelle
   */
  private final List<TableInfoImpl<T>> tables = new ArrayList<>(8);

  /**
   * Holds the assignments for this statement.
   *
   * @author paouelle
   */
  private final AssignmentsImpl<T> assignments;

  /**
   * Holds the where statement part.
   *
   * @author paouelle
   */
  private final WhereImpl<T> where;

  /**
   * Holds the "USING" options.
   *
   * @author paouelle
   */
  private final OptionsImpl<T> usings;

  /**
   * Holds the conditions for the update statement.
   *
   * @author paouelle
   */
  private final ConditionsImpl<T> conditions;

  /**
   * Flag indicating if the "IF EXISTS" option has been selected.
   *
   * @author paouelle
   */
  private volatile boolean ifExists;

  /**
   * Instantiates a new <code>UpdateImpl</code> object.
   *
   * @author paouelle
   *
   * @param  context the non-<code>null</code> class info context for the POJO
   *         associated with this statement
   * @param  mgr the non-<code>null</code> statement manager
   * @param  bridge the non-<code>null</code> statement bridge
   * @throws NullPointerException if <code>context</code> is <code>null</code>
   * @throws IllegalArgumentException if unable to compute the keyspace or table
   *         names based from the given object
   */
  public UpdateImpl(
    ClassInfoImpl<T>.POJOContext context,
    StatementManagerImpl mgr,
    StatementBridge bridge
  ) {
    this(context, null, mgr, bridge);
  }

  /**
   * Instantiates a new <code>UpdateImpl</code> object.
   *
   * @author paouelle
   *
   * @param  context the non-<code>null</code> class info context for the POJO
   *         associated with this statement
   * @param  tables the tables to update
   * @param  mgr the non-<code>null</code> statement manager
   * @param  bridge the non-<code>null</code> statement bridge
   * @throws NullPointerException if <code>context</code> is <code>null</code>
   * @throws IllegalArgumentException if any of the specified tables are not
   *         defined in the POJO
   */
  public UpdateImpl(
    ClassInfoImpl<T>.POJOContext context,
    String[] tables,
    StatementManagerImpl mgr,
    StatementBridge bridge
  ) {
    super(Void.class, context, mgr, bridge);
    if (tables != null) {
      for (final String table: tables) {
        if (table != null) {
          this.tables.add((TableInfoImpl<T>)context.getClassInfo().getTable(table)); // will throw IAE
        } // else - skip
      }
    } else { // fallback to all
      this.tables.addAll(context.getClassInfo().getTablesImpl());
    }
    this.assignments = new AssignmentsImpl<>(this);
    this.where = new WhereImpl<>(this);
    this.usings = new OptionsImpl<>(this);
    this.conditions = new ConditionsImpl<>(this);
  }

  /**
   * Ands the specified assignment along with others while validating them.
   * <p>
   * <i>Note:</i> Assignments referencing columns that are not defined in the
   * specified table are simply ignored.
   *
   * @author paouelle
   *
   * @param  table the non-<code>null</code> table for which to validate or
   *         extract column's values for
   * @param  assignments the non-<code>null</code> list of assignments to which
   *         to and the assignment
   * @param  assignment the non-<code>null</code> assignment to and together
   *         with others
   * @throws IllegalArgumentException if assignments reference invalid values
   *         or if missing mandatory columns are referenced for the specified
   *         table
   * @throws ColumnPersistenceException if unable to persist a column's value
   */
  private void andAssignment(
    TableInfoImpl<T> table,
    List<AssignmentImpl> assignments,
    AssignmentImpl assignment
  ) {
    // checked if the assignment is a delayed one in which case we need to
    // process it with the POJO and continue with the resulting list of
    // assignments instead of it
    if (assignment instanceof AssignmentImpl.DelayedWithObject) {
      for (final AssignmentImpl a: ((AssignmentImpl.DelayedWithObject)assignment).processWith(table, getPOJOContext())) {
        andAssignment(table, assignments, a); // recurse to add the processed assignment
      }
    } else {
      // only pay attention if the referenced column name is defined in the table
      if (table.hasColumn(assignment.getColumnName())) {
        try {
          assignment.validate(table);
        } catch (EmptyOptionalPrimaryKeyException e) {
          if (assignment instanceof AssignmentImpl.ReplaceAssignmentImpl) {
            // special case for replace assignment as we will need to potentially
            // delete the old row if old was not null or add only a new row if
            // old was null but not new
            // so fall through in all cases and let the update take care of it
          } else {
            throw e;
          }
        }
        assignments.add(assignment);
      }
    }
  }

  /**
   * Ands the specified clause along with others while validating them.
   * <p>
   * <i>Note:</i> Clauses referencing columns that are not defined in the
   * specified table are simply ignored.
   *
   * @author paouelle
   *
   * @param  table the non-<code>null</code> table for which to validate or
   *         extract column's values for
   * @param  clauses the non-<code>null</code> map of clauses to which
   *         to and the clause keyed by the column name
   * @param  clause the non-<code>null</code> assignment to and together
   *         with others
   * @throws IllegalArgumentException if clauses reference columns which are
   *         not primary keys or index columns in the POJO for the specified
   *         table or reference invalid values
   */
  private void andClause(
    TableInfoImpl<T> table, Map<String, ClauseImpl> clauses, ClauseImpl clause
  ) {
    // checked if the assignment is a delayed one in which case we need to
    // process it with the POJO and continue with the resulting list of
    // assignments instead of it
    if (clause instanceof ClauseImpl.Delayed) {
      for (final ClauseImpl c: ((ClauseImpl.Delayed)clause).processWith(table)) {
        andClause(table, clauses, c); // recurse to add the processed clause
      }
    } else if (clause instanceof ClauseImpl.DelayedWithObject) {
      for (final ClauseImpl c: ((ClauseImpl.DelayedWithObject)clause).processWith(
        table, getPOJOContext()
      )) {
        andClause(table, clauses, c); // recurse to add the processed clause
      }
    } else {
      // only pay attention if the referenced column name is defined in the table
      // as a primary key
      if (table.hasPrimaryKey(clause.getColumnName())) {
        clause.validate(table);
        clauses.put(clause.getColumnName().toString(), clause);
      }
    }
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
   *         and cannot be computed with the provided suffixes yet or if
   *         assignments reference columns not defined in the POJO or invalid
   *         values or if missing mandatory columns are referenced for the
   *         specified table
   * @throws ColumnPersistenceException if unable to persist a column's value
   */
  @SuppressWarnings("synthetic-access")
  private void buildQueryStrings(
    TableInfoImpl<T> table, List<StringBuilder> builders
  ) {
    final StringBuilder builder = new StringBuilder();

    builder.append("UPDATE ");
    if (getKeyspace() != null) {
      Utils.appendName(getKeyspace(), builder).append(".");
    }
    Utils.appendName(table.getName(), builder);
    if (!usings.usings.isEmpty()) {
      builder.append(" USING ");
      Utils.joinAndAppend(table, builder, " AND ", usings.usings);
    }
    final Collection<FieldInfoImpl<T>> multiKeys = table.getMultiKeys();
    // TBR: final String mkname = (multiKey != null) ? multiKey.getColumnName() : null;
    AssignmentsImpl<T> assignments = this.assignments;

    if (assignments.isEmpty()) {
      // if it is empty then fallback to all non-primary columns
      assignments = (AssignmentsImpl<T>)new AssignmentsImpl<>(this).and(
        new AssignmentImpl.DelayedSetAllAssignmentImpl()
      );
      if (!multiKeys.isEmpty()) {
        // we need to add the key set for each multi-keys in the assignments as
        // it would not have been added by the DelayedSetAll since it is marked
        // as a key. However, in this context the real key is
        // the special "mk_<name>" one and not the set which is annotated as one
        for (final FieldInfoImpl<T> finfo: multiKeys) {
          assignments.assignments.add(new AssignmentImpl.DelayedSetAssignmentImpl(
            finfo.getColumnName())
          );
        }
      }
    }
    if (!assignments.assignments.isEmpty()) {
      // let's first preprocess and validate the assignments for this table
      final List<AssignmentImpl> as = new ArrayList<>(assignments.assignments.size());

      for (final AssignmentImpl a: assignments.assignments) {
        if (assignments.hasAllFromObject()) {
          if (a instanceof AssignmentImpl.DelayedSetAssignmentImpl) {
            if (((AssignmentImpl.DelayedSetAssignmentImpl)a).object == null) {
              // skip it as we are already adding all from the object anyway
              continue;
            }
          }
        }
        andAssignment(table, as, a);
      }
      if (as.isEmpty()) { // nothing to set for this table so skip
        return;
      }
      // if we detect that a primary key is part of the SET, then we can only
      // do a full INSERT of the whole POJO as we are actually creating a whole
      // new row in the table. It will be up to the user to delete any old one
      // as we have no clue what they were before since the POJO only keep track
      // of the new ones, however, we will verify if the value has changed
      // compared to the one reported by the object; if it has then we will first
      // generate a delete and then follow by the full insert
      boolean foundPK = false;
      Map<String, Object> old_values = null;

      for (final AssignmentImpl a: as) {
        if (table.getPrimaryKey(a.getColumnName()) != null) { // assigning to a primary key
          foundPK = true;
          if (a instanceof AssignmentImpl.ReplaceAssignmentImpl) { // great, we know what was the old one!
            final AssignmentImpl.ReplaceAssignmentImpl ra = (AssignmentImpl.ReplaceAssignmentImpl)a;

            if (old_values == null) {
              old_values = new LinkedHashMap<>(table.getPrimaryKeys().size());
            }
            final Object oldval = ra.getOldValue();

            // check if this assignment is for a multi-column and if it is then we
            // only want to keep the entries in the set that are not present in
            // in the new multi key to avoid having DELETE generate for these
            // keys which are supposed to remain in place
            if (!multiKeys.isEmpty()) {
              final FieldInfoImpl<?> f = table.getColumnImpl(a.getColumnName());

              if (f.isMultiKey()) {
                ((Set<?>)oldval).removeAll((Set<?>)(ra.getValue()));
              }
            }
            old_values.put(a.getColumnName().toString(), oldval);
          }
        }
      }
      if (foundPK) {
        if (old_values != null) {
          // primary key has changed so we need to first do a delete in this
          // table which will be followed by a brand new insert
          new DeleteImpl<>(
            getPOJOContext(),
            new String[] { table.getName() },
            null,
            true,
            old_values, // pass our old values as override for the primary keys in the POJO
            mgr,
            bridge
          ).buildQueryString(table, builders);
        }
        // time to shift gears to a full insert in which case we must rely
        // on the whole POJO as the assignments might not be complete
        new InsertImpl<>(getPOJOContext(), new String[] { table.getName() }, mgr, bridge)
          .buildQueryStrings(table, builders);
        return;
      }
      builder.append(" SET ");
      // make sure we do not add any duplicates
      Utils.joinAndAppendWithNoDuplicates(table, builder, ",", as);
    } else { // nothing to set for this table
      return;
    }
    if (!where.clauses.isEmpty()) {
      // let's first preprocess and validate the clauses for this table
      final List<ClauseImpl> whereClauses = where.getClauses(table);
      final Map<String, ClauseImpl> cs = new LinkedHashMap<>(whereClauses.size()); // preserver order

      for (final ClauseImpl c: whereClauses) {
        andClause(table, cs, c);
      }
      if (cs.isEmpty()) { // nothing to select for this update so skip
        return;
      }
      builder.append(" WHERE ");
      if (!multiKeys.isEmpty()) {
        // prepare all sets of values for all multi-keys present in the clause
        final List<Collection<ClauseImpl.EqClauseImpl>> sets = new ArrayList<>(multiKeys.size());

        for (final FieldInfoImpl<T> finfo: multiKeys) {
          final ClauseImpl mkc = cs.remove(finfo.getColumnName());

          if (mkc != null) { // we have a clause for this multi-key column
            final List<ClauseImpl.EqClauseImpl> set = new ArrayList<>();

            for (final Object v: mkc.values()) {
              if (v instanceof Set<?>) {
                for (final Object sv: (Set<?>)v) {
                  set.add(new ClauseImpl.EqClauseImpl(
                    StatementImpl.MK_PREFIX + finfo.getColumnName(), sv
                  ));
                }
              } else {
                set.add(new ClauseImpl.EqClauseImpl(
                  StatementImpl.MK_PREFIX + finfo.getColumnName(), v
                ));
              }
            }
            if (!set.isEmpty()) {
              sets.add(set);
            }
          }
        }
        if (!sets.isEmpty()) {
          // now iterate all combination of these sets to generate update statements
          // for each combination
          @SuppressWarnings("unchecked")
          final Collection<ClauseImpl.EqClauseImpl>[] asets = new Collection[sets.size()];

          for (final Iterator<List<ClauseImpl.EqClauseImpl>> i = new CombinationIterator<>(ClauseImpl.EqClauseImpl.class, sets.toArray(asets)); i.hasNext(); ) {
            // add the multi-key clause values from this combination to the map of clauses
            for (final ClauseImpl.EqClauseImpl kc: i.next()) {
              cs.put(kc.getColumnName().toString(), kc);
            }
            final StringBuilder sb = new StringBuilder(builder);

            Utils.joinAndAppend(table, sb, " AND ", cs.values());
            builders.add(finishBuildingQueryString(table, sb));
          }
          return;
        }
      }
      // we didn't have any multi-keys in the clauses so just update it based
      // on the given clause
      Utils.joinAndAppend(table, builder, " AND ", cs.values());
    } else {
      // add where clauses for all primary key columns
      try {
        final Map<String, Object> pkeys
          = getPOJOContext().getPrimaryKeyColumnValues(table.getName());

        if (!pkeys.isEmpty()) {
          builder.append(" WHERE ");
          if (!multiKeys.isEmpty()) {
            // prepare all sets of values for all multi-keys present in the clause
            final List<String> cnames = new ArrayList<>(multiKeys.size());
            final List<Collection<Object>> sets = new ArrayList<>(multiKeys.size());

            for (final FieldInfoImpl<T> finfo: multiKeys) {
              @SuppressWarnings("unchecked")
              final Set<Object> set = (Set<Object>)pkeys.remove(finfo.getColumnName());

              if (set != null) { // we have keys for this multi-key column
                cnames.add(finfo.getColumnName());
                sets.add(set);
              }
            }
            if (!sets.isEmpty()) {
              // now iterate all combination of these sets to generate update statements
              // for each combination
              @SuppressWarnings("unchecked")
              final Collection<Object>[] asets = new Collection[sets.size()];

              for (final Iterator<List<Object>> i = new CombinationIterator<>(Object.class, sets.toArray(asets)); i.hasNext(); ) {
                // add the multi-key clause values from this combination to the map of primary keys
                int j = -1;
                for (final Object k: i.next()) {
                  pkeys.put(StatementImpl.MK_PREFIX + cnames.get(++j), k);
                }
                final StringBuilder sb = new StringBuilder(builder);

                Utils.joinAndAppendNamesAndValues(sb, " AND ", "=", pkeys);
                builders.add(finishBuildingQueryString(table, sb));
              }
              return;
            }
          }
          // we didn't have any multi-keys in the list (unlikely) so just update it
          // based on the provided list
          Utils.joinAndAppendNamesAndValues(builder, " AND ", "=", pkeys);
        }
      } catch (EmptyOptionalPrimaryKeyException e) {
        // ignore and continue without updating this table
        return;
      }
    }
    builders.add(finishBuildingQueryString(table, builder));
  }

  /**
   * Finishes building a query string for the specified table.
   *
   * @author paouelle
   *
   * @param  table the non-<code>null</code> table for which to build a query
   *         string
   * @param  builder the non-<code>null</code> builder where to add the rest of
   *         the query string to build
   * @return <code>builder</code>
   * @throws ColumnPersistenceException if unable to persist a column's value
   */
  @SuppressWarnings("synthetic-access")
  private StringBuilder finishBuildingQueryString(
    TableInfoImpl<T> table, StringBuilder builder
  ) {
    if (ifExists) {
      builder.append(" IF EXISTS");
    } else if (!conditions.conditions.isEmpty()) {
      // TODO: we need to also filter based on this table as there might not be any condition to set
      // let's first validate the condition for this table
      for (final ClauseImpl c: conditions.conditions) {
        c.validate(table);
      }
      builder.append(" IF ");
      Utils.joinAndAppend(table, builder, " AND ", conditions.conditions);
    }
    return builder;
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
   * @see org.helenus.driver.Update#tables()
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
   * @see org.helenus.driver.Update#ifExists()
   */
  @Override
  public Update<T> ifExists() {
    this.ifExists = true;
    setDirty();
    return this;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.Update#with(org.helenus.driver.Assignment[])
   */
  @Override
  public Assignments<T> with(Assignment... assignments) {
    return this.assignments.and(assignments);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.Update#with()
   */
  @Override
  public Assignments<T> with() {
    return assignments;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.Update#where(org.helenus.driver.Clause)
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
   * @see org.helenus.driver.Update#where()
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
   * @see org.helenus.driver.Update#onlyIf(org.helenus.driver.Clause)
   */
  @Override
  public Conditions<T> onlyIf(Clause condition) {
    return conditions.and(condition);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.Update#onlyIf()
   */
  @Override
  public Conditions<T> onlyIf() {
    org.apache.commons.lang3.Validate.isTrue(
      !ifExists,
      "cannot combined additional conditions with IF EXISTS"
    );
    return conditions;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.Update#using(org.helenus.driver.Using)
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
   * @see org.helenus.driver.impl.StatementImpl#executeAsync0()
   */
  @SuppressWarnings("synthetic-access")
  @Override
  public VoidFuture executeAsync0() {
    // if we have no conditions then no need for special treatment of the response
    if (conditions.conditions.isEmpty()) {
      return super.executeAsync0();
    }
    return bridge.newVoidFuture(executeAsyncRaw0(), new VoidFuture.PostProcessor() {
      @Override
      public void postProcess(ResultSet result) {
        // update result set when using conditions have only one row
        // where the entry "[applied]" is a boolean indicating if the update was
        // successful and the rest are all the conditional values specified in
        // the UPDATE request

        // check if the condition was successful
        final Row row = result.one();

        if (row == null) {
          throw new UpdateNotAppliedException("no result row returned");
        }
        if (!row.getBool("[applied]")) {
          throw new UpdateNotAppliedException(row, "update not applied");
        }
        // else all good
      }
    });
  }

  /**
   * The <code>AssignmentsImpl</code> class defines the assignments of an UPDATE
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
  public static class AssignmentsImpl<T>
    extends ForwardingStatementImpl<Void, VoidFuture, T, UpdateImpl<T>>
    implements Assignments<T> {
    /**
     * Holds the assignments for the statement.
     *
     * @author paouelle
     */
    private final List<AssignmentImpl> assignments = new ArrayList<>(25);

    /**
     * Flag indicating if all columns from the objects are being added using the
     * special {@link StatementBuilder#setAllFromObject} assignment.
     *
     * @author paouelle
     */
    private boolean hasAllFromObject = false;

    /**
     * Instantiates a new <code>AssignmentsImpl</code> object.
     *
     * @author paouelle
     *
     * @param statement the encapsulated statement
     */
    AssignmentsImpl(UpdateImpl<T> statement) {
      super(statement);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.Update.Assignments#and(org.helenus.driver.Assignment[])
     */
    @Override
    public Assignments<T> and(Assignment... assignments) {
      org.apache.commons.lang3.Validate.notNull(
        assignments, "invalid null assignments"
      );
      for (final Assignment assignment: assignments) {
        org.apache.commons.lang3.Validate.notNull(
          assignment, "invalid null assignment"
        );
        org.apache.commons.lang3.Validate.isTrue(
          assignment instanceof AssignmentImpl,
          "unsupported class of assignments: %s",
          assignment.getClass().getName()
        );
        final AssignmentImpl a = (AssignmentImpl)assignment;

        statement.setCounterOp(a instanceof CounterAssignmentImpl);
        if (a instanceof AssignmentImpl.DelayedSetAllAssignmentImpl) {
          if (((AssignmentImpl.DelayedSetAllAssignmentImpl)a).object == null) {
            this.hasAllFromObject = true;
          }
        }
        if (!(a instanceof AssignmentImpl.DelayedWithObject)) {
          // pre-validate against any table
          getPOJOContext().getClassInfo().validateColumn(a.getColumnName().toString());
        }
        this.assignments.add(a);
        setDirty();
      }
      return this;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.Update.Assignments#isEmpty()
     */
    @Override
    public boolean isEmpty() {
      return assignments.isEmpty();
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.Update.Assignments#hasAllFromObject()
     */
    @Override
    public boolean hasAllFromObject() {
      return assignments.isEmpty() || hasAllFromObject;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.Update.Assignments#where(org.helenus.driver.Clause)
     */
    @Override
    public Where<T> where(Clause clause) {
      return statement.where(clause);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.Update.Assignments#using(org.helenus.driver.Using)
     */
    @Override
    public Options<T> using(Using using) {
      return statement.using(using);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.Update.Assignments#onlyIf(org.helenus.driver.Clause)
     */
    @Override
    public Conditions<T> onlyIf(Clause condition) {
      return statement.onlyIf(condition);
    }
  }

  /**
   * The <code>WhereImpl</code> class defines a WHERE clause for an UPDATE statement.
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
    extends ForwardingStatementImpl<Void, VoidFuture, T, UpdateImpl<T>>
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
     * @param statement the non-<code>null</code> statement to which this
     *        "WHERE" part belongs to
     */
    WhereImpl(UpdateImpl<T> statement) {
      super(statement);
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
      final List<ClauseImpl> clauses = new ArrayList<>(this.clauses);

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
     * @see org.helenus.driver.Update.Where#and(org.helenus.driver.Clause)
     */
    @Override
    public Where<T> and(Clause clause) {
      org.apache.commons.lang3.Validate.notNull(clause, "invalid null clause");
      org.apache.commons.lang3.Validate.isTrue(
        clause instanceof ClauseImpl,
        "unsupported class of clauses: %s",
        clause.getClass().getName()
      );
      final ClauseImpl c = (ClauseImpl)clause;

      if (!(c instanceof ClauseImpl.Delayed) && !(c instanceof ClauseImpl.DelayedWithObject)) {
        // pre-validate against any table
        getPOJOContext().getClassInfo().validateColumn(c.getColumnName().toString());
      }
      clauses.add(c);
      setDirty();
      return this;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.Update.Where#with(org.helenus.driver.Assignment[])
     */
    @Override
    public Assignments<T> with(Assignment... assignments) {
      return statement.with(assignments);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.Update.Where#using(org.helenus.driver.Using)
     */
    @Override
    public Options<T> using(Using using) {
      return statement.using(using);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.Update.Where#onlyIf(org.helenus.driver.Clause)
     */
    @Override
    public Conditions<T> onlyIf(Clause condition) {
      return statement.onlyIf(condition);
    }
  }

  /**
   * The <code>OptionsImpl</code> class defines the options of an UPDATE statement.
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
    extends ForwardingStatementImpl<Void, VoidFuture, T, UpdateImpl<T>>
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
    OptionsImpl(UpdateImpl<T> statement) {
      super(statement);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.Update.Options#and(org.helenus.driver.Using)
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
     * @see org.helenus.driver.Update.Options#with(org.helenus.driver.Assignment[])
     */
    @Override
    public Assignments<T> with(Assignment... assignments) {
      return statement.with(assignments);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.Update.Options#onlyIf(org.helenus.driver.Clause)
     */
    @Override
    public Conditions<T> onlyIf(Clause condition) {
      return statement.onlyIf(condition);
    }
  }

  /**
   * Conditions for an UDPATE statement.
   * <p>
   * When provided some conditions, an update will not apply unless the provided
   * conditions applies.
   * <p>
   * Please keep in mind that provided conditions has a non negligible
   * performance impact and should be avoided when possible.
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
  public static class ConditionsImpl<T>
    extends ForwardingStatementImpl<Void, VoidFuture, T, UpdateImpl<T>>
    implements Conditions<T> {
    /**
     * Holds the list of conditions for the statement
     *
     * @author paouelle
     */
    private final List<ClauseImpl> conditions = new ArrayList<>(10);

    /**
     * Instantiates a new <code>ConditionsImpl</code> object.
     *
     * @author paouelle
     *
     * @param statement the non-<code>null</code> statement for which we are
     *        creating conditions
     */
    ConditionsImpl(UpdateImpl<T> statement) {
      super(statement);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.Update.Conditions#and(org.helenus.driver.Clause)
     */
    @SuppressWarnings("synthetic-access")
    @Override
    public Conditions<T> and(Clause condition) {
      org.apache.commons.lang3.Validate.notNull(condition, "invalid null condition");
      org.apache.commons.lang3.Validate.isTrue(
        !statement.ifExists,
        "cannot combined additional conditions with IF EXISTS"
      );
      org.apache.commons.lang3.Validate.isTrue(
        condition instanceof ClauseImpl,
        "unsupported class of clauses: %s",
        condition.getClass().getName()
      );
      final ClauseImpl c = (ClauseImpl)condition;

      // just to be safe, validate anyway
      org.apache.commons.lang3.Validate.isTrue("=".equals(c.getOperation()), "unsupported condition: %s", c);
      // un-comment only if the column name had to be a primary key or an index
      // context.validatePrimaryKeyOrIndexColumn(c.name());
      // pre-validate against any table
      getPOJOContext().getClassInfo().validateColumn(c.getColumnName().toString());
      conditions.add(c);
      setDirty();
      return this;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.Update.Conditions#with(org.helenus.driver.Assignment[])
     */
    @Override
    public Assignments<T> with(Assignment... assignments) {
      return statement.with(assignments);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.Update.Conditions#where(org.helenus.driver.Clause)
     */
    @Override
    public Where<T> where(Clause clause) {
      return statement.where(clause);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.Update.Conditions#using(org.helenus.driver.Using)
     */
    @Override
    public Options<T> using(Using using) {
      return statement.using(using);
    }
  }
}

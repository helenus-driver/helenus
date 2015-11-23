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
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;

import org.helenus.driver.BindMarker;
import org.helenus.driver.Clause;
import org.helenus.driver.ColumnPersistenceException;

/**
 * The <code>ClauseImpl</code> class extends Cassandra's
 * {@link com.datastax.driver.core.querybuilder.Clause} to provide support for
 * POJOs.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 19, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
public abstract class ClauseImpl
  extends Utils.Appendeable
  implements Clause {
  /**
   * Holds the column name for this clause.
   *
   * @author paouelle
   */
  protected final CharSequence name;

  /**
   * Instantiates a new <code>ClauseImpl</code> object.
   *
   * @author paouelle
   *
   * @param  name the non-<code>null</code> column name for this clause
   * @throws NullPointerException if <code>name</code> is <code>null</code>
   */
  ClauseImpl(CharSequence name) {
    org.apache.commons.lang3.Validate.notNull(name, "invalid null column name");
    this.name = name;
  }

  /**
   * Gets the column name for this clause.
   *
   * @author paouelle
   *
   * @return the column name for this clause
   */
  CharSequence getColumnName() {
    return name;
  }

  /**
   * Gets the first value defined by this clause.
   *
   * @author paouelle
   *
   * @return the first value defined by this clause
   */
  Object firstValue() {
    return null;
  };

  /**
   * Gets all values defined by this clause.
   *
   * @author paouelle
   *
   * @return all values defined by this clause
   */
  Collection<?> values() {
    return null;
  }

  /**
   * Gets the operation for this condition.
   *
   * @author paouelle
   *
   * @return the non-<code>null</code> operation for this condition
   */
  abstract String getOperation();

  /**
   * Validates the clause using the specified table.
   *
   * @author paouelle
   *
   * @param <T> the type of POJO
   *
   * @param  table the non-<code>null</code> table to validate the clause with
   * @throws IllegalArgumentException if the clause references columns not defined
   *         by the POJO or invalid values
   */
  abstract <T> void validate(TableInfoImpl<T> table);

  /**
   * The <code>Delayed</code> interface is used by clauses that
   * do not have all their content defined at the time of creation but instead
   * at the time the clause is added to the statement.
   *
   * @copyright 2015-2015 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 19, 2015 - paouelle - Creation
   *
   * @since 1.0
   */
  interface Delayed {
    /**
     * Called at the time the clause is added to a statement to complete the
     * clause.
     *
     * @author paouelle
     *
     * @param  table the non-<code>null</code> table associated with
     *         the statement
     * @return a non-<code>null</code> list of new clause(s) corresponding to
     *         this one
     * @throws IllegalArgumentException if missing mandatory columns are processed
     */
    public <T> List<ClauseImpl> processWith(TableInfoImpl<T> table);

    /**
     * Called at the time the clause is added to a statement to complete the
     * clause by processing only the clause that references suffixes.
     *
     * @author paouelle
     *
     * @param  cinfo the non-<code>null</code> POJO class info associated with
     *         the statement
     * @return a non-<code>null</code> list of new clause(s) corresponding to
     *         this one
     * @throws IllegalArgumentException if missing mandatory suffixes are processed
     */
    public <T> List<ClauseImpl> processWith(ClassInfoImpl<T> cinfo);
  }

  /**
   * The <code>DelayedWithObject</code> interface is used by clauses that
   * do not have all their content defined at the time of creation but instead
   * at the time the clause is added to a statement initialized with a POJO.
   *
   * @copyright 2015-2015 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 19, 2015 - paouelle - Creation
   *
   * @since 1.0
   */
  interface DelayedWithObject {
    /**
     * Called at the time the clause is added to a statement to complete the
     * clause.
     *
     * @author paouelle
     *
     * @param  table the non-<code>null</code> table associated with
     *         the statement
     * @param  context the non-<code>null</code> POJO context associated with
     *         the statement
     * @return a non-<code>null</code> list of new clause(s) corresponding to
     *         this one
     * @throws IllegalArgumentException if missing mandatory columns are processed
     * @throws ColumnPersistenceException if unable to persist a column's value
     */
    public <T> List<ClauseImpl> processWith(
      TableInfoImpl<T> table, ClassInfoImpl<T>.POJOContext context
    );
  }

  /**
   * The <code>SimpleClauseImpl</code> class defines a clause specifying a column,
   * operator, and single value.
   *
   * @copyright 2015-2015 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 19, 2015 - paouelle - Creation
   *
   * @since 1.0
   */
  static class SimpleClauseImpl extends ClauseImpl {
    /**
     * Holds the operator for this clause.
     *
     * @author paouelle
     */
    private final String op;

    /**
     * Holds the value associated with this clause.
     *
     * @author paouelle
     */
    protected final Object value;

    /**
     * Instantiates a new <code>SimpleClauseImpl</code> object.
     *
     * @author paouelle
     *
     * @param  name the column name for this clause
     * @param  op the operator for this clause
     * @param  value the value for this clause
     * @throws NullPointerException if <code>name</code> or <code>op</code>
     *         is <code>null</code>
     */
    SimpleClauseImpl(CharSequence name, String op, Object value) {
      super(name);
      org.apache.commons.lang3.Validate.notNull(op, "invalid null operation");
      this.op = op;
      this.value = value;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.Utils.Appendeable#appendTo(org.helenus.driver.impl.TableInfoImpl, java.lang.StringBuilder)
     */
    @Override
    void appendTo(TableInfoImpl<?> tinfo, StringBuilder sb) {
      Utils.appendName(name, sb).append(op);
      FieldInfoImpl<?> finfo = tinfo.getColumnImpl(name);

      if (finfo == null) { // check if the field is the multi-key
        String sname = name.toString();

        if (sname.startsWith(StatementImpl.MK_PREFIX)) {
          sname = sname.substring(StatementImpl.MK_PREFIX.length()); // strip mk prefix
          finfo = tinfo.getColumnImpl(sname);
          if ((finfo != null) && finfo.isMultiKey()) {
            Utils.appendValue(finfo.encodeElementValue(value), sb);
            return;
          }
        }
        throw new IllegalStateException("unknown column '" + name + "'");
      }
      Utils.appendValue(finfo.encodeValue(value), sb);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClauseImpl#firstValue()
     */
    @Override
    Object firstValue() {
      return value;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClauseImpl#values()
     */
    @Override
    Collection<?> values() {
      return Collections.singleton(value);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClauseImpl#getOperation()
     */
    @Override
    String getOperation() {
      return op;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClauseImpl#validate(org.helenus.driver.impl.TableInfoImpl)
     */
    @Override
    <T> void validate(TableInfoImpl<T> table) {
      table.validateColumnAndValue(name, value);
    }
  }


  /**
   * The <code>SimpleClauseImpl</code> class defines a clause specifying a column,
   * operator, and single value.
   *
   * @copyright 2015-2015 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 19, 2015 - paouelle - Creation
   *
   * @since 1.0
   */
  static class EqClauseImpl
    extends SimpleClauseImpl
    implements Clause.Equality {
    /**
     * Instantiates a new <code>SimpleClauseImpl</code> object.
     *
     * @author paouelle
     *
     * @param  name the column name for this clause
     * @param  value the value for this clause
     * @throws NullPointerException if <code>name</code> is <code>null</code>
     */
    EqClauseImpl(CharSequence name, Object value) {
      super(name, "=", value);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClauseImpl#validate(org.helenus.driver.impl.TableInfoImpl)
     */
    @Override
    <T> void validate(TableInfoImpl<T> table) {
      table.validateSuffixOrColumnAndValue(name, value);
    }
  }

  /**
   * The <code>InClauseImpl</code> class defines the IN statement clause.
   *
   * @copyright 2015-2015 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 19, 2015 - paouelle - Creation
   *
   * @since 1.0
   */
  static class InClauseImpl extends ClauseImpl implements Clause.In {
    /**
     * Holds the values for this clause.
     *
     * @author paouelle
     */
    private final Collection<?> values;

    /**
     * Instantiates a new <code>InClauseImpl</code> object.
     *
     * @author paouelle
     *
     * @param  name the name of the column for this clause
     * @param  values the values for this clause
     * @throws NullPointerException if <code>name</code> is <code>null</code>
     * @throws IllegalArgumentException if <code>values</code> is empty
     */
    InClauseImpl(CharSequence name, Collection<?> values) {
      super(name);
      this.values = values;
      org.apache.commons.lang3.Validate.isTrue(
        !CollectionUtils.isEmpty(values),
        "missing values for IN clause"
      );
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.Utils.Appendeable#appendTo(org.helenus.driver.impl.TableInfoImpl, java.lang.StringBuilder)
     */
    @Override
    void appendTo(TableInfoImpl<?> tinfo, StringBuilder sb) {
      // We special case the case of just one bind marker because there is
      // little
      // reasons to do:
      // ... IN (?) ...
      // since in that case it's more elegant to use an equal. On the other
      // side,
      // it is a lot more useful to do:
      // ... IN ? ...
      // which binds the variable to the full list the IN is on.
      final Object fv = firstValue();

      if ((values.size() == 1)
          && ((fv instanceof BindMarker)
              || (fv instanceof com.datastax.driver.core.querybuilder.BindMarker))) {
        Utils.appendName(name, sb).append("IN ").append(fv);
        return;
      }
      Utils.appendName(name, sb).append(" IN (");
      FieldInfoImpl<?> finfo = tinfo.getColumnImpl(name);

      if (finfo == null) { // check if the name reference a multi-key
        String sname = name.toString();

        if (sname.startsWith(StatementImpl.MK_PREFIX)) {
          sname = sname.substring(StatementImpl.MK_PREFIX.length()); // strip mk prefix
          finfo = tinfo.getColumnImpl(sname);
          if ((finfo != null) && finfo.isMultiKey()) {
            if (finfo.isPersisted()) {
              final List<Object> pvals = new ArrayList<>(values.size());

              for (final Object val: values) {
                pvals.add(finfo.encodeElementValue(val));
              }
              Utils.joinAndAppendValues(sb, ",", pvals).append(")");
            } else {
              Utils.joinAndAppendValues(sb, ",", values).append(")");
            }
            return;
          }
        }
        throw new IllegalStateException("unknown column '" + name + "'");
      }
      if (finfo.isPersisted()) {
        final List<Object> pvals = new ArrayList<>(values.size());

        for (final Object val: values) {
          pvals.add(finfo.encodeValue(val));
        }
        Utils.joinAndAppendValues(sb, ",", pvals).append(")");
      } else {
        Utils.joinAndAppendValues(sb, ",", values).append(")");
      }
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClauseImpl#firstValue()
     */
    @Override
    Object firstValue() {
      return values.iterator().next();
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClauseImpl#values()
     */
    @Override
    Collection<?> values() {
      return values;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClauseImpl#getOperation()
     */
    @Override
    String getOperation() {
      return "IN";
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClauseImpl#validate(org.helenus.driver.impl.TableInfoImpl)
     */
    @Override
    <T> void validate(TableInfoImpl<T> table) {
      table.validateColumnAndValues(name, values);
    }
  }

  /**
   * The <code>IsClauseImpl</code> class defines a delayed clause that generates a
   * set of "equal" clauses for all primary key columns of the provided POJO
   * when added to a statement.
   *
   * @copyright 2015-2015 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 19, 2015 - paouelle - Creation
   *
   * @since 1.0
   */
  static class IsClauseImpl extends ClauseImpl implements Delayed {
    /**
     * Holds the POJO object from which to generate all "equal" clauses or
     * <code>null</code> if we should use the POJO provided with the UPDATE
     * statement.
     *
     * @author paouelle
     */
    protected final Object object;

    /**
     * Instantiates a new <code>IsClauseImpl</code> object.
     *
     * @author paouelle
     *
     * @param  object the POJO object
     * @throws NullPointerException if <code>object</code> is <code>null</code>
     */
    IsClauseImpl(Object object) {
      super("");
      org.apache.commons.lang3.Validate.notNull(object, "invalid null object");
      this.object = object;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClauseImpl.Delayed#processWith(org.helenus.driver.impl.TableInfoImpl)
     */
    @SuppressWarnings("unchecked")
    @Override
    public <T> List<ClauseImpl> processWith(TableInfoImpl<T> table) {
      org.apache.commons.lang3.Validate.isTrue(
        table.getObjectClass().isInstance(object),
        "invalid POJO class '"
        + object.getClass().getName()
        + "'; expecting '"
        + table.getObjectClass().getName()
        + "'"
      );
      final Collection<FieldInfoImpl<T>> keys = table.getSuffixAndPrimaryKeys();
      final List<ClauseImpl> clauses = new ArrayList<>(keys.size());

      for (final FieldInfoImpl<T> finfo: keys) {
        final String name = finfo.isColumn() ? finfo.getColumnName() : finfo.getSuffixKeyName();

        clauses.add(new ClauseImpl.EqClauseImpl(name, finfo.getValue((T)object)));
      }
      return clauses;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClauseImpl.Delayed#processWith(org.helenus.driver.impl.ClassInfoImpl)
     */
    @SuppressWarnings("unchecked")
    @Override
    public <T> List<ClauseImpl> processWith(ClassInfoImpl<T> cinfo) {
      org.apache.commons.lang3.Validate.isTrue(
        cinfo.getObjectClass().isInstance(object),
        "invalid POJO class '"
        + object.getClass().getName()
        + "'; expecting '"
        + cinfo.getObjectClass().getName()
        + "'"
      );
      final Collection<FieldInfoImpl<T>> keys = cinfo.getSuffixKeys().values();
      final List<ClauseImpl> clauses = new ArrayList<>(keys.size());

      for (final FieldInfoImpl<T> finfo: keys) {
        clauses.add(new ClauseImpl.EqClauseImpl(finfo.getSuffixKeyName(), finfo.getValue((T)object)));
      }
      return clauses;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClauseImpl#getOperation()
     */
    @Override
    String getOperation() {
      return "IS";
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.Utils.Appendeable#appendTo(org.helenus.driver.impl.TableInfoImpl, java.lang.StringBuilder)
     */
    @Override
    void appendTo(TableInfoImpl<?> tinfo, StringBuilder sb) {}

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClauseImpl#validate(org.helenus.driver.impl.TableInfoImpl)
     */
    @Override
    <T> void validate(TableInfoImpl<T> table) {
      throw new IllegalStateException("should not be called");
    }
  }

  /**
   * The <code>IsPartitionedLikeClauseImpl</code> class defines a delayed clause that
   * generates a set of "equal" clauses for all partition primary key columns
   * of the provided POJO when added to a statement.
   *
   * @copyright 2015-2015 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 19, 2015 - paouelle - Creation
   *
   * @since 1.0
   */
  static class IsPartitionedLikeClauseImpl extends IsClauseImpl {
    /**
     * Instantiates a new <code>IsClauseImpl</code> object.
     *
     * @author paouelle
     *
     * @param  object the POJO object
     * @throws NullPointerException if <code>object</code> is <code>null</code>
     */
    IsPartitionedLikeClauseImpl(Object object) {
      super(object);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClauseImpl.Delayed#processWith(org.helenus.driver.impl.TableInfoImpl)
     */
    @SuppressWarnings("unchecked")
    @Override
    public <T> List<ClauseImpl> processWith(TableInfoImpl<T> table) {
      org.apache.commons.lang3.Validate.isTrue(
        table.getObjectClass().isInstance(object),
        "invalid POJO class '"
        + object.getClass().getName()
        + "'; expecting '"
        + table.getObjectClass().getName()
        + "'"
      );
      final Collection<FieldInfoImpl<T>> keys = table.getSuffixAndPartitionKeys();
      final List<ClauseImpl> clauses = new ArrayList<>(keys.size());

      for (final FieldInfoImpl<T> finfo: keys) {
        final String name = finfo.isColumn() ? finfo.getColumnName() : finfo.getSuffixKeyName();

        clauses.add(new ClauseImpl.EqClauseImpl(name, finfo.getValue((T)object)));
      }
      return clauses;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClauseImpl#getOperation()
     */
    @Override
    String getOperation() {
      return "IS PARTITIONED LIKE";
    }
  }

  /**
   * The <code>IsSuffixedLikeClauseImpl</code> class defines a delayed clause that
   * generates a set of "equal" clauses for all suffix keys
   * of the provided POJO when added to a statement.
   *
   * @copyright 2015-2015 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 19, 2015 - paouelle - Creation
   *
   * @since 1.0
   */
  static class IsSuffixedLikeClauseImpl extends IsClauseImpl {
    /**
     * Instantiates a new <code>IsSuffixedLikeClauseImpl</code> object.
     *
     * @author paouelle
     *
     * @param  object the POJO object
     * @throws NullPointerException if <code>object</code> is <code>null</code>
     */
    IsSuffixedLikeClauseImpl(Object object) {
      super(object);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClauseImpl.Delayed#processWith(org.helenus.driver.impl.TableInfoImpl)
     */
    @SuppressWarnings("unchecked")
    @Override
    public <T> List<ClauseImpl> processWith(TableInfoImpl<T> table) {
      org.apache.commons.lang3.Validate.isTrue(
        table.getObjectClass().isInstance(object),
        "invalid POJO class '"
        + object.getClass().getName()
        + "'; expecting '"
        + table.getObjectClass().getName()
        + "'"
      );
      final Map<String, FieldInfoImpl<T>> keys = ((ClassInfoImpl<T>)table.getClassInfo()).getSuffixKeys();
      final List<ClauseImpl> clauses = new ArrayList<>(keys.size());

      for (final Map.Entry<String, FieldInfoImpl<T>> e: keys.entrySet()) {
        clauses.add(new ClauseImpl.EqClauseImpl(e.getKey(), e.getValue().getValue((T)object)));
      }
      return clauses;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClauseImpl#getOperation()
     */
    @Override
    String getOperation() {
      return "IS SUFFIXED LIKE";
    }
  }

  /**
   * The <code>IsObjectClauseImpl</code> class defines a delayed clause that generates a
   * set of "equal" clauses for all primary key columns of the POJO used when the
   * statement was initialized at the time it is added to a statement.
   *
   * @copyright 2015-2015 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 19, 2015 - paouelle - Creation
   *
   * @since 1.0
   */
  static class IsObjectClauseImpl extends ClauseImpl implements DelayedWithObject {
    /**
     * Instantiates a new <code>IsObjectClauseImpl</code> object.
     *
     * @author paouelle
     */
    IsObjectClauseImpl() {
      super("");
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClauseImpl.DelayedWithObject#processWith(org.helenus.driver.impl.TableInfoImpl, org.helenus.driver.impl.ClassInfoImpl.POJOContext)
     */
    @Override
    public <T> List<ClauseImpl> processWith(
      TableInfoImpl<T> table, ClassInfoImpl<T>.POJOContext context
    ) {
      final Map<String, Object> pkeys = context.getSuffixAndPrimaryKeyColumnValues(table.getName());
      final List<ClauseImpl> clauses = new ArrayList<>(pkeys.size());

      for (final Map.Entry<String, Object> e: pkeys.entrySet()) {
        clauses.add(new ClauseImpl.EqClauseImpl(e.getKey(), e.getValue()));
      }
      return clauses;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClauseImpl#getOperation()
     */
    @Override
    String getOperation() {
      return "IS OBJECT";
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.Utils.Appendeable#appendTo(org.helenus.driver.impl.TableInfoImpl, java.lang.StringBuilder)
     */
    @Override
    void appendTo(TableInfoImpl<?> tinfo, StringBuilder sb) {}

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClauseImpl#validate(org.helenus.driver.impl.TableInfoImpl)
     */
    @Override
    <T> void validate(TableInfoImpl<T> table) {
      throw new IllegalStateException("should not be called");
    }
  }

  /**
   * The <code>IsPartitionedLikeObjectClauseImpl</code> class defines a delayed clause that
   * generates a set of "equal" clauses for all partition primary key columns
   * of the provided POJO when added to a statement.
   *
   * @copyright 2015-2015 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 19, 2015 - paouelle - Creation
   *
   * @since 1.0
   */
  static class IsPartitionedLikeObjectClauseImpl extends ClauseImpl implements DelayedWithObject {
    /**
     * Instantiates a new <code>IsPartitionedLikeObjectClauseImpl</code> object.
     *
     * @author paouelle
     */
    IsPartitionedLikeObjectClauseImpl() {
      super("");
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClauseImpl.DelayedWithObject#processWith(org.helenus.driver.impl.TableInfoImpl, org.helenus.driver.impl.ClassInfoImpl.POJOContext)
     */
    @Override
    public <T> List<ClauseImpl> processWith(
      TableInfoImpl<T> table, ClassInfoImpl<T>.POJOContext context
    ) {
      final Map<String, Object> pkeys = context.getSuffixAndPartitionKeyColumnValues(table.getName());
      final List<ClauseImpl> clauses = new ArrayList<>(pkeys.size());

      for (final Map.Entry<String, Object> e: pkeys.entrySet()) {
        clauses.add(new ClauseImpl.EqClauseImpl(e.getKey(), e.getValue()));
      }
      return clauses;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClauseImpl#getOperation()
     */
    @Override
    String getOperation() {
      return "IS PARTITIONED LIKE OBJECT";
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.Utils.Appendeable#appendTo(org.helenus.driver.impl.TableInfoImpl, java.lang.StringBuilder)
     */
    @Override
    void appendTo(TableInfoImpl<?> tinfo, StringBuilder sb) {}

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClauseImpl#validate(org.helenus.driver.impl.TableInfoImpl)
     */
    @Override
    <T> void validate(TableInfoImpl<T> table) {
      throw new IllegalStateException("should not be called");
    }
  }

  /**
   * The <code>IsSuffixedLikeObjectClauseImpl</code> class defines a delayed clause that
   * generates a set of "equal" clauses for all suffix key columns
   * of the provided POJO when added to a statement.
   *
   * @copyright 2015-2015 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 19, 2015 - paouelle - Creation
   *
   * @since 1.0
   */
  static class IsSuffixedLikeObjectClauseImpl extends ClauseImpl implements DelayedWithObject {
    /**
     * Instantiates a new <code>IsSuffixedLikeObjectClauseImpl</code> object.
     *
     * @author paouelle
     */
    IsSuffixedLikeObjectClauseImpl() {
      super("");
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClauseImpl.DelayedWithObject#processWith(org.helenus.driver.impl.TableInfoImpl, org.helenus.driver.impl.ClassInfoImpl.POJOContext)
     */
    @Override
    public <T> List<ClauseImpl> processWith(
      TableInfoImpl<T> table, ClassInfoImpl<T>.POJOContext context
    ) {
      final Map<String, Object> pkeys = context.getSuffixKeyValues();
      final List<ClauseImpl> clauses = new ArrayList<>(pkeys.size());

      for (final Map.Entry<String, Object> e: pkeys.entrySet()) {
        clauses.add(new ClauseImpl.EqClauseImpl(e.getKey(), e.getValue()));
      }
      return clauses;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClauseImpl#getOperation()
     */
    @Override
    String getOperation() {
      return "IS SUFFIXED LIKE OBJECT";
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.Utils.Appendeable#appendTo(org.helenus.driver.impl.TableInfoImpl, java.lang.StringBuilder)
     */
    @Override
    void appendTo(TableInfoImpl<?> tinfo, StringBuilder sb) {}

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClauseImpl#validate(org.helenus.driver.impl.TableInfoImpl)
     */
    @Override
    <T> void validate(TableInfoImpl<T> table) {
      throw new IllegalStateException("should not be called");
    }
  }
}

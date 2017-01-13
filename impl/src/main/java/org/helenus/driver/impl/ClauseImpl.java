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
package org.helenus.driver.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Triple;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.TypeCodec;

import org.helenus.driver.Clause;
import org.helenus.driver.codecs.ArgumentsCodec;
import org.helenus.driver.persistence.CQLDataType;

/**
 * The <code>ClauseImpl</code> class extends Cassandra's
 * {@link com.datastax.driver.core.querybuilder.Clause} to provide support for
 * POJOs.
 *
 * @copyright 2015-2016 The Helenus Driver Project Authors
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
   * Checks if this clause contains the specified column.
   *
   * @author paouelle
   *
   * @param  name the name of the column to check for
   * @return <code>true</code> if this clause includes the specified column;
   *         <code>false</code> otherwise
   */
  boolean containsColumn(String name) {
    return this.name.toString().equals(name);
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
  }

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
   * @copyright 2015-2016 The Helenus Driver Project Authors
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
     * clause by processing only the clause that references keyspace keys.
     *
     * @author paouelle
     *
     * @param  cinfo the non-<code>null</code> POJO class info associated with
     *         the statement
     * @return a non-<code>null</code> list of new clause(s) corresponding to
     *         this one
     * @throws IllegalArgumentException if missing mandatory v keys are processed
     */
    public <T> List<ClauseImpl> processWith(ClassInfoImpl<T> cinfo);
  }

  /**
   * The <code>DelayedWithObject</code> interface is used by clauses that
   * do not have all their content defined at the time of creation but instead
   * at the time the clause is added to a statement initialized with a POJO.
   *
   * @copyright 2015-2016 The Helenus Driver Project Authors
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
     */
    public <T> List<ClauseImpl> processWith(
      TableInfoImpl<T> table, ClassInfoImpl<T>.POJOContext context
    );
  }

  /**
   * The <code>Compound</code> interface is used by clauses that compounds
   * multiple columns together.
   *
   * @copyright 2015-2016 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Oct 13, 2016 - paouelle - Creation
   *
   * @since 3.0
   */
  interface Compound {
    /**
     * Gets the column names for this clause.
     *
     * @author paouelle
     *
     * @return the column names for this clause
     */
    public List<String> getColumnNames();

    /**
     * Gets the column values for this clause.
     *
     * @author paouelle
     *
     * @return the column values for this clause
     */
    public List<?> getColumnValues();

    /**
     * Called at the time the clause is added to a statement to extract special
     * columns (i.e. multi-key or case insensitive keys) from this clause into
     * their own simple one.
     *
     * @author paouelle
     *
     * @param  table the non-<code>null</code> table associated with
     *         the statement
     * @return a list of new clause(s) corresponding to this one or <code>null</code>
     *         if no changes was required
     */
    public <T> List<ClauseImpl> extractSpecialColumns(TableInfoImpl<T> table);
  }

  /**
   * The <code>SimpleClauseImpl</code> class defines a clause specifying a column,
   * operator, and single value.
   *
   * @copyright 2015-2016 The Helenus Driver Project Authors
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
     * Holds the definition associated with the value if any.
     *
     * @author paouelle
     */
    protected final CQLDataType definition;

    /**
     * Holds the codec associated with the value if any.
     *
     * @author paouelle
     */
    protected final TypeCodec<?>codec;

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
      this(name, op, value, null);
    }

    /**
     * Instantiates a new <code>SimpleClauseImpl</code> object.
     *
     * @author paouelle
     *
     * @param  name the column name for this clause
     * @param  op the operator for this clause
     * @param  tvalue the value and its associated definition for this clause
     * @throws NullPointerException if <code>name</code> or <code>op</code>
     *         is <code>null</code>
     */
    SimpleClauseImpl(CharSequence name, String op, Triple<Object, CQLDataType, TypeCodec<?>> tvalue) {
      super(name);
      org.apache.commons.lang3.Validate.notNull(op, "invalid null operation");
      this.op = op;
      if (tvalue != null) {
        this.value = tvalue.getLeft();
        this.definition = tvalue.getMiddle();
        this.codec = tvalue.getRight();
      } else {
        this.value = null;
        this.definition = null;
        this.codec = null;
      }
    }

    /**
     * Instantiates a new <code>SimpleClauseImpl</code> object.
     *
     * @author paouelle
     *
     * @param  name the column name for this clause
     * @param  op the operator for this clause
     * @param  value the value for this clause
     * @param  definition the definition associated with the value if any
     * @throws NullPointerException if <code>name</code> or <code>op</code>
     *         is <code>null</code>
     */
    SimpleClauseImpl(CharSequence name, String op, Object value, CQLDataType definition) {
      super(name);
      org.apache.commons.lang3.Validate.notNull(op, "invalid null operation");
      this.op = op;
      this.value = value;
      this.definition = definition;
      this.codec = null;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.Utils.Appendeable#appendTo(org.helenus.driver.impl.TableInfoImpl, com.datastax.driver.core.TypeCodec, com.datastax.driver.core.CodecRegistry, java.lang.StringBuilder, java.util.List)
     */
    @Override
    void appendTo(
      TableInfoImpl<?> tinfo,
      TypeCodec<?> codec,
      CodecRegistry codecRegistry,
      StringBuilder sb,
      List<Object> variables
    ) {
      Utils.appendName(tinfo, null, codecRegistry, sb, name).append(op);
      FieldInfoImpl<?> finfo = tinfo.getColumnImpl(name);

      if (finfo == null) { // check if the field is the multi-key
        String sname = name.toString();

        if (sname.startsWith(StatementImpl.MK_PREFIX)) {
          sname = sname.substring(StatementImpl.MK_PREFIX.length()); // strip mk prefix
          finfo = tinfo.getColumnImpl(sname);
          if ((finfo != null) && finfo.isMultiKey()) {
            Utils.appendValue(
              (definition != null) ? definition : finfo.getDataType().getElementType(),
              (codec != null) ? codec : ((this.codec != null) ? this.codec : ((ArgumentsCodec<?>)finfo.getCodec()).codec(0)),
              codecRegistry,
              sb,
              value,
              variables
            );
            return;
          }
        } else if (sname.startsWith(StatementImpl.CI_PREFIX)) {
          sname = sname.substring(StatementImpl.CI_PREFIX.length()); // strip ci prefix
          finfo = tinfo.getColumnImpl(sname);
          if ((finfo != null) && finfo.isCaseInsensitiveKey()) {
            Utils.appendValue(
              (definition != null) ? definition : finfo.getDataType(),
              (codec != null) ? codec : ((this.codec != null) ? this.codec : finfo.getCodec()),
              codecRegistry,
              sb,
              value,
              variables
            );
            return;
          }
        }
        throw new IllegalStateException("unknown column '" + name + "'");
      }
      Utils.appendValue(
        (definition != null) ? definition : finfo.getDataType(),
        (codec != null) ? codec : ((this.codec != null) ? this.codec : finfo.getCodec()),
        codecRegistry,
        sb,
        value,
        variables
      );
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.Utils.Appendeable#containsBindMarker()
     */
    @Override
    boolean containsBindMarker() {
      return Utils.containsBindMarker(value);
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

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
      return name + " " + getOperation() + " " + firstValue();
    }
  }

  /**
   * The <code>EqClauseImpl</code> class defines a clause specifying a column,
   * operator, and single value.
   *
   * @copyright 2015-2016 The Helenus Driver Project Authors
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
     * Instantiates a new <code>EqClauseImpl</code> object.
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
     * Instantiates a new <code>EqClauseImpl</code> object.
     *
     * @author paouelle
     *
     * @param  name the column name for this clause
     * @param  tvalue the value and its associated definition for this clause
     * @throws NullPointerException if <code>name</code> is <code>null</code>
     */
    EqClauseImpl(CharSequence name, Triple<Object, CQLDataType, TypeCodec<?>> tvalue) {
      super(name, "=", tvalue);
    }

    /**
     * Instantiates a new <code>EqClauseImpl</code> object.
     *
     * @author paouelle
     *
     * @param  name the column name for this clause
     * @param  value the value for this clause
     * @param  definition the definition associated with the value if any
     * @throws NullPointerException if <code>name</code> is <code>null</code>
     */
    EqClauseImpl(CharSequence name, Object value, CQLDataType definition) {
      super(name, "=", value, definition);
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
      table.validateKeyspaceKeyOrColumnAndValue(name, value);
    }
  }

  /**
   * The <code>InClauseImpl</code> class defines the IN statement clause.
   *
   * @copyright 2015-2016 The Helenus Driver Project Authors
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
     * @see org.helenus.driver.impl.Utils.Appendeable#appendTo(org.helenus.driver.impl.TableInfoImpl, com.datastax.driver.core.TypeCodec, com.datastax.driver.core.CodecRegistry, java.lang.StringBuilder, java.util.List)
     */
    @Override
    void appendTo(
      TableInfoImpl<?> tinfo,
      TypeCodec<?> codec,
      CodecRegistry codecRegistry,
      StringBuilder sb,
      List<Object> variables
    ) {
      if (values.size() == 1) {
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

        if (Utils.isBindMarker(fv)) {
          Utils.appendName(tinfo, null, codecRegistry, sb, name).append(" IN ").append(fv);
          return;
        }
      }
      Utils.appendName(tinfo, null, codecRegistry, sb, name).append(" IN (");
      FieldInfoImpl<?> finfo = tinfo.getColumnImpl(name);

      if (finfo == null) { // check if the name reference a multi-key
        String sname = name.toString();

        if (sname.startsWith(StatementImpl.MK_PREFIX)) {
          sname = sname.substring(StatementImpl.MK_PREFIX.length()); // strip mk prefix
          finfo = tinfo.getColumnImpl(sname);
          if ((finfo != null) && finfo.isMultiKey()) {
            Utils.joinAndAppendValues(
              (codec != null) ? codec : ((ArgumentsCodec<?>)finfo.getCodec()).codec(0),
              codecRegistry,
              sb,
              ",",
              values,
              finfo.getDataType().getElementType(),
              variables
            ).append(")");
            return;
          }
        } else if (sname.startsWith(StatementImpl.CI_PREFIX)) {
          sname = sname.substring(StatementImpl.CI_PREFIX.length()); // strip ci prefix
          finfo = tinfo.getColumnImpl(sname);
          if ((finfo != null) && finfo.isCaseInsensitiveKey()) {
            Utils.joinAndAppendValues(
              (codec != null) ? codec : finfo.getCodec(),
              codecRegistry,
              sb,
              ",",
              values,
              finfo.getDataType(),
              variables
            ).append(")");
            return;
          }
        }
        throw new IllegalStateException("unknown column '" + sname + "'");
      }
      Utils.joinAndAppendValues(
        (codec != null) ? codec : finfo.getCodec(),
        codecRegistry,
        sb,
        ",",
        values,
        finfo.getDataType(),
        variables
      ).append(")");
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.Utils.Appendeable#containsBindMarker()
     */
    @Override
    boolean containsBindMarker() {
      return Utils.containsBindMarker(values);
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
      return !values().isEmpty() ? values.iterator().next() : null;
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

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
      return name + " " + getOperation() + " " + values;
    }
  }

  /**
   * The <code>ContainsClauseImpl</code> class defines the CONTAINS statement
   * clause.
   *
   * @copyright 2015-2016 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Oct 17, 2016 - paouelle - Creation
   *
   * @since 3.0
   */
  static class ContainsClauseImpl extends ClauseImpl {
    /**
     * Holds the value that must be contained.
     *
     * @author paouelle
     */
    private final Object value;

    /**
     * Instantiates a new <code>ContainsClauseImpl</code> object.
     *
     * @author paouelle
     *
     * @param  name the name of the column for this clause
     * @param  value the value for this clause
     * @throws NullPointerException if <code>name</code> or <code>value</code>
     *         is <code>null</code>
     */
    ContainsClauseImpl(CharSequence name, Object value) {
      super(name);
      this.value = value;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.Utils.Appendeable#appendTo(org.helenus.driver.impl.TableInfoImpl, com.datastax.driver.core.TypeCodec, com.datastax.driver.core.CodecRegistry, java.lang.StringBuilder, java.util.List)
     */
    @Override
    void appendTo(
      TableInfoImpl<?> tinfo,
      TypeCodec<?> codec,
      CodecRegistry codecRegistry,
      StringBuilder sb,
      List<Object> variables
    ) {
      Utils.appendName(tinfo, null, codecRegistry, sb, name).append(" CONTAINS ");
      final FieldInfoImpl<?> finfo = tinfo.getColumnImpl(name);

      Utils.appendValue(
        finfo.getDataType().getElementType(),
        (codec != null) ? codec : ((ArgumentsCodec<?>)finfo.getCodec()).codec(0),
        codecRegistry,
        sb,
        value,
        variables
      );
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.Utils.Appendeable#containsBindMarker()
     */
    @Override
    boolean containsBindMarker() {
      return Utils.containsBindMarker(value);
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
      return "CONTAINS";
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
      table.validateCollectionColumnAndValue(name, value);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
      return name + " " + getOperation() + " " + value;
    }
  }

  /**
   * The <code>ContainsKeyClauseImpl</code> class defines the CONTAINS statement
   * clause.
   *
   * @copyright 2015-2016 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Oct 17, 2016 - paouelle - Creation
   *
   * @since 3.0
   */
  static class ContainsKeyClauseImpl extends ClauseImpl {
    /**
     * Holds the key that must be contained.
     *
     * @author paouelle
     */
    private final Object key;

    /**
     * Instantiates a new <code>ContainsKeyClauseImpl</code> object.
     *
     * @author paouelle
     *
     * @param  name the name of the column for this clause
     * @param  key the key for this clause
     * @throws NullPointerException if <code>name</code> or <code>key</code>
     *         is <code>null</code>
     */
    ContainsKeyClauseImpl(CharSequence name, Object key) {
      super(name);
      this.key = key;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.Utils.Appendeable#appendTo(org.helenus.driver.impl.TableInfoImpl, com.datastax.driver.core.TypeCodec, com.datastax.driver.core.CodecRegistry, java.lang.StringBuilder, java.util.List)
     */
    @Override
    void appendTo(
      TableInfoImpl<?> tinfo,
      TypeCodec<?> codec,
      CodecRegistry codecRegistry,
      StringBuilder sb,
      List<Object> variables
    ) {
      Utils.appendName(tinfo, null, codecRegistry, sb, name).append(" CONTAINS KEY ");
      final FieldInfoImpl<?> finfo = tinfo.getColumnImpl(name);

      Utils.appendValue(
        finfo.getDataType().getFirstArgumentType(),
        (codec != null) ? codec : ((ArgumentsCodec<?>)finfo.getCodec()).codec(0),
        codecRegistry,
        sb,
        key,
        variables
      );
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.Utils.Appendeable#containsBindMarker()
     */
    @Override
    boolean containsBindMarker() {
      return Utils.containsBindMarker(key);
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
      return key;
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
      return Collections.singleton(key);
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
      return "CONTAINS KEY";
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
      table.validateMapColumnAndKey(name, key);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
      return name + " " + getOperation() + " " + key;
    }
  }

  /**
   * The <code>CompoundClauseImpl</code> class defines a clause specifying a set
   * of columns, an operator, and a set of corresponding value.
   *
   * @copyright 2015-2016 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Oct 12, 2016 - paouelle - Creation
   *
   * @since 3.0
   */
  static class CompoundClauseImpl
    extends ClauseImpl
    implements Clause.Compound, Compound {
    /**
     * Holds the operator for this clause.
     *
     * @author paouelle
     */
    private final String op;

    /**
     * Holds the column names.
     *
     * @author paouelle
     */
    protected final List<String> names;

    /**
     * Holds the corresponding column value associated with this clause.
     *
     * @author paouelle
     */
    protected final List<?> values;

    /**
     * Holds the corresponding column definitions associated with the values if
     * any (might contains <code>null</code>).
     *
     * @author paouelle
     */
    protected final List<CQLDataType> definitions;

    /**
     * Instantiates a new <code>CompoundClauseImpl</code> object.
     *
     * @author paouelle
     *
     * @param  names the column names for this clause
     * @param  op the operator for this clause
     * @param  values the values for this clause
     * @throws NullPointerException if <code>names</code>, <code>op</code>, or
     *         <code>values</code> is <code>null</code>
     * @throws IllegalArgumentException if the there is not the same number of
     *         values (or definitions if provided) and column names
     */
    CompoundClauseImpl(List<String> names, String op, List<?> values) {
      this(names, op, values, null);
    }

//    /**
//     * Instantiates a new <code>CompoundClauseImpl</code> object.
//     *
//     * @author paouelle
//     *
//     * @param  name the column name for this clause
//     * @param  op the operator for this clause
//     * @param  pvalue the value and its associated definition for this clause
//     * @throws NullPointerException if <code>names</code>, <code>op</code>, or
//     *         <code>tvalues</code> is <code>null</code>
//     * @throws IllegalArgumentException if the there is not the same number of
//     *         values and column names
//     */
//    CompoundClauseImpl(
//      List<String> names, String op, List<Triple<Object, CQLDataType, TypeCodec<?>>> tvalues
//    ) {
//      super("");
//      org.apache.commons.lang3.Validate.notNull(op, "invalid null operation");
//      if (names.size() != pvalues.size()) {
//        throw new IllegalArgumentException(
//          "mismatched number of column names and values: "
//          + names.size()
//          + " names and "
//          + pvalues.size()
//          + " values"
//        );
//      }
//      this.names = names;
//      this.op = op;
//      this.definitions = new ArrayList<>(tvalues.size());
//      this.values = pvalues.stream()
//        .peek(t -> definitions.add(t.getRight()))
//        .map(Pair::getLeft)
//        .collect(Collectors.toList());
//    }

    /**
     * Instantiates a new <code>CompoundClauseImpl</code> object.
     *
     * @author paouelle
     *
     * @param  names the column names for this clause
     * @param  op the operator for this clause
     * @param  values the values for this clause
     * @param  definitions the definitions associated with the values if any
     * @throws NullPointerException if <code>names</code>, <code>op</code>, or
     *         <code>values</code> is <code>null</code>
     * @throws IllegalArgumentException if the there is not the same number of
     *         values (or definitions if provided) and column names
     */
    CompoundClauseImpl(
      List<String> names, String op, List<?> values, List<CQLDataType> definitions
    ) {
      super("");
      org.apache.commons.lang3.Validate.notNull(op, "invalid null operation");
      if (names.size() != values.size()) {
        throw new IllegalArgumentException(
          "mismatched number of column names and values: "
          + names.size()
          + " names and "
          + values.size()
          + " values"
        );
      }
      if ((definitions != null) && (names.size() != definitions.size())) {
        throw new IllegalArgumentException(
          "mismatched number of column names and definitions: "
          + names.size()
          + " names and "
          + definitions.size()
          + " definitions"
        );
      }
      this.names = names;
      this.op = op;
      this.values = values;
      if (definitions != null) {
        this.definitions = definitions;
      } else {
        this.definitions = new ArrayList<>(names.size());
        for (int i = 0; i < names.size(); i++) {
          this.definitions.add(null);
        }
      }
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClauseImpl.Compound#extractSpecialColumns(org.helenus.driver.impl.TableInfoImpl)
     */
    @Override
    public <T> List<ClauseImpl> extractSpecialColumns(TableInfoImpl<T> table) {
      if (table.hasCaseInsensitiveKeys() || table.hasMultiKeys()) {
        final List<ClauseImpl> clauses = new ArrayList<>(names.size());
        final List<CQLDataType> cdefinitions = new ArrayList<>(names.size());
        final List<Object> cvalues = new ArrayList<>(names.size());
        final List<String> cnames = new ArrayList<>(names.size());

        for (int i = 0; i < names.size(); i++) {
          final CQLDataType definition = definitions.get(i);
          final Object value = values.get(i);
          final String name = names.get(i);
          final FieldInfoImpl<T> finfo = table.getColumnImpl(name);

          if ((finfo != null) && (finfo.isCaseInsensitiveKey() || finfo.isMultiKey())) {
            clauses.add(new ClauseImpl.SimpleClauseImpl(name, op, value, definition));
          } else {
            cnames.add(name);
            cvalues.add(value);
            cdefinitions.add(definition);
          }
        }
        if (!clauses.isEmpty()) { // add one clause for the remaining columns if any
          if (cnames.size() == 1) { // special case if only one remains
            clauses.add(new ClauseImpl.SimpleClauseImpl(cnames.get(0), op, cvalues.get(0), cdefinitions.get(0)));
          } else {
            clauses.add(new ClauseImpl.CompoundClauseImpl(cnames, op, cvalues, cdefinitions));
          }
          return clauses;
        }
      }
      return null;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.Utils.Appendeable#appendTo(org.helenus.driver.impl.TableInfoImpl, com.datastax.driver.core.TypeCodec, com.datastax.driver.core.CodecRegistry, java.lang.StringBuilder, java.util.List)
     */
    @Override
    void appendTo(
      TableInfoImpl<?> tinfo,
      TypeCodec<?> codec,
      CodecRegistry codecRegistry,
      StringBuilder sb,
      List<Object> variables
    ) {
      final StringBuilder vsb = new StringBuilder(300);

      sb.append('(');
      vsb.append('(');
      for (int i = 0; i < names.size(); i++) {
        final CQLDataType definition = definitions.get(i);
        final Object value = values.get(i);
        String sname = names.get(i);

        if (i > 0) {
          sb.append(',');
          vsb.append(',');
        }
        Utils.appendName(sb, sname);
        FieldInfoImpl<?> finfo = tinfo.getColumnImpl(sname);

        if (finfo == null) { // check if the field is the multi-key
          if (sname.startsWith(StatementImpl.MK_PREFIX)) {
            sname = sname.substring(StatementImpl.MK_PREFIX.length()); // strip mk prefix
            finfo = tinfo.getColumnImpl(sname);
            if ((finfo != null) && finfo.isMultiKey()) {
              Utils.appendValue(
                (definition != null) ? definition : finfo.getDataType().getElementType(),
                (codec != null) ? codec : ((ArgumentsCodec<?>)finfo.getCodec()).codec(0),
                codecRegistry,
                vsb,
                value,
                variables
              );
              continue;
            }
          } else if (sname.startsWith(StatementImpl.CI_PREFIX)) {
            sname = sname.substring(StatementImpl.CI_PREFIX.length()); // strip ci prefix
            finfo = tinfo.getColumnImpl(sname);
            if ((finfo != null) && finfo.isCaseInsensitiveKey()) {
              Utils.appendValue(
                (definition != null) ? definition : finfo.getDataType(),
                (codec != null) ? codec : finfo.getCodec(),
                codecRegistry,
                vsb,
                value,
                variables
              );
              continue;
            }
          }
          throw new IllegalStateException("unknown column '" + sname + "'");
        }
        Utils.appendValue(
          (definition != null) ? definition : finfo.getDataType(),
          (codec != null) ? codec : finfo.getCodec(),
          codecRegistry,
          vsb,
          value,
          variables
        );
      }
      sb.append(')').append(op).append(vsb).append(')');
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.Utils.Appendeable#containsBindMarker()
     */
    @Override
    boolean containsBindMarker() {
      return Utils.containsBindMarker(values);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClauseImpl#getColumnName()
     */
    @Override
    CharSequence getColumnName() {
      throw new IllegalStateException("should not be called");
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClauseImpl#containsColumn(java.lang.String)
     */
    @Override
    boolean containsColumn(String name) {
      return names.contains(name);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClauseImpl.Compound#getColumnNames()
     */
    @Override
    public List<String> getColumnNames() {
      return names;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClauseImpl.Compound#getColumnValues()
     */
    @Override
    public List<?> getColumnValues() {
      return values;
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
      return !values.isEmpty() ? values.get(0) : null;
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
      for (int i = 0; i < names.size(); i++) {
        table.validateColumnAndValue(names.get(i), values.get(i));
      }
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
      return names + " " + getOperation() + " " + values;
    }
  }

  /**
   * The <code>CompoundEqClauseImpl</code> class defines a clause specifying a set
   * of columns, an operator, and a set of corresponding value.
   *
   * @copyright 2015-2016 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Oct 12, 2016 - paouelle - Creation
   *
   * @since 3.0
   */
  static class CompoundEqClauseImpl
    extends CompoundClauseImpl
    implements Clause.Equality {
    /**
     * Instantiates a new <code>CompoundEqClauseImpl</code> object.
     *
     * @author paouelle
     *
     * @param  names the column names for this clause
     * @param  values the values for this clause
     * @throws NullPointerException if <code>names</code> or
     *         <code>values</code> is <code>null</code>
     * @throws IllegalArgumentException if the there is not the same number of
     *         values (or definitions if provided) and column names
     */
    CompoundEqClauseImpl(List<String> names, List<?> values) {
      super(names, "=", values);
    }

//    /**
//     * Instantiates a new <code>CompoundEqClauseImpl</code> object.
//     *
//     * @author paouelle
//     *
//     * @param  name the column name for this clause
//     * @param  pvalue the value and its associated definition for this clause
//     * @throws NullPointerException if <code>names</code>  or
//     *         <code>tvalues</code> is <code>null</code>
//     * @throws IllegalArgumentException if the there is not the same number of
//     *         values and column names
//     */
//    CompoundEqClauseImpl(List<String> names, List<Triple<Object, CQLDataType, TypeCodec<?>>> tvalues) {
//      super(names, "=", tvalues);
//    }

    /**
     * Instantiates a new <code>CompoundEqClauseImpl</code> object.
     *
     * @author paouelle
     *
     * @param  names the column names for this clause
     * @param  values the values for this clause
     * @param  definitions the definitions associated with the values if any
     * @throws NullPointerException if <code>names</code>, <code>op</code>, or
     *         <code>values</code> is <code>null</code>
     * @throws IllegalArgumentException if the there is not the same number of
     *         values (or definitions if provided) and column names
     */
    CompoundEqClauseImpl(
      List<String> names, List<?> values, List<CQLDataType> definitions
    ) {
      super(names, "=", values, definitions);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClauseImpl.CompoundClauseImpl#validate(org.helenus.driver.impl.TableInfoImpl)
     */
    @Override
    <T> void validate(TableInfoImpl<T> table) {
      for (int i = 0; i < names.size(); i++) {
        table.validateKeyspaceKeyOrColumnAndValue(names.get(i), values.get(i));
      }
    }
  }

  /**
   * The <code>CompoundInClauseImpl</code> class defines the IN statement clause.
   *
   * @copyright 2015-2016 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Oct 12, 2016 - paouelle - Creation
   *
   * @since 3.0
   */
  static class CompoundInClauseImpl
    extends ClauseImpl
    implements Clause.Compound, Clause.In, Compound {
    /**
     * Holds the column names.
     *
     * @author paouelle
     */
    protected final List<String> names;

    /**
     * Holds the corresponding collection of values for this clause.
     *
     * @author paouelle
     */
    private final List<? extends Collection<?>> values;

    /**
     * Instantiates a new <code>CompoundInClauseImpl</code> object.
     *
     * @author paouelle
     *
     * @param  names the column names for this clause
     * @param  values the values for this clause
     * @throws NullPointerException if <code>names</code> or
     *         <code>values</code> is <code>null</code>
     * @throws IllegalArgumentException if the there is not the same number of
     *         values (or definitions if provided) and column names
     */
    CompoundInClauseImpl(
      List<String> names, List<? extends Collection<?>> values
    ) {
      super("");
      if (names.size() != values.size()) {
        throw new IllegalArgumentException(
          "mismatched number of column names and collection of values: "
          + names.size()
          + " names and "
          + values.size()
          + " values"
        );
      }
      this.names = names;
      this.values = values;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClauseImpl.Compound#extractSpecialColumns(org.helenus.driver.impl.TableInfoImpl)
     */
    @Override
    public <T> List<ClauseImpl> extractSpecialColumns(TableInfoImpl<T> table) {
      if (table.hasCaseInsensitiveKeys() || table.hasMultiKeys()) {
        final List<ClauseImpl> clauses = new ArrayList<>(names.size());
        final List<Collection<?>> cvalues = new ArrayList<>(names.size());
        final List<String> cnames = new ArrayList<>(names.size());

        for (int i = 0; i < names.size(); i++) {
          final Collection<?> value = values.get(i);
          final String name = names.get(i);
          final FieldInfoImpl<T> finfo = table.getColumnImpl(name);

          if ((finfo != null) && (finfo.isCaseInsensitiveKey() || finfo.isMultiKey())) {
            clauses.add(new ClauseImpl.InClauseImpl(name, value));
          } else {
            cnames.add(name);
            cvalues.add(value);
          }
        }
        if (!clauses.isEmpty()) { // add one clause for the remaining columns if any
          if (cnames.size() == 1) { // special case if only one remains
            clauses.add(new ClauseImpl.InClauseImpl(cnames.get(0), cvalues.get(0)));
          } else {
            clauses.add(new ClauseImpl.CompoundInClauseImpl(cnames, cvalues));
          }
          return clauses;
        }
      }
      return null;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.Utils.Appendeable#appendTo(org.helenus.driver.impl.TableInfoImpl, com.datastax.driver.core.TypeCodec, com.datastax.driver.core.CodecRegistry, java.lang.StringBuilder, java.util.List)
     */
    @Override
    void appendTo(
      TableInfoImpl<?> tinfo,
      TypeCodec<?> codec,
      CodecRegistry codecRegistry,
      StringBuilder sb,
      List<Object> variables
    ) {
      final StringBuilder vsb = new StringBuilder(300);
      int j = 0;

      sb.append('(');
      vsb.append('(');
      for (int i = 0; i < names.size(); i++) {
        final Collection<?> values = this.values.get(i);
        String sname = names.get(i);

        if (values.isEmpty()) { // no values in IN so skip that column
          continue;
        }
        if (j++ > 0) {
          sb.append(',');
          vsb.append(',');
        }
        if (values.size() == 1) {
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

          if (Utils.isBindMarker(fv)) {
            Utils.appendName(sb, sname);
            vsb.append(fv);
            continue;
          }
        }
        Utils.appendName(sb, sname);
        vsb.append('(');
        FieldInfoImpl<?> finfo = tinfo.getColumnImpl(sname);

        if (finfo == null) { // check if the name reference a multi-key
          if (sname.startsWith(StatementImpl.MK_PREFIX)) {
            sname = sname.substring(StatementImpl.MK_PREFIX.length()); // strip mk prefix
            finfo = tinfo.getColumnImpl(sname);
            if ((finfo != null) && finfo.isMultiKey()) {
              Utils.joinAndAppendValues(
                (codec != null) ? codec : ((ArgumentsCodec<?>)finfo.getCodec()).codec(0),
                codecRegistry,
                vsb,
                ",",
                values,
                finfo.getDataType().getElementType(),
                variables
              ).append(")");
              continue;
            }
          } else if (sname.startsWith(StatementImpl.CI_PREFIX)) {
            sname = sname.substring(StatementImpl.CI_PREFIX.length()); // strip ci prefix
            finfo = tinfo.getColumnImpl(sname);
            if ((finfo != null) && finfo.isCaseInsensitiveKey()) {
              Utils.joinAndAppendValues(
                (codec != null) ? codec : finfo.getCodec(),
                codecRegistry,
                vsb,
                ",",
                values,
                finfo.getDataType(),
                variables
              ).append(")");
              continue;
            }
          }
          throw new IllegalStateException("unknown column '" + sname + "'");
        }
        Utils.joinAndAppendValues(
          (codec != null) ? codec : finfo.getCodec(),
          codecRegistry,
          vsb,
          ",",
          values,
          finfo.getDataType(),
          variables
        ).append(")");
      }
      sb.append(") IN ").append(vsb).append(')');
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.Utils.Appendeable#containsBindMarker()
     */
    @Override
    boolean containsBindMarker() {
      return Utils.containsBindMarker(values);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClauseImpl#getColumnName()
     */
    @Override
    CharSequence getColumnName() {
      throw new IllegalStateException("should not be called");
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClauseImpl#containsColumn(java.lang.String)
     */
    @Override
    boolean containsColumn(String name) {
      return names.contains(name);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClauseImpl.Compound#getColumnNames()
     */
    @Override
    public List<String> getColumnNames() {
      return names;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClauseImpl.Compound#getColumnValues()
     */
    @Override
    public List<?> getColumnValues() {
      return values;
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
      for (int i = 0; i < names.size(); i++) {
        table.validateColumnAndValues(names.get(i), values.get(i));
      }
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
      return name + " " + getOperation() + " " + values;
    }
  }

  /**
   * The <code>IsClauseImpl</code> class defines a delayed clause that generates a
   * set of "equal" clauses for all primary key columns of the provided POJO
   * when added to a statement.
   *
   * @copyright 2015-2016 The Helenus Driver Project Authors
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
      final Collection<FieldInfoImpl<T>> keys = table.getKeyspaceAndPrimaryKeys();
      final List<ClauseImpl> clauses = new ArrayList<>(keys.size());

      for (final FieldInfoImpl<T> finfo: keys) {
        final String name = finfo.isColumn() ? finfo.getColumnName() : finfo.getKeyspaceKeyName();

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
      final Collection<FieldInfoImpl<T>> keys = cinfo.getKeyspaceKeys().values();
      final List<ClauseImpl> clauses = new ArrayList<>(keys.size());

      for (final FieldInfoImpl<T> finfo: keys) {
        clauses.add(new ClauseImpl.EqClauseImpl(finfo.getKeyspaceKeyName(), finfo.getValue((T)object)));
      }
      return clauses;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClauseImpl#getColumnName()
     */
    @Override
    CharSequence getColumnName() {
      throw new IllegalStateException("should not be called");
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClauseImpl#containsColumn(java.lang.String)
     */
    @Override
    boolean containsColumn(String name) {
      throw new IllegalStateException("should not be called");
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
     * @see org.helenus.driver.impl.Utils.Appendeable#appendTo(org.helenus.driver.impl.TableInfoImpl, com.datastax.driver.core.TypeCodec, com.datastax.driver.core.CodecRegistry, java.lang.StringBuilder, java.util.List)
     */
    @Override
    void appendTo(
      TableInfoImpl<?> tinfo,
      TypeCodec<?> codec,
      CodecRegistry codecRegistry,
      StringBuilder sb,
      List<Object> variables
    ) {
      throw new IllegalStateException("should not be called");
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.Utils.Appendeable#containsBindMarker()
     */
    @Override
    boolean containsBindMarker() {
      throw new IllegalStateException("should not be called");
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
      throw new IllegalStateException("should not be called");
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
      return name + " " + getOperation() + " " + object;
    }
  }

  /**
   * The <code>IsPartitionedLikeClauseImpl</code> class defines a delayed clause that
   * generates a set of "equal" clauses for all partition primary key columns
   * of the provided POJO when added to a statement.
   *
   * @copyright 2015-2016 The Helenus Driver Project Authors
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
      final Collection<FieldInfoImpl<T>> keys = table.getKeyspaceAndPartitionKeys();
      final List<ClauseImpl> clauses = new ArrayList<>(keys.size());

      for (final FieldInfoImpl<T> finfo: keys) {
        final String name = finfo.isColumn() ? finfo.getColumnName() : finfo.getKeyspaceKeyName();

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
   * The <code>IsKeyspacedLikeClauseImpl</code> class defines a delayed clause that
   * generates a set of "equal" clauses for all keyspace keys
   * of the provided POJO when added to a statement.
   *
   * @copyright 2015-2016 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 19, 2015 - paouelle - Creation
   *
   * @since 1.0
   */
  static class IsKeyspacedLikeClauseImpl extends IsClauseImpl {
    /**
     * Instantiates a new <code>IsKeyspacedLikeClauseImpl</code> object.
     *
     * @author paouelle
     *
     * @param  object the POJO object
     * @throws NullPointerException if <code>object</code> is <code>null</code>
     */
    IsKeyspacedLikeClauseImpl(Object object) {
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
      final Map<String, FieldInfoImpl<T>> keys = ((ClassInfoImpl<T>)table.getClassInfo()).getKeyspaceKeys();
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
      return "IS KEYSPACED LIKE";
    }
  }

  /**
   * The <code>IsObjectClauseImpl</code> class defines a delayed clause that generates a
   * set of "equal" clauses for all primary key columns of the POJO used when the
   * statement was initialized at the time it is added to a statement.
   *
   * @copyright 2015-2016 The Helenus Driver Project Authors
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
      final Map<String, Triple<Object, CQLDataType, TypeCodec<?>>> pkeys = context.getKeyspaceAndPrimaryKeyColumnValues(table.getName());
      final List<ClauseImpl> clauses = new ArrayList<>(pkeys.size());

      for (final Map.Entry<String, Triple<Object, CQLDataType, TypeCodec<?>>> e: pkeys.entrySet()) {
        clauses.add(new ClauseImpl.EqClauseImpl(e.getKey(), e.getValue()));
      }
      return clauses;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClauseImpl#getColumnName()
     */
    @Override
    CharSequence getColumnName() {
      throw new IllegalStateException("should not be called");
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClauseImpl#containsColumn(java.lang.String)
     */
    @Override
    boolean containsColumn(String name) {
      throw new IllegalStateException("should not be called");
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
     * @see org.helenus.driver.impl.Utils.Appendeable#appendTo(org.helenus.driver.impl.TableInfoImpl, com.datastax.driver.core.TypeCodec, com.datastax.driver.core.CodecRegistry, java.lang.StringBuilder, java.util.List)
     */
    @Override
    void appendTo(
      TableInfoImpl<?> tinfo,
      TypeCodec<?> codec,
      CodecRegistry codecRegistry,
      StringBuilder sb,
      List<Object> variables
    ) {
      throw new IllegalStateException("should not be called");
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.Utils.Appendeable#containsBindMarker()
     */
    @Override
    boolean containsBindMarker() {
      throw new IllegalStateException("should not be called");
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
      throw new IllegalStateException("should not be called");
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
      return getOperation();
    }
  }

  /**
   * The <code>IsPartitionedLikeObjectClauseImpl</code> class defines a delayed clause that
   * generates a set of "equal" clauses for all partition primary key columns
   * of the provided POJO when added to a statement.
   *
   * @copyright 2015-2016 The Helenus Driver Project Authors
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
      final Map<String, Triple<Object, CQLDataType, TypeCodec<?>>> pkeys
        = context.getKeyspaceAndPartitionKeyColumnValues(table.getName());
      final List<ClauseImpl> clauses = new ArrayList<>(pkeys.size());

      for (final Map.Entry<String, Triple<Object, CQLDataType, TypeCodec<?>>> e: pkeys.entrySet()) {
        clauses.add(new ClauseImpl.EqClauseImpl(e.getKey(), e.getValue()));
      }
      return clauses;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClauseImpl#getColumnName()
     */
    @Override
    CharSequence getColumnName() {
      throw new IllegalStateException("should not be called");
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClauseImpl#containsColumn(java.lang.String)
     */
    @Override
    boolean containsColumn(String name) {
      throw new IllegalStateException("should not be called");
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
     * @see org.helenus.driver.impl.Utils.Appendeable#appendTo(org.helenus.driver.impl.TableInfoImpl, com.datastax.driver.core.TypeCodec, com.datastax.driver.core.CodecRegistry, java.lang.StringBuilder, java.util.List)
     */
    @Override
    void appendTo(
      TableInfoImpl<?> tinfo,
      TypeCodec<?> codec,
      CodecRegistry codecRegistry,
      StringBuilder sb,
      List<Object> variables
    ) {
      throw new IllegalStateException("should not be called");
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.Utils.Appendeable#containsBindMarker()
     */
    @Override
    boolean containsBindMarker() {
      throw new IllegalStateException("should not be called");
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
      throw new IllegalStateException("should not be called");
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
      return getOperation();
    }
  }

  /**
   * The <code>IsKeyspacedLikeObjectClauseImpl</code> class defines a delayed clause that
   * generates a set of "equal" clauses for all keyspace key columns
   * of the provided POJO when added to a statement.
   *
   * @copyright 2015-2016 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 19, 2015 - paouelle - Creation
   *
   * @since 1.0
   */
  static class IsKeyspacedLikeObjectClauseImpl extends ClauseImpl implements DelayedWithObject {
    /**
     * Instantiates a new <code>IsKeyspacedLikeObjectClauseImpl</code> object.
     *
     * @author paouelle
     */
    IsKeyspacedLikeObjectClauseImpl() {
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
      final Map<String, Triple<Object, CQLDataType, TypeCodec<?>>> pkeys
        = context.getKeyspaceKeyValues();
      final List<ClauseImpl> clauses = new ArrayList<>(pkeys.size());

      for (final Map.Entry<String, Triple<Object, CQLDataType, TypeCodec<?>>> e: pkeys.entrySet()) {
        clauses.add(new ClauseImpl.EqClauseImpl(e.getKey(), e.getValue()));
      }
      return clauses;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClauseImpl#getColumnName()
     */
    @Override
    CharSequence getColumnName() {
      throw new IllegalStateException("should not be called");
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.ClauseImpl#containsColumn(java.lang.String)
     */
    @Override
    boolean containsColumn(String name) {
      throw new IllegalStateException("should not be called");
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
      return "IS KEYSPACED LIKE OBJECT";
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.Utils.Appendeable#appendTo(org.helenus.driver.impl.TableInfoImpl, com.datastax.driver.core.TypeCodec, com.datastax.driver.core.CodecRegistry, java.lang.StringBuilder, java.util.List)
     */
    @Override
    void appendTo(
      TableInfoImpl<?> tinfo,
      TypeCodec<?> codec,
      CodecRegistry codecRegistry,
      StringBuilder sb,
      List<Object> variables
    ) {
      throw new IllegalStateException("should not be called");
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.Utils.Appendeable#containsBindMarker()
     */
    @Override
    boolean containsBindMarker() {
      throw new IllegalStateException("should not be called");
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
      throw new IllegalStateException("should not be called");
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
      return getOperation();
    }
  }
}

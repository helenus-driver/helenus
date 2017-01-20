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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.lang3.tuple.Triple;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.TypeCodec;

import org.helenus.driver.Assignment;
import org.helenus.driver.BindMarker;
import org.helenus.driver.codecs.ArgumentsCodec;
import org.helenus.driver.persistence.CQLDataType;
import org.helenus.driver.persistence.DataType;

/**
 * The <code>AssignmentImpl</code> class extends the functionality of Cassandra's
 * {@link com.datastax.driver.core.querybuilder.Assignment} class to provide
 * support for POJOs.
 *
 * @copyright 2015-2016 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 19, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
public abstract class AssignmentImpl
  extends Utils.Appendeable
  implements Assignment {
  /**
   * Holds the column name for this assignment.
   *
   * @author paouelle
   */
  protected final CharSequence name;

  /**
   * Instantiates a new <code>AssignmentImpl</code> object.
   *
   * @author paouelle
   *
   * @param  name the column name for this assignment
   * @throws NullPointerException if <code>name</code> is <code>null</code>
   */
  AssignmentImpl(CharSequence name) {
    org.apache.commons.lang3.Validate.notNull(name, "invalid null column name");
    this.name = name;
  }

  /**
   * Gets the column name for this assignment.
   *
   * @author paouelle
   *
   * @return the column name for this assignment
   */
  CharSequence getColumnName() {
    return name;
  }

  /**
   * Checks if this assignment is idempotent.
   *
   * @author paouelle
   *
   * @return <code>true</code> if it is idempotent; <code>false</code> otherwise
   */
  abstract boolean isIdempotent();

  /**
   * Validates the assignment using the specified context.
   *
   * @author paouelle
   *
   * @param  table the non-<code>null</code> table to validate the assignment with
   * @throws IllegalArgumentException if the assignment is not valid
   */
  abstract void validate(TableInfoImpl<?> table);

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(
      this, ToStringStyle.SHORT_PREFIX_STYLE, true
    );
  }

  /**
   * The <code>DelayedWithObject</code> interface is used by assignments that
   * do not have all their content defined at the time of creation but instead
   * at the time the assignment is added to the UPDATE statement initialized with
   * a POJO.
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
     * Called at the time the assignment is added to an UPDATE statement to
     * complete the assignment based on the POJO in play.
     *
     * @author paouelle
     *
     * @param <T> the type of POJO
     *
     * @param  table the non-<code>null</code> table to process the assignment for
     * @param  context the non-<code>null</code> POJO context associated with
     *         the statement
     * @return a non-<code>null</code> list of new assignment(s) corresponding to
     *         this one
     * @throws IllegalArgumentException if missing mandatory columns are processed
     */
    public <T> List<AssignmentImpl> processWith(
      TableInfoImpl<?> table, ClassInfoImpl<T>.POJOContext context
    );
  }

  /**
   * The <code>WithOldValue</code> interface is used by assignments that keep track
   * of an old value for a column.
   *
   * @copyright 2015-2016 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 7, 2015 - paouelle - Creation
   *
   * @since 1.0
   */
  interface WithOldValue {
    /**
     * Gets the old value to replace.
     *
     * @author paouelle
     *
     * @return the old value to replace
     */
    public Object getOldValue();
  }

  /**
   * The <code>SetAssignmentImpl</code> class defines a "SET" assignment specifying
   * a column name of the POJO and a value.
   *
   * @copyright 2015-2016 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 19, 2015 - paouelle - Creation
   *
   * @since 1.0
   */
  static class SetAssignmentImpl extends AssignmentImpl {
    /**
     * Holds the value to assign to the column name.
     *
     * @author paouelle
     */
    protected volatile Object value;

    /**
     * Holds the definition associated with the value if any.
     *
     * @author paouelle
     */
    private final CQLDataType definition;

    /**
     * Holds the codec associated with the value if any.
     *
     * @author paouelle
     */
    private final TypeCodec<?> codec;

    /**
     * Instantiates a new <code>SetAssignmentImpl</code> object.
     * <p>
     * <i>Note:</i> This constructor is meant to be called by derived POJO
     * assignment such that the value can be plugged-in at the time the assignment
     * is added to the statement.
     *
     * @author paouelle
     *
     * @param  name the column name for this assignment
     * @throws NullPointerException if <code>name</code> is <code>null</code>
     */
    SetAssignmentImpl(CharSequence name) {
      this(name, null, null);
    }

    /**
     * Instantiates a new <code>SetAssignmentImpl</code> object.
     *
     * @author paouelle
     *
     * @param  name the column name for this assignment
     * @param  value the value for this assignment
     * @throws NullPointerException if <code>name</code> is <code>null</code>
     */
    SetAssignmentImpl(CharSequence name, Object value) {
      this(name, value, null);
    }

    /**
     * Instantiates a new <code>SetAssignmentImpl</code> object.
     *
     * @author paouelle
     *
     * @param  name the column name for this assignment
     * @param  tvalue the value and its associated definition for this assignment
     * @throws NullPointerException if <code>name</code> is <code>null</code>
     */
    SetAssignmentImpl(CharSequence name, Triple<Object, CQLDataType, TypeCodec<?>> tvalue) {
      super(name);
      if (tvalue != null) {
        Object value = tvalue.getLeft();

        if (value instanceof Optional) {
          value = ((Optional<?>)value).orElse(null);
        }
        this.value = value;
        this.definition = tvalue.getMiddle();
        this.codec = tvalue.getRight();
      } else {
        this.value = null;
        this.definition = null;
        this.codec = null;
      }
    }

    /**
     * Instantiates a new <code>SetAssignmentImpl</code> object.
     *
     * @author paouelle
     *
     * @param  name the column name for this assignment
     * @param  value the value for this assignment
     * @param  definition the definition associated with the value if any
     * @throws NullPointerException if <code>name</code> is <code>null</code>
     */
    SetAssignmentImpl(CharSequence name, Object value, CQLDataType definition) {
      super(name);
      if (value instanceof Optional) {
        value = ((Optional<?>)value).orElse(null);
      }
      this.value = value;
      this.definition = definition;
      this.codec = null;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.Utils.Appendeable#appendTo(java.lang.String, org.helenus.driver.impl.TableInfoImpl, com.datastax.driver.core.TypeCodec, com.datastax.driver.core.CodecRegistry, java.lang.StringBuilder, java.util.List)
     */
    @Override
    void appendTo(
      String keyspace,
      TableInfoImpl<?> tinfo,
      TypeCodec<?> codec,
      CodecRegistry codecRegistry,
      StringBuilder sb,
      List<Object> variables
    ) {
      Utils.appendName(tinfo, null, codecRegistry, sb, name);
      sb.append("=");
      final FieldInfoImpl<?> field = tinfo.getColumnImpl(name);

      Utils.appendValue(
        (definition != null) ? definition : field.getDataType(),
        (codec != null) ? codec : ((this.codec != null) ? this.codec : field.getCodec(keyspace)),
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
     * @see org.helenus.driver.impl.AssignmentImpl#isIdempotent()
     */
    @Override
    boolean isIdempotent() {
      return Utils.isIdempotent(value);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.AssignmentImpl#validate(org.helenus.driver.impl.TableInfoImpl)
     */
    @Override
    void validate(TableInfoImpl<?> table) {
      table.validateColumnAndValue(name, value);
    }

    /**
     * Gets the value to set.
     *
     * @author paouelle
     *
     * @return the value to set
     */
    Object getValue() {
      return value;
    }
  }

  /**
   * The <code>ReplaceAssignmentImpl</code> class defines a "SET" assignment
   * specifying a column name of the POJO, an old value to be replaced with a
   * new value.
   *
   * @copyright 2015-2016 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 19, 2015 - paouelle - Creation
   *
   * @since 1.0
   */
  static class ReplaceAssignmentImpl
    extends SetAssignmentImpl implements WithOldValue {
    /**
     * Holds the old value to replace for the column name.
     *
     * @author paouelle
     */
    protected volatile Object old;

    /**
     * Instantiates a new <code>ReplaceAssignmentImpl</code> object.
     *
     * @author paouelle
     *
     * @param  name the column name for this assignment
     * @param  value the value for this assignment
     * @param  old the old value to replace for this assignment
     * @throws NullPointerException if <code>name</code> is <code>null</code>
     */
    ReplaceAssignmentImpl(CharSequence name, Object value, Object old) {
      super(name, value);
      if (old instanceof Optional) {
        old = ((Optional<?>)old).orElse(null);
      }
      this.old = old;
    }

    /**
     * Instantiates a new <code>ReplaceAssignmentImpl</code> object.
     *
     * @author paouelle
     *
     * @param  name the column name for this assignment
     * @param  tvalue the value and its definition for this assignment
     * @param  old the old value to replace for this assignment
     * @throws NullPointerException if <code>name</code> is <code>null</code>
     */
    ReplaceAssignmentImpl(
      CharSequence name, Triple<Object, CQLDataType, TypeCodec<?>> tvalue, Object old
    ) {
      super(name, tvalue);
      if (old instanceof Optional) {
        old = ((Optional<?>)old).orElse(null);
      }
      this.old = old;
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
      return super.containsBindMarker() || Utils.containsBindMarker(old);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.AssignmentImpl#isIdempotent()
     */
    @Override
    boolean isIdempotent() {
      return super.isIdempotent() || Utils.isIdempotent(old);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.AssignmentImpl#validate(org.helenus.driver.impl.TableInfoImpl)
     */
    @Override
    void validate(TableInfoImpl<?> table) {
      // special case for assignment as we want to remember optional primary keys set as null
      super.validate(table); // take care of set assignment
      // make sure the old value is valid too
      table.validateColumnAndValue(name, old, true);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.AssignmentImpl.WithOldValue#getOldValue()
     */
    @Override
    public Object getOldValue() {
      return old;
    }
  }

  /**
   * The <code>PreviousAssignmentImpl</code> class defines a hint assignment for
   * the specifying a column name of the POJO to its old value being replaced.
   *
   * @copyright 2015-2016 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 7, 2016 - paouelle - Creation
   *
   * @since 1.0
   */
  static class PreviousAssignmentImpl
    extends AssignmentImpl implements WithOldValue {
    /**
     * Holds the old value to replace for the column name.
     *
     * @author paouelle
     */
    protected volatile Object old;

    /**
     * Instantiates a new <code>PreviousAssignmentImpl</code> object.
     *
     * @author paouelle
     *
     * @param  name the column name for this assignment
     * @param  old the old value to replace for this assignment
     * @throws NullPointerException if <code>name</code> is <code>null</code>
     */
    PreviousAssignmentImpl(CharSequence name, Object old) {
      super(name);
      if (old instanceof Optional) {
        old = ((Optional<?>)old).orElse(null);
      }
      this.old = old;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.Utils.Appendeable#appendTo(java.lang.String, org.helenus.driver.impl.TableInfoImpl, com.datastax.driver.core.TypeCodec, com.datastax.driver.core.CodecRegistry, java.lang.StringBuilder, java.util.List)
     */
    @Override
    void appendTo(
      String keyspace,
      TableInfoImpl<?> tinfo,
      TypeCodec<?> codec,
      CodecRegistry codecRegistry,
      StringBuilder sb,
      List<Object> variables
    ) {}

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.Utils.Appendeable#containsBindMarker()
     */
    @Override
    boolean containsBindMarker() {
      return false; // this statement is never used towards Cassandra
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.AssignmentImpl#isIdempotent()
     */
    @Override
    boolean isIdempotent() {
      return true; // this statement is never used towards Cassandra
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.AssignmentImpl#validate(org.helenus.driver.impl.TableInfoImpl)
     */
    @Override
    void validate(TableInfoImpl<?> table) {
      table.validateColumnAndValue(name, old, true);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.AssignmentImpl.WithOldValue#getOldValue()
     */
    @Override
    public Object getOldValue() {
      return old;
    }
  }

  /**
   * The <code>DelayedSetAssignmentImpl</code> class defines a "SET" assignment
   * specifying a column name of the POJO and where the value is extracted from
   * the POJO in play.
   *
   * @copyright 2015-2016 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 19, 2015 - paouelle - Creation
   *
   * @since 1.0
   */
  static class DelayedSetAssignmentImpl
    extends SetAssignmentImpl implements DelayedWithObject {
    /**
     * Holds the POJO from which to get the non-primary column value or
     * <code>null</code> if this assignment is used as part of a POJO-based
     * UPDATE statement.
     *
     * @author paouelle
     */
    final Object object;

    /**
     * Instantiates a new <code>DelayedSetAssignmentImpl</code> object.
     *
     * @author paouelle
     *
     * @param  name the column name for this assignment
     * @throws NullPointerException if <code>name</code> is <code>null</code>
     */
    DelayedSetAssignmentImpl(CharSequence name) {
      this(null, name);
    }

    /**
     * Instantiates a new <code>DelayedSetAssignmentImpl</code> object.
     *
     * @author paouelle
     *
     * @param  object the POJO from which to extract the non-primary column
     *         value
     * @param  name the column name for this assignment
     * @throws NullPointerException if <code>name</code> is <code>null</code>
     */
    DelayedSetAssignmentImpl(Object object, CharSequence name) {
      super(name);
      this.object = object;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.AssignmentImpl.DelayedWithObject#processWith(org.helenus.driver.impl.TableInfoImpl, org.helenus.driver.impl.ClassInfoImpl.POJOContext)
     */
    @SuppressWarnings({"cast", "unchecked", "rawtypes"})
    @Override
    public <T> List<AssignmentImpl> processWith(
      TableInfoImpl<?> table, ClassInfoImpl<T>.POJOContext context
    ) {
      if (object != null) {
        org.apache.commons.lang3.Validate.isTrue(
          context.getObjectClass().isInstance(object),
          "setAll() object class '%s' is not compatible with POJO class '%s'",
          object.getClass().getName(),
          context.getObjectClass().getName()
        );
        // get a POJO context for the POJO passed on the setFrom()
        context = context.getClassInfo().newContext((T)object);
      }
      if (table.getColumnImpl(name) == null) { // column not defined in the table
        return Collections.emptyList();
      }
      return (List<AssignmentImpl>)(List)Arrays.asList(
        new SetAssignmentImpl(name, context.getColumnValue(table.getName(), name))
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
      return false; // the value will be extracted from the pojo and that doesn't have bind markers by design
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.AssignmentImpl.SetAssignmentImpl#isIdempotent()
     */
    @Override
    boolean isIdempotent() {
      return true; // the value will be extracted from the pojo and that is idempotent by design
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.AssignmentImpl.SetAssignmentImpl#validate(org.helenus.driver.impl.TableInfoImpl)
     */
    @Override
    void validate(TableInfoImpl<?> table) {
      throw new IllegalStateException("should not be called");
    }
  }

  /**
   * The <code>DelayedReplaceAssignmentImpl</code> class defines a "SET" assignment
   * specifying a column name of the POJO, an old value to be replaced with a value
   * extracted from the POJO in play.
   *
   * @copyright 2015-2016 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - May 26, 2016 - paouelle - Creation
   *
   * @since 1.0
   */
  static class DelayedReplaceAssignmentImpl
    extends DelayedSetAssignmentImpl implements WithOldValue {
    /**
     * Holds the old value to replace for the column name.
     *
     * @author paouelle
     */
    protected volatile Object old;

    /**
     * Instantiates a new <code>DelayedReplaceAssignmentImpl</code> object.
     *
     * @author paouelle
     *
     * @param  name the column name for this assignment
     * @param  old the old value to replace for this assignment
     * @throws NullPointerException if <code>name</code> is <code>null</code>
     */
    DelayedReplaceAssignmentImpl(CharSequence name, Object old) {
      super(name);
      if (old instanceof Optional) {
        old = ((Optional<?>)old).orElse(null);
      }
      this.old = old;
    }

    /**
     * Instantiates a new <code>DelayedReplaceAssignmentImpl</code> object.
     *
     * @author paouelle
     *
     * @param  object the POJO from which to extract the non-primary column
     *         value
     * @param  name the column name for this assignment
     * @param  old the old value to replace for this assignment
     * @throws NullPointerException if <code>name</code> is <code>null</code>
     */
    DelayedReplaceAssignmentImpl(Object object, CharSequence name, Object old) {
      super(object, name);
      if (old instanceof Optional) {
        old = ((Optional<?>)old).orElse(null);
      }
      this.old = old;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.AssignmentImpl.DelayedWithObject#processWith(org.helenus.driver.impl.TableInfoImpl, org.helenus.driver.impl.ClassInfoImpl.POJOContext)
     */
    @SuppressWarnings({"cast", "unchecked", "rawtypes"})
    @Override
    public <T> List<AssignmentImpl> processWith(
      TableInfoImpl<?> table, ClassInfoImpl<T>.POJOContext context
    ) {
      if (object != null) {
        org.apache.commons.lang3.Validate.isTrue(
          context.getObjectClass().isInstance(object),
          "setAll() object class '%s' is not compatible with POJO class '%s'",
          object.getClass().getName(),
          context.getObjectClass().getName()
        );
        // get a POJO context for the POJO passed on the setFrom()
        context = context.getClassInfo().newContext((T)object);
      }
      if (table.getColumnImpl(name) == null) { // column not defined in the table
        return Collections.emptyList();
      }
      Triple<Object, CQLDataType, TypeCodec<?>> neval;

      try {
        neval = context.getColumnValue(table.getName(), name);
      } catch (EmptyOptionalPrimaryKeyException e) {
        // special case where we still want to let the assignment go through
        // as we will at least generate a delete for the previous value
        neval = null;
      }
      return (List<AssignmentImpl>)(List)Arrays.asList(
        new ReplaceAssignmentImpl(name, neval, old)
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
      return Utils.containsBindMarker(old);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.AssignmentImpl#isIdempotent()
     */
    @Override
    boolean isIdempotent() {
      return Utils.isIdempotent(old);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.AssignmentImpl.WithOldValue#getOldValue()
     */
    @Override
    public Object getOldValue() {
      return old;
    }
  }

  /**
   * The <code>DelayedSetAllAssignmentImpl</code> class defines a series of "SET"
   * assignment specifying all non-primary key columns of the POJO in play. The
   * column names and values will actually be filled at the time the assignment
   * is added to the statement.
   *
   * @copyright 2015-2016 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 19, 2015 - paouelle - Creation
   *
   * @since 1.0
   */
  static class DelayedSetAllAssignmentImpl
    extends AssignmentImpl implements DelayedWithObject {
    /**
     * Holds the POJO from which to get the non-primary column values or
     * <code>null</code> if this assignment is used as part of a POJO-based
     * UPDATE statement.
     *
     * @author paouelle
     */
    final Object object;

    /**
     * Instantiates a new <code>DelayedSetAllAssignmentImpl</code> object.
     *
     * @author paouelle
     */
    DelayedSetAllAssignmentImpl() {
      this(null);
    }

    /**
     * Instantiates a new <code>DelayedSetAllAssignmentImpl</code> object.
     *
     * @author paouelle
     *
     * @param object the POJO from which to extract the non-primary column
     *        values
     */
    DelayedSetAllAssignmentImpl(Object object) {
      super(""); // name is not important at this stage
      this.object = object;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.Utils.Appendeable#appendTo(java.lang.String, org.helenus.driver.impl.TableInfoImpl, com.datastax.driver.core.TypeCodec, com.datastax.driver.core.CodecRegistry, java.lang.StringBuilder, java.util.List)
     */
    @Override
    void appendTo(
      String keyspace,
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
     * @see org.helenus.driver.impl.AssignmentImpl.DelayedWithObject#processWith(org.helenus.driver.impl.TableInfoImpl, org.helenus.driver.impl.ClassInfoImpl.POJOContext)
     */
    @SuppressWarnings("unchecked")
    @Override
    public <T> List<AssignmentImpl> processWith(
      TableInfoImpl<?> table, ClassInfoImpl<T>.POJOContext context
    ) {
      final List<AssignmentImpl> assignments = new ArrayList<>(25);

      if (object != null) {
        org.apache.commons.lang3.Validate.isTrue(
          context.getObjectClass().isInstance(object),
          "setAll() object class '%s' is not compatible with POJO class '%s'",
          object.getClass().getName(),
          context.getObjectClass().getName()
        );
        // get a POJO context for the POJO passed on the setAllFrom()
        context = context.getClassInfo().newContext((T)object);
      }
      for (final Map.Entry<String, Triple<Object, CQLDataType, TypeCodec<?>>> e: context.getNonPrimaryKeyColumnValues(
        table.getName()
      ).entrySet()) {
        assignments.add(new SetAssignmentImpl(e.getKey(), e.getValue()));
      }
      return assignments;
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
      return false; // the values will be extracted from the pojo and that doesn't contain bind markers by design
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.AssignmentImpl#isIdempotent()
     */
    @Override
    boolean isIdempotent() {
      return true; // the values will be extracted from the pojo and that is idempotent by design
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.AssignmentImpl#validate(org.helenus.driver.impl.TableInfoImpl)
     */
    @Override
    void validate(TableInfoImpl<?> table) {
      throw new IllegalStateException("should not be called");
    }
  }

  /**
   * The <code>CounterAssignmentImpl</code> class defines a counter-specific
   * increment or decrement assignment.
   *
   * @copyright 2015-2016 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 19, 2015 - paouelle - Creation
   *
   * @since 1.0
   */
  static class CounterAssignmentImpl extends AssignmentImpl {
    /**
     * Holds the increment/decrement value or a bind marker representing the value.
     *
     * @author paouelle
     */
    private final Object value;

    /**
     * Flag indicating if it is an increment (<code>true</code>) or a decrement
     * (<code>false</code>).
     *
     * @author paouelle
     */
    private final boolean isIncr;

    /**
     * Instantiates a new <code>CounterAssignmentImpl</code> object.
     *
     * @author paouelle
     *
     * @param  name the counter column name for this assignment
     * @param  value the increment value
     * @param  isIncr <code>true</code> if it is an increment; <code>false</code>
     *         if it is a decrement
     * @throws NullPointerException if <code>name</code> is <code>null</code>
     */
    CounterAssignmentImpl(CharSequence name, long value, boolean isIncr) {
      super(name);
      if (!isIncr && (value < 0L)) {
        this.value = -value;
        this.isIncr = true;
      } else {
        this.value = value;
        this.isIncr = isIncr;
      }
    }

    /**
     * Instantiates a new <code>CounterAssignmentImpl</code> object.
     *
     * @author paouelle
     *
     * @param  name the counter column name for this assignment
     * @param  marker bind marker for the increment value
     * @param  isIncr <code>true</code> if it is an increment; <code>false</code>
     *         if it is a decrement
     * @throws NullPointerException if <code>name</code> is <code>null</code>
     */
    CounterAssignmentImpl(CharSequence name, BindMarker marker, boolean isIncr) {
      super(name);
      this.value = marker;
      this.isIncr = isIncr;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.Utils.Appendeable#appendTo(java.lang.String, org.helenus.driver.impl.TableInfoImpl, com.datastax.driver.core.TypeCodec, com.datastax.driver.core.CodecRegistry, java.lang.StringBuilder, java.util.List)
     */
    @Override
    void appendTo(
      String keyspace,
      TableInfoImpl<?> tinfo,
      TypeCodec<?> codec,
      CodecRegistry codecRegistry,
      StringBuilder sb,
      List<Object> variables
    ) {
      final FieldInfoImpl<?> finfo = tinfo.getColumnImpl(name);
      final StringBuilder nsb = new StringBuilder(80);

      Utils.appendName(tinfo, null, codecRegistry, nsb, name);
      sb.append(nsb).append('=').append(nsb).append(isIncr ? '+' : '-');
      Utils.appendValue(
        finfo.getDataType(),
        (codec != null) ? codec : finfo.getCodec(keyspace),
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
     * @see org.helenus.driver.impl.AssignmentImpl#isIdempotent()
     */
    @Override
    boolean isIdempotent() {
      return false;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.AssignmentImpl#validate(org.helenus.driver.impl.TableInfoImpl)
     */
    @Override
    void validate(TableInfoImpl<?> table) {
      table.validateCounterColumn(name);
    }
  }

  /**
   * The <code>ListPrependAssignmentImpl</code> class defines an assignment that
   * preprends elements to a list.
   *
   * @copyright 2015-2016 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 19, 2015 - paouelle - Creation
   *
   * @since 1.0
   */
  static class ListPrependAssignmentImpl extends AssignmentImpl {
    /**
     * Holds the elements to prepend to the list or a bind marker representing
     * them.
     *
     * @author paouelle
     */
    private final Object value;

    /**
     * Instantiates a new <code>ListPrependAssignmentImpl</code> object.
     *
     * @author paouelle
     *
     * @param  name the column name for this assignment
     * @param  value the elements to prepend or a bind marker representing them
     * @throws NullPointerException if <code>name</code> is <code>null</code>
     */
    ListPrependAssignmentImpl(CharSequence name, Object value) {
      super(name);
      this.value = value;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.Utils.Appendeable#appendTo(java.lang.String, org.helenus.driver.impl.TableInfoImpl, com.datastax.driver.core.TypeCodec, com.datastax.driver.core.CodecRegistry, java.lang.StringBuilder, java.util.List)
     */
    @Override
    void appendTo(
      String keyspace,
      TableInfoImpl<?> tinfo,
      TypeCodec<?> codec,
      CodecRegistry codecRegistry,
      StringBuilder sb,
      List<Object> variables
    ) {
      final FieldInfoImpl<?> finfo = tinfo.getColumnImpl(name);
      final StringBuilder nsb = new StringBuilder(80);

      Utils.appendName(tinfo, null, codecRegistry, nsb, name);
      sb.append(nsb).append('=');
      Utils.appendList(
        finfo.getDataType(),
        (codec != null) ? codec : finfo.getCodec(keyspace),
        codecRegistry,
        sb,
        (List<?>)value,
        variables
      );
      sb.append('+').append(nsb);
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
     * @see org.helenus.driver.impl.AssignmentImpl#isIdempotent()
     */
    @Override
    boolean isIdempotent() {
      return false;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.AssignmentImpl#validate(org.helenus.driver.impl.TableInfoImpl)
     */
    @Override
    void validate(TableInfoImpl<?> table) {
      table.validateListColumnAndValue(name, value);
    }
  }

  /**
   * The <code>ListSetIdxAssignmentImpl</code> class defines an assignment that
   * allows replacement of a specific index in a list.
   *
   * @copyright 2015-2016 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 19, 2015 - paouelle - Creation
   *
   * @since 1.0
   */
  static class ListSetIdxAssignmentImpl extends AssignmentImpl {
    /**
     * Holds the index of the element to replace.
     *
     * @author paouelle
     */
    private final int idx;

    /**
     * Holds the new element's value.
     *
     * @author paouelle
     */
    private final Object value;

    /**
     * Instantiates a new <code>ListSetIdxAssignmentImpl</code> object.
     *
     * @author paouelle
     *
     * @param  name the column name for this assignment
     * @param  idx the index of the element to replace
     * @param  value the new element's value
     * @throws NullPointerException if <code>name</code> is <code>null</code>
     */
    ListSetIdxAssignmentImpl(CharSequence name, int idx, Object value) {
      super(name);
      this.idx = idx;
      this.value = value;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.Utils.Appendeable#appendTo(java.lang.String, org.helenus.driver.impl.TableInfoImpl, com.datastax.driver.core.TypeCodec, com.datastax.driver.core.CodecRegistry, java.lang.StringBuilder, java.util.List)
     */
    @Override
    void appendTo(
      String keyspace,
      TableInfoImpl<?> tinfo,
      TypeCodec<?> codec,
      CodecRegistry codecRegistry,
      StringBuilder sb,
      List<Object> variables
    ) {
      Utils.appendName(tinfo, null, codecRegistry, sb, name)
        .append('[').append(idx).append("]=");
      final FieldInfoImpl<?> finfo = tinfo.getColumnImpl(name);

      Utils.appendValue(
        finfo.getDataType().getElementType(),
        (codec != null) ? codec : ((ArgumentsCodec<?>)finfo.getCodec(keyspace)).codec(0),
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
     * @see org.helenus.driver.impl.AssignmentImpl#isIdempotent()
     */
    @Override
    boolean isIdempotent() {
      return true;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.AssignmentImpl#validate(org.helenus.driver.impl.TableInfoImpl)
     */
    @Override
    void validate(TableInfoImpl<?> table) {
      table.validateListColumnAndValue(name, value);
    }
  }

  /**
   * The <code>CollectionAssignmentImpl</code> class defines an assignment that
   * enables new/existing elements to be appended/discarded to/from a list or a
   * set.
   *
   * @copyright 2015-2016 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 19, 2015 - paouelle - Creation
   *
   * @since 1.0
   */
  static class CollectionAssignmentImpl extends AssignmentImpl {
    /**
     * Holds the collection type.
     *
     * @author paouelle
     */
    private final DataType ctype;

    /**
     * Holds the elements to be appended/discarded to/from the list or set.
     *
     * @author paouelle
     */
    private final Object collection;

    /**
     * Flag indicating if the elements are appended (<code>true</code>) or
     * discarded (<code>false</code>) to/from the list or set.
     *
     * @author paouelle
     */
    private final boolean isAdd;

    /**
     * Holds a flag indicating if this collection statement is idempotent.
     *
     * @author paouelle
     */
    private final boolean isIdempotent;

    /**
     * Instantiates a new <code>CollectionAssignmentImpl</code> object.
     *
     * @author paouelle
     *
     * @param  ctype the non-<code>null</code> collection type
     * @param  name the column name for this assignment
     * @param  collection the elements to be appended or discarded
     * @param  isAdd <code>true</code> if the elements are appended; <code>false</code>
     *         if they should be discarded
     * @param  isIdempotent whether this assignment is idempotent or not
     * @throws NullPointerException if <code>name</code> is <code>null</code>
     */
    CollectionAssignmentImpl(
      DataType ctype,
      CharSequence name,
      Object collection,
      boolean isAdd,
      boolean isIdempotent
    ) {
      super(name);
      this.ctype = ctype;
      this.collection = collection;
      this.isAdd = isAdd;
      this.isIdempotent = isIdempotent;
    }

    /**
     * Instantiates a new <code>CollectionAssignmentImpl</code> object.
     *
     * @author paouelle
     *
     * @param  ctype the non-<code>null</code> collection type
     * @param  name the column name for this assignment
     * @param  collection the elements to be appended or discarded
     * @param  isAdd <code>true</code> if the elements are appended; <code>false</code>
     *         if they should be discarded
     * @throws NullPointerException if <code>name</code> is <code>null</code>
     */
    CollectionAssignmentImpl(
      DataType ctype,
      CharSequence name,
      Object collection,
      boolean isAdd
    ) {
      this(ctype, name, collection, isAdd, true);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.Utils.Appendeable#appendTo(java.lang.String, org.helenus.driver.impl.TableInfoImpl, com.datastax.driver.core.TypeCodec, com.datastax.driver.core.CodecRegistry, java.lang.StringBuilder, java.util.List)
     */
    @Override
    void appendTo(
      String keyspace,
      TableInfoImpl<?> tinfo,
      TypeCodec<?> codec,
      CodecRegistry codecRegistry,
      StringBuilder sb,
      List<Object> variables
    ) {
      final FieldInfoImpl<?> finfo = tinfo.getColumnImpl(name);
      final StringBuilder nsb = new StringBuilder(80);

      Utils.appendName(tinfo, null, codecRegistry, nsb, name);
      sb.append(nsb).append('=').append(nsb).append(isAdd ? '+' : '-');
      Utils.appendValue(
        finfo.getDataType(),
        (codec != null) ? codec : finfo.getCodec(keyspace),
        codecRegistry,
        sb,
        collection,
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
      return Utils.containsBindMarker(collection);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.AssignmentImpl#isIdempotent()
     */
    @Override
    boolean isIdempotent() {
      return isIdempotent;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.AssignmentImpl#validate(org.helenus.driver.impl.TableInfoImpl)
     */
    @Override
    void validate(TableInfoImpl<?> table) {
      if ((ctype == DataType.MAP)
          || (ctype == DataType.SORTED_MAP)) {
        table.validateMapColumnAndKeyValues(name, (Map<?, ?>)collection);
      } else {
        final FieldInfoImpl<?> finfo = table.getColumnImpl(name);

        if ((finfo != null)
            && ((finfo.getDataType().getMainType() == DataType.MAP)
                || (finfo.getDataType().getMainType() == DataType.SORTED_MAP))) {
          table.validateMapColumnAndKeys(name, (Iterable<?>)collection);
        } else if ((ctype == DataType.SET)
                   || (ctype == DataType.ORDERED_SET)
                   || (ctype == DataType.SORTED_SET)) {
          table.validateSetColumnAndValues(name, (Iterable<?>)collection);
        } else { // must be a list
          table.validateListColumnAndValues(name, (Iterable<?>)collection);
        }
      }
    }
  }

  /**
   * The <code>MapPutAssignmentImpl</code> class defines an assignment which provides
   * the ability to add mappings in a map.
   *
   * @copyright 2015-2016 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 19, 2015 - paouelle - Creation
   *
   * @since 1.0
   */
  static class MapPutAssignmentImpl extends AssignmentImpl {
    /**
     * Holds the key of the mapping to be added.
     *
     * @author paouelle
     */
    private final Object key;

    /**
     * Holds the value of the mapping to be added.
     *
     * @author paouelle
     */
    private final Object value;

    /**
     * Instantiates a new <code>MapPutAssignmentImpl</code> object.
     *
     * @author paouelle
     *
     * @param  name the column name for this assignment
     * @param  key the key of the mapping to be added
     * @param  value the value of the mapping to be added
     * @throws NullPointerException if <code>name</code> is <code>null</code>
     */
    MapPutAssignmentImpl(CharSequence name, Object key, Object value) {
      super(name);
      this.key = key;
      this.value = value;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.Utils.Appendeable#appendTo(java.lang.String, org.helenus.driver.impl.TableInfoImpl, com.datastax.driver.core.TypeCodec, com.datastax.driver.core.CodecRegistry, java.lang.StringBuilder, java.util.List)
     */
    @Override
    void appendTo(
      String keyspace,
      TableInfoImpl<?> tinfo,
      TypeCodec<?> codec,
      CodecRegistry codecRegistry,
      StringBuilder sb,
      List<Object> variables
    ) {
      Utils.appendName(tinfo, null, codecRegistry, sb, name).append('[');
      final FieldInfoImpl<?> finfo = tinfo.getColumnImpl(name);

      Utils.appendValue(
        finfo.getDataType().getFirstArgumentType(),
        (codec != null) ? codec : ((ArgumentsCodec<?>)finfo.getCodec(keyspace)).codec(0),
        codecRegistry,
        sb,
        key,
        variables
      );
      sb.append("]=");
      Utils.appendValue(
        finfo.getDataType().getElementType(),
        (codec != null) ? codec : ((ArgumentsCodec<?>)finfo.getCodec(keyspace)).codec(1),
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
      return Utils.containsBindMarker(key) || Utils.containsBindMarker(value);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.AssignmentImpl#isIdempotent()
     */
    @Override
    boolean isIdempotent() {
      return true;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.AssignmentImpl#validate(org.helenus.driver.impl.TableInfoImpl)
     */
    @Override
    void validate(TableInfoImpl<?> table) {
      table.validateMapColumnAndKeyValue(name, key, value);
    }
  }
}

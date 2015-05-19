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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.github.helenusdriver.driver.Assignment;
import com.github.helenusdriver.driver.ColumnPersistenceException;
import com.github.helenusdriver.persistence.DataType;

/**
 * The <code>AssignmentImpl</code> class extends the functionality of Cassandra's
 * {@link com.datastax.driver.core.querybuilder.Assignment} class to provide
 * support for POJOs.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 19, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
@lombok.ToString
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
   * Validates the assignment using the specified context.
   *
   * @author paouelle
   *
   * @param  table the non-<code>null</code> table to validate the assignment with
   * @throws IllegalArgumentException if the assignment is not valid
   */
  abstract void validate(TableInfoImpl<?> table);

  /**
   * The <code>DelayedWithObject</code> interface is used by assignments that
   * do not have all their content defined at the time of creation but instead
   * at the time the assignment is added to the UPDATE statement initialized with
   * a POJO.
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
     * @throws ColumnPersistenceException if unable to persist a column's value
     */
    public <T> List<AssignmentImpl> processWith(
      TableInfoImpl<?> table, ClassInfoImpl<T>.POJOContext context
    );
  }

  /**
   * The <code>SetAssignmentImpl</code> class defines a "SET" assignment specifying
   * a column name of the POJO and a value.
   *
   * @copyright 2015-2015 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 19, 2015 - paouelle - Creation
   *
   * @since 1.0
   */
  @lombok.ToString(callSuper=true)
  static class SetAssignmentImpl extends AssignmentImpl {
    /**
     * Holds the value to assign to the column name.
     *
     * @author paouelle
     */
    protected volatile Object value;

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
      this(name, null);
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
      super(name);
      if (value instanceof Optional) {
        value = ((Optional<?>)value).orElse(null);
      }
      this.value = value;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.github.helenusdriver.driver.impl.Utils.Appendeable#appendTo(com.github.helenusdriver.driver.impl.TableInfoImpl, java.lang.StringBuilder)
     */
    @Override
    void appendTo(TableInfoImpl<?> tinfo, StringBuilder sb) {
      Utils.appendName(name, sb);
      sb.append("=");
      final FieldInfoImpl<?> field = tinfo.getColumn(name);

      Utils.appendValue(field.encodeValue(value), sb);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.github.helenusdriver.driver.impl.AssignmentImpl#validate(com.github.helenusdriver.driver.impl.TableInfoImpl)
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
   * @copyright 2015-2015 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 19, 2015 - paouelle - Creation
   *
   * @since 1.0
   */
  @lombok.ToString(callSuper=true)
  static class ReplaceAssignmentImpl extends SetAssignmentImpl {
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
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.github.helenusdriver.driver.impl.AssignmentImpl#validate(com.github.helenusdriver.driver.impl.TableInfoImpl)
     */
    @Override
    void validate(TableInfoImpl<?> table) {
      // special case for assignment as we want to remember optional primary keys set as null
      super.validate(table); // take care of set assignment
      // make sure the old value is valid too
      table.validateColumnAndValue(name, old);
    }

    /**
     * Gets the old value to replace.
     *
     * @author paouelle
     *
     * @return the old value to replace
     */
    Object getOldValue() {
      return old;
    }
  }

  /**
   * The <code>DelayedSetAssignmentImpl</code> class defines a "SET" assignment
   * specifying a column name of the POJO and where the value is extracted from
   * the POJO in play.
   *
   * @copyright 2015-2015 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 19, 2015 - paouelle - Creation
   *
   * @since 1.0
   */
  @lombok.ToString(callSuper=true)
  static class DelayedSetAssignmentImpl extends SetAssignmentImpl implements DelayedWithObject {
    /**
     * Holds the POJO from which to get the non-primary column value or
     * <code>null</code> if this assignment is used as part of a POJO-based
     * UPDATE statement.
     *
     * @author paouelle
     */
    private final Object object;

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
     * @see com.github.helenusdriver.driver.impl.AssignmentImpl.DelayedWithObject#processWith(com.github.helenusdriver.driver.impl.TableInfoImpl, com.github.helenusdriver.driver.impl.ClassInfoImpl.POJOContext)
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
      if (table.getColumn(name) == null) { // column not defined in the table
        return Collections.emptyList();
      }
      return (List<AssignmentImpl>)(List)Arrays.asList(
        new SetAssignmentImpl(name, context.getColumnNonEncodedValue(table.getName(), name))
      );
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.github.helenusdriver.driver.impl.AssignmentImpl.SetAssignmentImpl#validate(com.github.helenusdriver.driver.impl.TableInfoImpl)
     */
    @Override
    void validate(TableInfoImpl<?> table) {
      throw new IllegalStateException("should not be called");
    }
  }

  /**
   * The <code>DelayedSetAllAssignmentImpl</code> class defines a series of "SET"
   * assignment specifying all non-primary key columns of the POJO in play. The
   * column names and values will actually be filled at the time the assignment
   * is added to the statement.
   *
   * @copyright 2015-2015 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 19, 2015 - paouelle - Creation
   *
   * @since 1.0
   */
  @lombok.ToString(callSuper=true)
  static class DelayedSetAllAssignmentImpl extends AssignmentImpl implements DelayedWithObject {
    /**
     * Holds the POJO from which to get the non-primary column values or
     * <code>null</code> if this assignment is used as part of a POJO-based
     * UPDATE statement.
     *
     * @author paouelle
     */
    private final Object object;

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
     * @see com.github.helenusdriver.driver.impl.Utils.Appendeable#appendTo(com.github.helenusdriver.driver.impl.TableInfoImpl, java.lang.StringBuilder)
     */
    @Override
    void appendTo(TableInfoImpl<?> tinfo, StringBuilder sb) {
      throw new IllegalStateException("should not be called");
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.github.helenusdriver.driver.impl.AssignmentImpl.DelayedWithObject#processWith(com.github.helenusdriver.driver.impl.TableInfoImpl, com.github.helenusdriver.driver.impl.ClassInfoImpl.POJOContext)
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
      for (final Map.Entry<String, Object> e: context.getNonPrimaryKeyColumnNonEncodedValues(
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
     * @see com.github.helenusdriver.driver.impl.AssignmentImpl#validate(com.github.helenusdriver.driver.impl.TableInfoImpl)
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
   * @copyright 2015-2015 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 19, 2015 - paouelle - Creation
   *
   * @since 1.0
   */
  @lombok.ToString(callSuper=true)
  static class CounterAssignmentImpl extends AssignmentImpl {
    /**
     * Holds the increment value.
     *
     * @author paouelle
     */
    private final long value;

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
     * @param name the counter column name for this assignment
     * @param value the increment value
     * @param isIncr <code>true</code> if it is an increment; <code>false</code>
     *        if it is a decrement
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
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.github.helenusdriver.driver.impl.Utils.Appendeable#appendTo(com.github.helenusdriver.driver.impl.TableInfoImpl, java.lang.StringBuilder)
     */
    @Override
    void appendTo(TableInfoImpl<?> tinfo, StringBuilder sb) {
      Utils.appendName(name, sb).append("=");
      Utils.appendName(name, sb).append(isIncr ? "+" : "-").append(value);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.github.helenusdriver.driver.impl.AssignmentImpl#validate(com.github.helenusdriver.driver.impl.TableInfoImpl)
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
   * @copyright 2015-2015 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 19, 2015 - paouelle - Creation
   *
   * @since 1.0
   */
  @lombok.ToString(callSuper=true)
  static class ListPrependAssignmentImpl extends AssignmentImpl {
    /**
     * Holds the elements to prepend to the list.
     *
     * @author paouelle
     */
    private final List<?> value;

    /**
     * Instantiates a new <code>ListPrependAssignmentImpl</code> object.
     *
     * @author paouelle
     *
     * @param  name the column name for this assignment
     * @param  value the elements to prepend
     * @throws NullPointerException if <code>name</code> is <code>null</code>
     */
    ListPrependAssignmentImpl(CharSequence name, List<?> value) {
      super(name);
      this.value = value;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.github.helenusdriver.driver.impl.Utils.Appendeable#appendTo(com.github.helenusdriver.driver.impl.TableInfoImpl, java.lang.StringBuilder)
     */
    @Override
    void appendTo(TableInfoImpl<?> tinfo, StringBuilder sb) {
      Utils.appendName(name, sb).append("=");
      final FieldInfoImpl<?> finfo = tinfo.getColumn(name);

      Utils.appendList((List<?>)finfo.encodeValue(value), sb);
      sb.append("+");
      Utils.appendName(name, sb);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.github.helenusdriver.driver.impl.AssignmentImpl#validate(com.github.helenusdriver.driver.impl.TableInfoImpl)
     */
    @Override
    void validate(TableInfoImpl<?> table) {
      table.validateCollectionColumnAndValue(name, DataType.LIST, value);
    }
  }

  /**
   * The <code>ListSetIdxAssignmentImpl</code> class defines an assignment that
   * allows replacement of a specific index in a list.
   *
   * @copyright 2015-2015 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 19, 2015 - paouelle - Creation
   *
   * @since 1.0
   */
  @lombok.ToString(callSuper=true)
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
     * @see com.github.helenusdriver.driver.impl.Utils.Appendeable#appendTo(com.github.helenusdriver.driver.impl.TableInfoImpl, java.lang.StringBuilder)
     */
    @Override
    void appendTo(TableInfoImpl<?> tinfo, StringBuilder sb) {
      Utils.appendName(name, sb).append("[").append(idx).append("]=");
      final FieldInfoImpl<?> finfo = tinfo.getColumn(name);

      Utils.appendValue(finfo.encodeElementValue(value), sb);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.github.helenusdriver.driver.impl.AssignmentImpl#validate(com.github.helenusdriver.driver.impl.TableInfoImpl)
     */
    @Override
    void validate(TableInfoImpl<?> table) {
      table.validateCollectionColumnAndValue(name, DataType.LIST, value);
    }
  }

  /**
   * The <code>CollectionAssignmentImpl</code> class defines an assignment that
   * enables new/existing elements to be appended/discarded to/from a list or a
   * set.
   *
   * @copyright 2015-2015 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 19, 2015 - paouelle - Creation
   *
   * @since 1.0
   */
  @lombok.ToString(callSuper=true)
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
      DataType ctype, CharSequence name, Object collection, boolean isAdd
    ) {
      super(name);
      this.ctype = ctype;
      this.collection = collection;
      this.isAdd = isAdd;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.github.helenusdriver.driver.impl.Utils.Appendeable#appendTo(com.github.helenusdriver.driver.impl.TableInfoImpl, java.lang.StringBuilder)
     */
    @Override
    void appendTo(TableInfoImpl<?> tinfo, StringBuilder sb) {
      Utils.appendName(name, sb).append("=");
      Utils.appendName(name, sb).append(isAdd ? "+" : "-");
      final FieldInfoImpl<?> finfo = tinfo.getColumn(name);

      Utils.appendCollection(finfo.encodeValue(collection), sb, null);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.github.helenusdriver.driver.impl.AssignmentImpl#validate(com.github.helenusdriver.driver.impl.TableInfoImpl)
     */
    @Override
    void validate(TableInfoImpl<?> table) {
      if (ctype == DataType.MAP) {
        table.validateMapColumnAndKeyValues(name, (Map<?, ?>)collection);
      } else {
        final FieldInfoImpl<?> finfo = table.getColumn(name);

        if ((finfo != null) && (finfo.getDataType().getType() == DataType.MAP)) {
          table.validateMapColumnAndKeys(name, (Collection<?>)collection);
        } else {
          table.validateCollectionColumnAndValues(
            name, ctype, (Collection<?>)collection
          );
        }
      }
    }
  }

  /**
   * The <code>MapPutAssignmentImpl</code> class defines an assignment which provides
   * the ability to add mappings in a map.
   *
   * @copyright 2015-2015 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 19, 2015 - paouelle - Creation
   *
   * @since 1.0
   */
  @lombok.ToString(callSuper=true)
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
     * @see com.github.helenusdriver.driver.impl.Utils.Appendeable#appendTo(com.github.helenusdriver.driver.impl.TableInfoImpl, java.lang.StringBuilder)
     */
    @Override
    void appendTo(TableInfoImpl<?> tinfo, StringBuilder sb) {
      Utils.appendName(name, sb).append("[");
      Utils.appendValue(key, sb);
      sb.append("]=");
      final FieldInfoImpl<?> finfo = tinfo.getColumn(name);

      // paouelle: 03/06/15 - I think this is a bug, it should be encoded as an
      // element of the map and not the type of the map as such, it should be
      // using the encodeElementValue and we should make sure that the
      // encodeElementValue, properly support MAP in to the definition.encodeElement()
      // --> Utils.appendValue(finfo.encodeValue(value), sb);
      Utils.appendValue(finfo.encodeElementValue(value), sb);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.github.helenusdriver.driver.impl.AssignmentImpl#validate(com.github.helenusdriver.driver.impl.TableInfoImpl)
     */
    @Override
    void validate(TableInfoImpl<?> table) {
      table.validateMapColumnAndKeyValue(name, key, value);
    }
  }
}

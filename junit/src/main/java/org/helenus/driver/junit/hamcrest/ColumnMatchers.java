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
package org.helenus.driver.junit.hamcrest;

import java.lang.reflect.Field;

import java.util.Objects;

import org.apache.commons.lang3.ArrayUtils;

import org.hamcrest.Description;
import org.hamcrest.DiagnosingMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.core.AllOf;
import org.helenus.commons.lang3.reflect.ReflectionUtils;
import org.helenus.driver.persistence.Column;

/**
 * The <code>ColumnMatchers</code> class defines static factory methods for
 * column matching.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jun 30, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
public abstract class ColumnMatchers {
  /**
   * Checks if a field associated with the specified columns should be ignored.
   *
   * @author paouelle
   *
   * @param  columns the set of columns defined for the field
   * @param  ignore column names to ignore during comparison (may be
   *         <code>null</code> or empty)
   * @return <code>true</code> if the field should be ignored; <code>false</code>
   *         if it should be compared
   */
  private static boolean ignoreField(Column[] columns, String... ignore) {
    for (final Column c: columns) {
      if (ArrayUtils.contains(ignore, c.name())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Checks if a field associated with the specified columns should be accepted.
   *
   * @author paouelle
   *
   * @param  columns the set of columns defined for the field
   * @param  column the name of the column to accept for comparison
   * @return <code>true</code> if the field should be accepted; <code>false</code>
   *         if it should be ignored
   */
  private static boolean acceptField(Column[] columns, String column) {
    for (final Column c: columns) {
      if (c.name().equals(column)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Creates a matcher that matches when the examined object has all columns
   * equal to the same columns of the specified <code>operand</code>, as
   * determined by calling {@link java.util.Objects#deepEquals} method with each
   * associated fields' values.
   *
   * @author paouelle
   *
   * @param  operand the object to compare against
   * @param  ignore column names to ignore during comparison (may be
   *         <code>null</code> or empty)
   * @return a corresponding matcher
   */
  public static <T> Matcher<T> columnsEqualTo(T operand, String... ignore) {
    final Matcher<T>[] matchers =
      ReflectionUtils.getAllAnnotationsForFieldsAnnotatedWith(
        operand.getClass(), Column.class, true
      ).entrySet().stream()
       .filter(e -> !ColumnMatchers.ignoreField(e.getValue(), ignore))
       .map(e -> new AreColumnsEqual<>(operand, e.getKey(), e.getValue()))
       .toArray(Matcher[]::new);

    if (matchers.length == 1) {
      return matchers[0];
    }
    return AllOf.allOf(matchers);
  }

  /**
   * Creates a matcher that matches when the examined object has a given column
   * equal to the same column on the specified <code>operand</code>, as
   * determined by calling {@link java.util.Objects#deepEquals} method with each
   * associated fields' values.
   *
   * @author paouelle
   *
   * @param  column the name of the column to compare
   * @param  operand the object to compare against
   * @return a corresponding matcher
   */
  public static <T> Matcher<T> columnsEqualTo(String column, T operand) {
    final Matcher<T>[] matchers =
      ReflectionUtils.getAllAnnotationsForFieldsAnnotatedWith(
        operand.getClass(), Column.class, true
      ).entrySet().stream()
       .filter(e -> ColumnMatchers.acceptField(e.getValue(), column))
       .map(e -> new AreColumnsEqual<>(operand, e.getKey(), column))
       .toArray(Matcher[]::new);

    if (matchers.length == 1) {
      return matchers[0];
    }
    return AllOf.allOf(matchers);
  }

  /**
   * Creates a matcher that matches when the examined object has a given column
   * <code>null</code>, as determined by the associated fields' values.
   *
   * @author paouelle
   *
   * @param  column the name of the column to compare
   * @param  operand the class of object to compare against
   * @return a corresponding matcher
   */
  public static <T> Matcher<T> columnIsNull(String column, Class<T> operand) {
    final Matcher<T>[] matchers =
      ReflectionUtils.getAllAnnotationsForFieldsAnnotatedWith(
        operand, Column.class, true
      ).entrySet().stream()
       .filter(e -> ColumnMatchers.acceptField(e.getValue(), column))
       .map(e -> new IsColumnNull<>(operand, e.getKey(), column))
       .toArray(Matcher[]::new);

    if (matchers.length == 1) {
      return matchers[0];
    }
    return AllOf.allOf(matchers);
  }

  /**
   * Creates a matcher that matches when the examined object has a given column
   * not <code>null</code>, as determined by the associated fields' values.
   *
   * @author paouelle
   *
   * @param  column the name of the column to compare
   * @param  operand the class of object to compare against
   * @return a corresponding matcher
   */
  public static <T> Matcher<T> columnIsNotNull(String column, Class<T> operand) {
    final Matcher<T>[] matchers =
      ReflectionUtils.getAllAnnotationsForFieldsAnnotatedWith(
        operand, Column.class, true
      ).entrySet().stream()
       .filter(e -> ColumnMatchers.acceptField(e.getValue(), column))
       .map(e -> new IsColumnNotNull<>(operand, e.getKey(), column))
       .toArray(Matcher[]::new);

    if (matchers.length == 1) {
      return matchers[0];
    }
    return AllOf.allOf(matchers);
  }
}

/**
 * The <code>AreColumnsEqual</code> class defines an Hamcrest matcher capable of
 * comparing 2 pojo objects to see if a specified column are equal. Instead
 * of using the {@link Object#equals}, this matcher uses reflection to find the
 * field's values and compares them using their {@link Object#equals} method.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jun 30, 2015 - paouelle - Creation
 *
 * @param <T> the type of objects being compared
 *
 * @since 1.0
 */
class AreColumnsEqual<T> extends DiagnosingMatcher<T> {
  /**
   * Holds the expected object.
   *
   * @author paouelle
   */
  private final Object expected;

  /**
   * Holds the field of the column to compare.
   *
   * @author paouelle
   */
  private final Field field;

  /**
   * Holds the names of the columns to compare.
   *
   * @author paouelle
   */
  private final String columns;

  /**
   * Instantiates a new <code>AreColumnsEqual</code> object.
   *
   * @author paouelle
   *
   * @param expected the expected object to be equal to
   * @param field the field of the column to compare
   * @param columns the names of the columns to compare
   */
  AreColumnsEqual(T expected, Field field, Column[] columns) {
    this.expected = expected;
    this.field = field;
    final StringBuilder sb = new StringBuilder();

    for (final Column c: columns) {
      if (sb.length() > 0) {
        sb.append(", ");
      }
      sb.append(c.name());
    }
    this.columns = sb.toString();
    field.setAccessible(true); // make sure we can access any private fields
  }

  /**
   * Instantiates a new <code>AreColumnsEqual</code> object.
   *
   * @author paouelle
   *
   * @param expected the expected object to be equal to
   * @param field the field of the column to compare
   * @param column the name of the column to compare
   */
  AreColumnsEqual(T expected, Field field, String column) {
    this.expected = expected;
    this.field = field;
    this.columns = column;
    field.setAccessible(true); // make sure we can access any private fields
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.hamcrest.DiagnosingMatcher#matches(java.lang.Object, org.hamcrest.Description)
   */
  @Override
  protected boolean matches(Object item, Description mismatch) {
    // first make sure they are the same class
    if (item == null) {
      if (expected != null) {
        mismatch
          .appendText(columns)
          .appendText(" was null");
        return false;
      }
      return true;
    } else if (item == expected) {
      return true;
    } else if (expected == null) {
      mismatch
        .appendText(columns)
        .appendText(" was not null");
      return false;
    } else if (!expected.getClass().isInstance(item)) {
      mismatch
        .appendText(columns)
        .appendText(" is not an instance of ")
        .appendText(expected.getClass().getName());
      return false;
    }
    try {
      final Object ival = field.get(item);
      final Object eval = field.get(expected);

      if (!Objects.deepEquals(ival, eval)) {
        mismatch
          .appendText(columns)
          .appendText(" was ")
          .appendValue(ival)
          .appendText(" instead of ")
          .appendValue(eval);
        return false;
      }
      return true;
    } catch (IllegalAccessException e) {
      throw new AssertionError(e);
    } catch (ExceptionInInitializerError e) {
      final Throwable t = e.getException();

      if (t instanceof AssertionError) {
        throw (AssertionError)t;
      }
      throw new AssertionError(t);
    }
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.hamcrest.SelfDescribing#describeTo(org.hamcrest.Description)
   */
  @Override
  public void describeTo(Description description) {
    try {
      description
        .appendText(columns)
        .appendText(" equals to ")
        .appendValue((expected != null) ? field.get(expected) : null);
    } catch (IllegalAccessException e) {
      throw new AssertionError(e);
    } catch (ExceptionInInitializerError e) {
      final Throwable t = e.getException();

      if (t instanceof AssertionError) {
        throw (AssertionError)t;
      }
      throw new AssertionError(t);
    }
  }
}

/**
 * The <code>IsColumnNull</code> class defines an Hamcrest matcher capable of
 * comparing a pojo objects to see if a specified column is <code>null</code>.
 * This matcher uses reflection to find the field's value to match.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jul 6, 2015 - paouelle - Creation
 *
 * @param <T> the type of objects being compared
 *
 * @since 1.0
 */
class IsColumnNull<T> extends DiagnosingMatcher<T> {
  /**
   * Holds the expected class.
   *
   * @author paouelle
   */
  private final Class<T> expected;

  /**
   * Holds the field of the column to compare.
   *
   * @author paouelle
   */
  private final Field field;

  /**
   * Holds the names of the column to compare.
   *
   * @author paouelle
   */
  private final String column;

  /**
   * Instantiates a new <code>IsColumnNull</code> object.
   *
   * @author paouelle
   *
   * @param expected the expected class to compare
   * @param field the field of the column to compare
   * @param column the name of the column to compare
   */
  IsColumnNull(Class<T> expected, Field field, String column) {
    this.expected = expected;
    this.field = field;
    this.column = column;
    field.setAccessible(true); // make sure we can access any private fields
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.hamcrest.DiagnosingMatcher#matches(java.lang.Object, org.hamcrest.Description)
   */
  @Override
  protected boolean matches(Object item, Description mismatch) {
    // first make sure they are the same class
    if (item == null) {
      return true;
    } else if (!expected.isInstance(item)) {
      mismatch
      .appendText(column)
        .appendText(" was not an instance of ")
        .appendText(expected.getName());
      return false;
    }
    try {
      final Object ival = field.get(item);

      if (ival != null) {
        mismatch
          .appendText(column)
          .appendText(" was not null");
        return false;
      }
      return true;
    } catch (IllegalAccessException e) {
      throw new AssertionError(e);
    } catch (ExceptionInInitializerError e) {
      final Throwable t = e.getException();

      if (t instanceof AssertionError) {
        throw (AssertionError)t;
      }
      throw new AssertionError(t);
    }
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.hamcrest.SelfDescribing#describeTo(org.hamcrest.Description)
   */
  @Override
  public void describeTo(Description description) {
    description.appendText(column).appendText(" is null");
  }
}

/**
 * The <code>IsColumnNotNull</code> class defines an Hamcrest matcher capable of
 * comparing a pojo objects to see if a specified column is not <code>null</code>.
 * This matcher uses reflection to find the field's value to match.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jul 6, 2015 - paouelle - Creation
 *
 * @param <T> the type of objects being compared
 *
 * @since 1.0
 */
class IsColumnNotNull<T> extends DiagnosingMatcher<T> {
  /**
   * Holds the expected class.
   *
   * @author paouelle
   */
  private final Class<T> expected;

  /**
   * Holds the field of the column to compare.
   *
   * @author paouelle
   */
  private final Field field;

  /**
   * Holds the names of the column to compare.
   *
   * @author paouelle
   */
  private final String column;

  /**
   * Instantiates a new <code>IsColumnNotNull</code> object.
   *
   * @author paouelle
   *
   * @param expected the expected class to compare
   * @param field the field of the column to compare
   * @param column the name of the column to compare
   */
  IsColumnNotNull(Class<T> expected, Field field, String column) {
    this.expected = expected;
    this.field = field;
    this.column = column;
    field.setAccessible(true); // make sure we can access any private fields
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.hamcrest.DiagnosingMatcher#matches(java.lang.Object, org.hamcrest.Description)
   */
  @Override
  protected boolean matches(Object item, Description mismatch) {
    // first make sure they are the same class
    if (item == null) {
      return true;
    } else if (!expected.isInstance(item)) {
      mismatch
        .appendText(column)
        .appendText(" was not an instance of ")
        .appendText(expected.getName());
      return false;
    }
    try {
      final Object ival = field.get(item);

      if (ival == null) {
        mismatch
          .appendText(column)
          .appendText(" was null");
        return false;
      }
      return true;
    } catch (IllegalAccessException e) {
      throw new AssertionError(e);
    } catch (ExceptionInInitializerError e) {
      final Throwable t = e.getException();

      if (t instanceof AssertionError) {
        throw (AssertionError)t;
      }
      throw new AssertionError(t);
    }
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.hamcrest.SelfDescribing#describeTo(org.hamcrest.Description)
   */
  @Override
  public void describeTo(Description description) {
    description.appendText(column).appendText(" is not null");
  }
}

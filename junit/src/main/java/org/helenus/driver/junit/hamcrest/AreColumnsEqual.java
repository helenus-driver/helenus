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

import java.lang.reflect.Array;
import java.lang.reflect.Field;

import java.util.Map;
import java.util.Objects;

import org.apache.commons.lang3.ArrayUtils;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;
import org.helenus.commons.lang3.reflect.ReflectionUtils;
import org.helenus.driver.persistence.Column;

/**
 * The <code>AreColumnsEqual</code> class defines an Hamcrest matcher capable of
 * comparing 2 pojo objects to see if all their defined columns are equal. Instead
 * of using the {@link Object#equals}, this matcher uses reflection to find all
 * fields annotated as {@link Column}s and compares them using their
 * {@link Object#equals} method.
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
public class AreColumnsEqual<T> extends BaseMatcher<T> {
  /**
   * Checks if the two specified objects have equal columns.
   *
   * @author paouelle
   *
   * @param  actual the actual object to compare
   * @param  expected the expected object to compare against
   * @param  ignore column names to ignore during comparison (may be
   *         <code>null</code> or empty)
   * @return <code>true</code> if both objects have equal columns; <code>false</code>
   *         otherwise
   */
  private static boolean areEqual(
    Object actual, Object expected, String... ignore
  ) {
    if (actual == null) {
      return (expected == null);
    } else if (actual == expected) {
      return true;
    } else if (expected == null) {
      return false;
    }
    final Class<?> clazz = actual.getClass();

    if (clazz.isArray()) {
      return (
        expected.getClass().isArray()
        && AreColumnsEqual.areArraysEqual(actual, expected, ignore)
      );
    }
    // make sure they are of the same class
    if (!expected.getClass().equals(clazz)) {
      return false;
    }
    return ReflectionUtils.getAllAnnotationsForFieldsAnnotatedWith(
      clazz, Column.class, true
    ).entrySet().stream()
     .filter(e -> !AreColumnsEqual.ignoreField(e.getValue(), ignore))
     .map(Map.Entry::getKey)
     .allMatch(f -> AreColumnsEqual.areFieldsEqual(f, actual, expected));
  }

  /**
   * Checks if the two specified objects have the specified column equal.
   *
   * @author paouelle
   *
   * @param  column the name of the column to compare
   * @param  actual the actual object to compare
   * @param  expected the expected object to compare against
   * @return <code>true</code> if both objects have the specified columns equal;
   *         <code>false</code> otherwise
   */
  private static boolean areEqual(
    String column, Object actual, Object expected
  ) {
    if (actual == null) {
      return (expected == null);
    } else if (actual == expected) {
      return true;
    } else if (expected == null) {
      return false;
    }
    final Class<?> clazz = actual.getClass();

    if (clazz.isArray()) {
      return (
        expected.getClass().isArray()
        && AreColumnsEqual.areArraysEqual(column, actual, expected)
      );
    }
    // make sure they are of the same class
    if (!expected.getClass().equals(clazz)) {
      return false;
    }
    return ReflectionUtils.getAllAnnotationsForFieldsAnnotatedWith(
      clazz, Column.class, true
    ).entrySet().stream()
     .filter(e -> AreColumnsEqual.acceptField(e.getValue(), column))
     .map(Map.Entry::getKey)
     .allMatch(f -> AreColumnsEqual.areFieldsEqual(f, actual, expected));
  }

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
      if (c.name().equals(c.name())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Checks if the specified field values from both objects are equal.
   *
   * @author paouelle
   *
   * @param  field the field to check
   * @param  actual the actual object to compare
   * @param  expected the expected object to compare against
   * @return <code>true</code> if both field values are equal; <code>false</code>
   *         otherwise
   */
  private static boolean areFieldsEqual(
    Field field, Object actual, Object expected
  ) {
    field.setAccessible(true); // make sure we can access any private fields
    try {
      final Object aval = field.get(actual);
      final Object eval = field.get(expected);

      return Objects.equals(aval, eval);
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
   * Checks if the two specified array of objects have equal columns.
   *
   * @author paouelle
   *
   * @param  actual the actual array of objects to compare
   * @param  expected the expected array of objects to compare against
   * @param  ignore column names to ignore during comparison (may be
   *         <code>null</code> or empty)
   * @return <code>true</code> if both array of objects have equal columns;
   *         <code>false</code> otherwise
   */
  private static boolean areArraysEqual(
    Object actual, Object expected, String... ignore
  ) {
    final int l = Array.getLength(actual);

    if (Array.getLength(expected) == l) {
      for (int i = 0; i < l; i++) {
        final Object aval = Array.get(actual, i);
        final Object eval = Array.get(expected, i);

        if (!AreColumnsEqual.areEqual(aval, eval, ignore)) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  /**
   * Checks if the two specified array of objects have the specified column
   * equal.
   *
   * @author paouelle
   *
   * @param  column the name of the column to compare
   * @param  actual the actual array of objects to compare
   * @param  expected the expected array of objects to compare against
   * @return <code>true</code> if both array of objects have the specified column
   *         equal; <code>false</code> otherwise
   */
  private static boolean areArraysEqual(
    String column, Object actual, Object expected
  ) {
    final int l = Array.getLength(actual);

    if (Array.getLength(expected) == l) {
      for (int i = 0; i < l; i++) {
        final Object aval = Array.get(actual, i);
        final Object eval = Array.get(expected, i);

        if (!AreColumnsEqual.areEqual(column, aval, eval)) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  /**
   * Creates a matcher that matches when the examined object has all columns
   * equal to the same columns of the specified <code>operand</code>, as
   * determined by calling the {@link java.lang.Object#equals} method on each
   * associated fields' values.
   * <p>
   * If the specified operand is <code>null</code> then the created matcher will
   * only match if the examined object's is also <code>null</code>.
   * <p>
   * The created matcher provides a special behavior when examining
   * <code>Array</code>s, whereby it will match if both the operand and the
   * examined object are arrays of the same length and contain items that are
   * equal to each other (according to the above rules) <b>in the same
   * indexes</b>.
   *
   * @author paouelle
   *
   * @param  operand the object to compare against
   * @param  ignore column names to ignore during comparison (may be
   *         <code>null</code> or empty)
   * @return a corresponding matcher
   */
  @Factory
  public static <T> Matcher<T> columnsEqualTo(T operand, String... ignore) {
    return new AreColumnsEqual<>(operand, ignore);
  }

  /**
   * Creates a matcher that matches when the examined object has a given column
   * equal to the same column on the specified <code>operand</code>, as
   * determined by calling the {@link java.lang.Object#equals} method on each
   * associated fields' values.
   * <p>
   * If the specified operand is <code>null</code> then the created matcher will
   * only match if the examined object's is also <code>null</code>.
   * <p>
   * The created matcher provides a special behavior when examining
   * <code>Array</code>s, whereby it will match if both the operand and the
   * examined object are arrays of the same length and contain items that are
   * equal to each other (according to the above rules) <b>in the same
   * indexes</b>.
   *
   * @author paouelle
   *
   * @param  column the name of the column to compare
   * @param  operand the object to compare against
   * @return a corresponding matcher
   */
  @Factory
  public static <T> Matcher<T> columnsEqualTo(String column, T operand) {
    return new AreColumnsEqual<>(column, operand);
  }

  /**
   * Holds the expected object.
   *
   * @author paouelle
   */
  private final Object expected;

  /**
   * Holds a set of column names to ignore during comparison (may be
   * <code>null</code>).
   *
   * @author paouelle
   */
  private final String[] ignore;

  /**
   * Holds the name of the column to compare (<code>null</code> to compare
   * all columns).
   *
   * @author paouelle
   */
  private final String column;

  /**
   * Instantiates a new <code>AreColumnsEqual</code> object.
   *
   * @author paouelle
   *
   * @param expected the expected object to be equal to
   * @param ignore column names to ignore during comparison (may be
   *        <code>null</code> or empty)
   */
  public AreColumnsEqual(T expected, String... ignore) {
    this.expected = expected;
    this.ignore = ignore;
    this.column = null;
  }

  /**
   * Instantiates a new <code>AreColumnsEqual</code> object.
   *
   * @author paouelle
   *
   * @param column the name of the column to compare
   * @param expected the expected object to be equal to
   */
  public AreColumnsEqual(String column, T expected) {
    this.expected = expected;
    this.ignore = null;
    this.column = column;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.hamcrest.Matcher#matches(java.lang.Object)
   */
  @Override
  public boolean matches(Object actual) {
    if (column != null) {

    }
    return AreColumnsEqual.areEqual(actual, expected, ignore);
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
    description.appendValue(expected);
  }
}

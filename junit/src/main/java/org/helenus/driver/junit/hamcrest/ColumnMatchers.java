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

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.ArrayUtils;

import org.hamcrest.Description;
import org.hamcrest.DiagnosingMatcher;
import org.hamcrest.Matcher;
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
   * @param  found a set where to record found columns from the ignore list
   * @param  ignore column names to ignore during comparison (may be
   *         <code>null</code> or empty)
   * @return <code>true</code> if the field should be ignored; <code>false</code>
   *         if it should be compared
   */
  private static boolean ignoreField(
    Column[] columns, Set<String> found, String... ignore
  ) {
    for (final Column c: columns) {
      if (ArrayUtils.contains(ignore, c.name())) {
        found.add(c.name());
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
   * @param <T> the type of object to match
   *
   * @param  operand the pojo to compare against
   * @param  ignore column names to ignore during comparison (may be
   *         <code>null</code> or empty)
   * @return a corresponding matcher
   */
  public static <T> Matcher<T> columnsEqualTo(T operand, String... ignore) {
    final Set<String> found = new HashSet<>(ignore.length * 3 / 2);

    final List<AreColumnsEqual<T>> matchers =
      ReflectionUtils.getAllAnnotationsForFieldsAnnotatedWith(
        operand.getClass(), Column.class, true
      ).entrySet().stream()
       .filter(e -> !ColumnMatchers.ignoreField(e.getValue(), found, ignore))
       .map(e -> new AreColumnsEqual<>(operand, e.getKey(), e.getValue()))
       .collect(Collectors.toList());

    org.apache.commons.lang3.Validate.isTrue(
      !matchers.isEmpty(),
      "no matching columns left defined in pojo class '%s'", operand.getClass().getSimpleName()
    );
    for (final String i: ignore) {
      org.apache.commons.lang3.Validate.isTrue(
        found.contains(i),
        "column '%s' not defined in pojo class '%s'", i, operand.getClass().getSimpleName()
      );
    }
    return new DiagnosingMatcher<T>() {
      @Override
      public boolean matches(Object o, Description mismatch) {
        for (final Matcher<T> matcher: matchers) {
          if (!matcher.matches(o)) {
            matcher.describeMismatch(o, mismatch);
            return false;
          }
        }
        return true;
      }
      @Override
      public void describeTo(Description description) {
        description
          .appendText(operand.getClass().getSimpleName())
          .appendText("[")
          .appendText(matchers.stream().map(m -> m.columns).collect(Collectors.joining(", ")))
          .appendText("] are all equal");
      }
    };
  }

  /**
   * Creates a matcher that matches when the examined object has a given column
   * equal to the same column on the specified <code>operand</code>, as
   * determined by calling {@link java.util.Objects#deepEquals} method with each
   * associated fields' values.
   *
   * @author paouelle
   *
   * @param <T> the type of object to match
   *
   * @param  column the name of the column to compare
   * @param  operand the pojo to compare against
   * @return a corresponding matcher
   */
  public static <T> Matcher<T> columnsEqualTo(String column, T operand) {
    final List<AreColumnsEqual<T>> matchers =
      ReflectionUtils.getAllAnnotationsForFieldsAnnotatedWith(
        operand.getClass(), Column.class, true
      ).entrySet().stream()
       .filter(e -> ColumnMatchers.acceptField(e.getValue(), column))
       .map(e -> new AreColumnsEqual<>(operand, e.getKey(), column))
       .collect(Collectors.toList());

    org.apache.commons.lang3.Validate.isTrue(
      !matchers.isEmpty(),
      "column '%s' not defined in pojo class '%s'", column, operand.getClass().getSimpleName()
    );
    return new DiagnosingMatcher<T>() {
      @Override
      public boolean matches(Object o, Description mismatch) {
        for (final Matcher<T> matcher : matchers) {
          if (!matcher.matches(o)) {
            matcher.describeMismatch(o, mismatch);
            return false;
          }
        }
        return true;
      }
      @Override
      public void describeTo(Description description) {
        description
          .appendText(operand.getClass().getSimpleName())
          .appendText("[")
          .appendText(matchers.stream().map(m -> m.columns).collect(Collectors.joining(", ")))
          .appendText("] is equal");
      }
    };
  }

  /**
   * Creates a matcher that matches when the examined object has all columns
   * equal with a tolerable error to the same columns of the specified
   * <code>operand</code>, as determined by calling
   * {@link org.helenus.util.Objects#deepEquals} method with each associated fields'
   * values.
   *
   * @author paouelle
   *
   * @param <T> the type of object to match
   *
   * @param  operand the pojo to compare against
   * @param  epsilon the tolerable error
   * @param  ignore column names to ignore during comparison (may be
   *         <code>null</code> or empty)
   * @return a corresponding matcher
   */
  public static <T> Matcher<T> columnsCloseTo(
    T operand, double epsilon, String... ignore
  ) {
    final Set<String> found = new HashSet<>(ignore.length * 3 / 2);

    final List<AreColumnsEqual<T>> matchers =
      ReflectionUtils.getAllAnnotationsForFieldsAnnotatedWith(
        operand.getClass(), Column.class, true
      ).entrySet().stream()
       .filter(e -> !ColumnMatchers.ignoreField(e.getValue(), found, ignore))
       .map(e -> new AreColumnsEqual<>(operand, e.getKey(), e.getValue(), epsilon))
       .collect(Collectors.toList());

    org.apache.commons.lang3.Validate.isTrue(
      !matchers.isEmpty(),
      "no matching columns left defined in pojo class '%s'", operand.getClass().getSimpleName()
    );
    for (final String i: ignore) {
      org.apache.commons.lang3.Validate.isTrue(
        found.contains(i),
        "column '%s' not defined in pojo class '%s'", i, operand.getClass().getSimpleName()
      );
    }
    return new DiagnosingMatcher<T>() {
      @Override
      public boolean matches(Object o, Description mismatch) {
        for (final Matcher<T> matcher : matchers) {
          if (!matcher.matches(o)) {
            matcher.describeMismatch(o, mismatch);
            return false;
          }
        }
        return true;
      }
      @Override
      public void describeTo(Description description) {
        description
          .appendText(operand.getClass().getSimpleName())
          .appendText("[")
          .appendText(matchers.stream().map(m -> m.columns).collect(Collectors.joining(", ")))
          .appendText("] are all close within " + epsilon);
      }
    };
  }

  /**
   * Creates a matcher that matches when the examined object has a given column
   * equal with a tolerable error to the same column on the specified
   * <code>operand</code>, as determined by calling
   * {@link org.helenus.util.Objects#deepEquals} method with each associated fields'
   * values.
   *
   * @author paouelle
   *
   * @param <T> the type of object to match
   *
   * @param  column the name of the column to compare
   * @param  operand the pojo to compare against
   * @param  epsilon the tolerable error
   * @return a corresponding matcher
   */
  public static <T> Matcher<T> columnsCloseTo(
    String column, T operand, double epsilon
  ) {
    final List<AreColumnsEqual<T>> matchers =
      ReflectionUtils.getAllAnnotationsForFieldsAnnotatedWith(
        operand.getClass(), Column.class, true
      ).entrySet().stream()
       .filter(e -> ColumnMatchers.acceptField(e.getValue(), column))
       .map(e -> new AreColumnsEqual<>(operand, e.getKey(), column, epsilon))
       .collect(Collectors.toList());

    org.apache.commons.lang3.Validate.isTrue(
      !matchers.isEmpty(),
      "column '%s' not defined in pojo class '%s'", column, operand.getClass().getSimpleName()
    );
    return new DiagnosingMatcher<T>() {
      @Override
      public boolean matches(Object o, Description mismatch) {
        for (final Matcher<T> matcher : matchers) {
          if (!matcher.matches(o)) {
            matcher.describeMismatch(o, mismatch);
            return false;
          }
        }
        return true;
      }
      @Override
      public void describeTo(Description description) {
        description
          .appendText(operand.getClass().getSimpleName())
          .appendText("[")
          .appendText(matchers.stream().map(m -> m.columns).collect(Collectors.joining(", ")))
          .appendText("] is close within " + epsilon);
      }
    };
  }

  /**
   * Creates a matcher that matches when the examined object has a given column
   * <code>null</code>, as determined by the associated fields' values.
   *
   * @author paouelle
   *
   * @param <T> the type of object to match
   *
   * @param  column the name of the column to compare
   * @param  operand the class of object to compare against
   * @return a corresponding matcher
   */
  public static <T> Matcher<T> columnIsNull(String column, Class<T> operand) {
    final List<IsColumnNull<T>> matchers =
      ReflectionUtils.getAllAnnotationsForFieldsAnnotatedWith(
        operand, Column.class, true
      ).entrySet().stream()
       .filter(e -> ColumnMatchers.acceptField(e.getValue(), column))
       .map(e -> new IsColumnNull<>(operand, e.getKey(), column))
       .collect(Collectors.toList());

    org.apache.commons.lang3.Validate.isTrue(
      !matchers.isEmpty(),
      "column '%s' not defined in pojo class '%s'", column, operand.getSimpleName()
    );
    return new DiagnosingMatcher<T>() {
      @Override
      public boolean matches(Object o, Description mismatch) {
        for (final Matcher<T> matcher : matchers) {
          if (!matcher.matches(o)) {
            matcher.describeMismatch(o, mismatch);
            return false;
          }
        }
        return true;
      }
      @Override
      public void describeTo(Description description) {
        description
          .appendText(operand.getSimpleName())
          .appendText("[")
          .appendText(matchers.stream().map(m -> m.column).collect(Collectors.joining(", ")))
          .appendText("] is null");
      }
    };
  }

  /**
   * Creates a matcher that matches when the examined object has a given column
   * not <code>null</code>, as determined by the associated fields' values.
   *
   * @author paouelle
   *
   * @param <T> the type of object to match
   *
   * @param  column the name of the column to compare
   * @param  operand the class of object to compare against
   * @return a corresponding matcher
   */
  public static <T> Matcher<T> columnIsNotNull(String column, Class<T> operand) {
    final List<IsColumnNotNull<T>> matchers =
      ReflectionUtils.getAllAnnotationsForFieldsAnnotatedWith(
        operand, Column.class, true
      ).entrySet().stream()
       .filter(e -> ColumnMatchers.acceptField(e.getValue(), column))
       .map(e -> new IsColumnNotNull<>(operand, e.getKey(), column))
       .collect(Collectors.toList());

    org.apache.commons.lang3.Validate.isTrue(
      !matchers.isEmpty(),
      "column '%s' not defined in pojo class '%s'", column, operand.getSimpleName()
    );
    return new DiagnosingMatcher<T>() {
      @Override
      public boolean matches(Object o, Description mismatch) {
        for (final Matcher<T> matcher : matchers) {
          if (!matcher.matches(o)) {
            matcher.describeMismatch(o, mismatch);
            return false;
          }
        }
        return true;
      }
      @Override
      public void describeTo(Description description) {
        description
          .appendText(operand.getSimpleName())
          .appendText("[")
          .appendText(matchers.stream().map(m -> m.column).collect(Collectors.joining(", ")))
          .appendText("] is not null");
      }
    };
  }

  /**
   * Creates a matcher that matches when the examined object has a given column
   * value that matches the specified matcher, as determined by the associated
   * fields' value.
   *
   * @author paouelle
   *
   * @param <T> the type of object to match
   * @param <V> the type of value for the column to match
   *
   * @param  column the name of the column to compare
   * @param  operand the class of object to compare against
   * @param  matcher the matcher for the value of the column to match with
   * @return a corresponding matcher
   */
  public static <T, V> Matcher<T> columnMatches(
    String column, Class<T> operand, Matcher<V> matcher
  ) {
    final List<ColumnMatches<T, V>> matchers =
      ReflectionUtils.getAllAnnotationsForFieldsAnnotatedWith(
        operand, Column.class, true
      ).entrySet().stream()
       .filter(e -> ColumnMatchers.acceptField(e.getValue(), column))
       .map(e -> new ColumnMatches<>(operand, e.getKey(), column, matcher))
       .collect(Collectors.toList());

    org.apache.commons.lang3.Validate.isTrue(
      !matchers.isEmpty(),
      "column '%s' not defined in pojo class '%s'", column, operand.getSimpleName()
    );
    return new DiagnosingMatcher<T>() {
      @Override
      public boolean matches(Object o, Description mismatch) {
        for (final Matcher<T> matcher: matchers) {
          if (!matcher.matches(o)) {
            matcher.describeMismatch(o, mismatch);
            return false;
          }
        }
        return true;
      }
      @Override
      public void describeTo(Description description) {
        description
          .appendText(operand.getSimpleName())
          .appendText("[")
          .appendText(matchers.stream().map(m -> m.column).collect(Collectors.joining(", ")))
          .appendText("] ")
          .appendDescriptionOf(matcher);
      }
    };
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
  final T expected;

  /**
   * Holds the field of the column to compare.
   *
   * @author paouelle
   */
  final Field field;

  /**
   * Holds the names of the columns to compare.
   *
   * @author paouelle
   */
  final String columns;

  /**
   * Holds the tolerable error when comparing or <code>null</code> if no tolerable
   * error is acceptable.
   *
   * @author paouelle
   */
  final Double epsilon;

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
    this(expected, field, columns, null);
  }

  /**
   * Instantiates a new <code>AreColumnsEqual</code> object.
   *
   * @author paouelle
   *
   * @param expected the expected object to be equal to
   * @param field the field of the column to compare
   * @param columns the names of the columns to compare
   * @param epsilon the tolerable error
   */
  AreColumnsEqual(T expected, Field field, Column[] columns, Double epsilon) {
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
    this.epsilon = epsilon;
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
    this(expected, field, column, null);
  }

  /**
   * Instantiates a new <code>AreColumnsEqual</code> object.
   *
   * @author paouelle
   *
   * @param expected the expected object to be equal to
   * @param field the field of the column to compare
   * @param column the name of the column to compare
   * @param epsilon the tolerable error
   */
  AreColumnsEqual(T expected, Field field, String column, Double epsilon) {
    this.expected = expected;
    this.field = field;
    this.columns = column;
    this.epsilon = epsilon;
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
        mismatch.appendText("however the pojo was null");
        return false;
      }
      return true;
    } else if (item == expected) {
      return true;
    } else if (expected == null) {
      mismatch.appendText("however the pojo was not null");
      return false;
    } else if (!expected.getClass().isInstance(item)) {
      mismatch
        .appendText("however the pojo was not an instance of ")
        .appendText(expected.getClass().getName());
      return false;
    }
    try {
      final Object ival = field.get(item);
      final Object eval = field.get(expected);
      final boolean matched;

      if (epsilon != null) {
        matched = org.helenus.util.Objects.deepEquals(ival, eval, epsilon);
      } else {
        matched = Objects.deepEquals(ival, eval);
      }
      if (!matched) {
        mismatch
          .appendText("however the column '")
          .appendText(columns)
          .appendText("' was ")
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
    description
      .appendText("column '")
      .appendText(columns);
    if (epsilon == null) {
      description.appendText("' are equal");
    } else {
      description.appendText("' are close to within ").appendText(epsilon.toString());
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
  final Class<T> expected;

  /**
   * Holds the field of the column to compare.
   *
   * @author paouelle
   */
  final Field field;

  /**
   * Holds the names of the column to compare.
   *
   * @author paouelle
   */
  final String column;

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
        .appendText("however the pojo was not an instance of ")
        .appendText(expected.getName());
      return false;
    }
    try {
      final Object ival = field.get(item);

      if (ival != null) {
        mismatch
          .appendText("however the column '")
          .appendText(column)
          .appendText("' was not null");
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
    description
      .appendText("column '")
      .appendText(column)
      .appendText("' is null");
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
  final Class<T> expected;

  /**
   * Holds the field of the column to compare.
   *
   * @author paouelle
   */
  final Field field;

  /**
   * Holds the names of the column to compare.
   *
   * @author paouelle
   */
  final String column;

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
      mismatch.appendText("however the pojo was null");
      return false;
    } else if (!expected.isInstance(item)) {
      mismatch
        .appendText("however the pojo was not an instance of ")
        .appendText(expected.getName());
      return false;
    }
    try {
      final Object ival = field.get(item);

      if (ival == null) {
        mismatch
          .appendText("however the column '")
          .appendText(column)
          .appendText("' was null");
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
    description
      .appendText("column '")
      .appendText(column)
      .appendText("' is not null");
  }
}

/**
 * The <code>ColumnMatches</code> class defines an Hamcrest matcher capable of
 * comparing a pojo objects to see if a specified column matches a given matcher.
 * This matcher uses reflection to find the field's value to match.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jul 20, 2015 - paouelle - Creation
 *
 * @param <T> the type of objects being compared
 * @param <V> the type of column values being compared
 *
 * @since 1.0
 */
class ColumnMatches<T, V> extends DiagnosingMatcher<T> {
  /**
   * Holds the expected class.
   *
   * @author paouelle
   */
  final Class<T> expected;

  /**
   * Holds the field of the column to compare.
   *
   * @author paouelle
   */
  final Field field;

  /**
   * Holds the names of the column to compare.
   *
   * @author paouelle
   */
  final String column;

  /**
   * Holds the matcher to use for the column value.
   *
   * @author paouelle
   */
  final Matcher<V> matcher;

  /**
   * Instantiates a new <code>ColumnMatches</code> object.
   *
   * @author paouelle
   *
   * @param expected the expected class to compare
   * @param field the field of the column to compare
   * @param column the name of the column to compare
   * @param matcher the matcher to use for matching the column's value
   */
  ColumnMatches(Class<T> expected, Field field, String column, Matcher<V> matcher) {
    this.expected = expected;
    this.field = field;
    this.column = column;
    this.matcher = matcher;
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
      mismatch.appendText("however the pojo was null");
      return false;
    } else if (!expected.isInstance(item)) {
      mismatch
        .appendText("however the pojo was not an instance of ")
        .appendText(expected.getName());
      return false;
    }
    try {
      final Object ival = field.get(item);

      if (!matcher.matches(ival)) {
        mismatch
          .appendText("however the column '")
          .appendText(column)
          .appendText("' ");
        matcher.describeMismatch(ival, mismatch);
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
    description
      .appendText("column '")
      .appendText(column)
      .appendText("' ")
      .appendDescriptionOf(matcher);
  }
}
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
package org.helenus.commons.lang3;

import java.util.Collection;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;

/**
 * The <code>Validate</code> class extends the class
 * {@link org.apache.commons.lang3.Validate} to provide support for logging
 * exceptions thrown out.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 15, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
public class Validate {
  /**
   * <p>Validate that the argument condition is {@code true}; otherwise
   * throwing an exception with the specified message. This method is useful when
   * validating according to an arbitrary boolean expression, such as validating a
   * primitive number or using your own custom validation expression.</p>
   *
   * <pre>Validate.isTrue(i &gt; 0.0, "The value must be greater than zero: %d", i);</pre>
   *
   * <p>For performance reasons, the long value is passed as a separate parameter and
   * appended to the exception message only in the case of an error.</p>
   *
   * @param logger  the logger via which to log the exception
   * @param expression  the boolean expression to check
   * @param message  the {@link String#format(String, Object...)} exception message if invalid, not null
   * @param value  the value to append to the message when invalid
   * @throws IllegalArgumentException if expression is {@code false}
   *
   * @see #isTrue(Logger, boolean)
   * @see #isTrue(Logger, boolean, String, double)
   * @see #isTrue(Logger, boolean, String, Object...)
   */
  public static void isTrue(Logger logger, boolean expression, String message, long value) {
    try {
      org.apache.commons.lang3.Validate.isTrue(expression, message, value);
    } catch (RuntimeException e) {
      throw logger.throwing(e);
    }
  }
  /**
   * <p>Validate that the argument condition is {@code true}; otherwise
   * throwing an exception with the specified message. This method is useful when
   * validating according to an arbitrary boolean expression, such as validating a
   * primitive number or using your own custom validation expression.</p>
   *
   * <pre>Validate.isTrue(i &gt; 0.0, "The value must be greater than zero: %d", i);</pre>
   *
   * <p>For performance reasons, the long value is passed as a separate parameter and
   * appended to the exception message only in the case of an error.</p>
   *
   * @param logger  the logger via which to log the exception
   * @param level  the level at which to log the exception
   * @param expression  the boolean expression to check
   * @param message  the {@link String#format(String, Object...)} exception message if invalid, not null
   * @param value  the value to append to the message when invalid
   * @throws IllegalArgumentException if expression is {@code false}
   *
   * @see #isTrue(Logger, boolean)
   * @see #isTrue(Logger, boolean, String, double)
   * @see #isTrue(Logger, boolean, String, Object...)
   */
  public static void isTrue(Logger logger, Level level, boolean expression, String message, long value) {
    try {
      org.apache.commons.lang3.Validate.isTrue(expression, message, value);
    } catch (RuntimeException e) {
      throw logger.throwing(level, e);
    }
  }

  /**
   * <p>Validate that the argument condition is {@code true}; otherwise
   * throwing an exception with the specified message. This method is useful when
   * validating according to an arbitrary boolean expression, such as validating a
   * primitive number or using your own custom validation expression.</p>
   *
   * <pre>Validate.isTrue(d &gt; 0.0, "The value must be greater than zero: %s", d);</pre>
   *
   * <p>For performance reasons, the double value is passed as a separate parameter and
   * appended to the exception message only in the case of an error.</p>
   *
   * @param logger  the logger via which to log the exception
   * @param expression  the boolean expression to check
   * @param message  the {@link String#format(String, Object...)} exception message if invalid, not null
   * @param value  the value to append to the message when invalid
   * @throws IllegalArgumentException if expression is {@code false}
   *
   * @see #isTrue(Logger, boolean)
   * @see #isTrue(Logger, boolean, String, long)
   * @see #isTrue(Logger, boolean, String, Object...)
   */
  public static void isTrue(Logger logger, boolean expression, String message, double value) {
    try {
      org.apache.commons.lang3.Validate.isTrue(expression, message, value);
    } catch (RuntimeException e) {
      throw logger.throwing(e);
    }
  }
  /**
   * <p>Validate that the argument condition is {@code true}; otherwise
   * throwing an exception with the specified message. This method is useful when
   * validating according to an arbitrary boolean expression, such as validating a
   * primitive number or using your own custom validation expression.</p>
   *
   * <pre>Validate.isTrue(d &gt; 0.0, "The value must be greater than zero: %s", d);</pre>
   *
   * <p>For performance reasons, the double value is passed as a separate parameter and
   * appended to the exception message only in the case of an error.</p>
   *
   * @param logger  the logger via which to log the exception
   * @param level  the level at which to log the exception
   * @param expression  the boolean expression to check
   * @param message  the {@link String#format(String, Object...)} exception message if invalid, not null
   * @param value  the value to append to the message when invalid
   * @throws IllegalArgumentException if expression is {@code false}
   *
   * @see #isTrue(Logger, boolean)
   * @see #isTrue(Logger, boolean, String, long)
   * @see #isTrue(Logger, boolean, String, Object...)
   */
  public static void isTrue(Logger logger, Level level, boolean expression, String message, double value) {
    try {
      org.apache.commons.lang3.Validate.isTrue(expression, message, value);
    } catch (RuntimeException e) {
      throw logger.throwing(level, e);
    }
  }

  /**
   * <p>Validate that the argument condition is {@code true}; otherwise
   * throwing an exception with the specified message. This method is useful when
   * validating according to an arbitrary boolean expression, such as validating a
   * primitive number or using your own custom validation expression.</p>
   *
   * <pre>
   * Validate.isTrue(i &gt;= min &amp; i &lt;= max, "The value must be between %d and %d", min, max);
   * Validate.isTrue(myObject.isOk(), "The object is not okay");</pre>
   *
   * @param logger  the logger via which to log the exception
   * @param expression  the boolean expression to check
   * @param message  the {@link String#format(String, Object...)} exception message if invalid, not null
   * @param values  the optional values for the formatted exception message, null array not recommended
   * @throws IllegalArgumentException if expression is {@code false}
   *
   * @see #isTrue(Logger, boolean)
   * @see #isTrue(Logger, boolean, String, long)
   * @see #isTrue(Logger, boolean, String, double)
   */
  public static void isTrue(Logger logger, boolean expression, String message, Object... values) {
    try {
      org.apache.commons.lang3.Validate.isTrue(expression, message, values);
    } catch (RuntimeException e) {
      throw logger.throwing(e);
    }
  }
  /**
   * <p>Validate that the argument condition is {@code true}; otherwise
   * throwing an exception with the specified message. This method is useful when
   * validating according to an arbitrary boolean expression, such as validating a
   * primitive number or using your own custom validation expression.</p>
   *
   * <pre>
   * Validate.isTrue(i &gt;= min &amp; i &lt;= max, "The value must be between %d and %d", min, max);
   * Validate.isTrue(myObject.isOk(), "The object is not okay");</pre>
   *
   * @param logger  the logger via which to log the exception
   * @param level  the level at which to log the exception
   * @param expression  the boolean expression to check
   * @param message  the {@link String#format(String, Object...)} exception message if invalid, not null
   * @param values  the optional values for the formatted exception message, null array not recommended
   * @throws IllegalArgumentException if expression is {@code false}
   *
   * @see #isTrue(Logger, boolean)
   * @see #isTrue(Logger, boolean, String, long)
   * @see #isTrue(Logger, boolean, String, double)
   */
  public static void isTrue(Logger logger, Level level, boolean expression, String message, Object... values) {
    try {
      org.apache.commons.lang3.Validate.isTrue(expression, message, values);
    } catch (RuntimeException e) {
      throw logger.throwing(level, e);
    }
  }

  /**
   * <p>Validate that the argument condition is {@code true}; otherwise
   * throwing an exception. This method is useful when validating according
   * to an arbitrary boolean expression, such as validating a
   * primitive number or using your own custom validation expression.</p>
   *
   * <pre>
   * Validate.isTrue(i &gt; 0);
   * Validate.isTrue(myObject.isOk());</pre>
   *
   * <p>The message of the exception is &quot;The validated expression is
   * false&quot;.</p>
   *
   * @param logger  the logger via which to log the exception
   * @param expression  the boolean expression to check
   * @throws IllegalArgumentException if expression is {@code false}
   *
   * @see #isTrue(Logger, boolean, String, long)
   * @see #isTrue(Logger, boolean, String, double)
   * @see #isTrue(Logger, boolean, String, Object...)
   */
  public static void isTrue(Logger logger, boolean expression) {
    try {
      org.apache.commons.lang3.Validate.isTrue(expression);
    } catch (RuntimeException e) {
      throw logger.throwing(e);
    }
  }
  /**
   * <p>Validate that the argument condition is {@code true}; otherwise
   * throwing an exception. This method is useful when validating according
   * to an arbitrary boolean expression, such as validating a
   * primitive number or using your own custom validation expression.</p>
   *
   * <pre>
   * Validate.isTrue(i &gt; 0);
   * Validate.isTrue(myObject.isOk());</pre>
   *
   * <p>The message of the exception is &quot;The validated expression is
   * false&quot;.</p>
   *
   * @param logger  the logger via which to log the exception
   * @param level  the level at which to log the exception
   * @param expression  the boolean expression to check
   * @throws IllegalArgumentException if expression is {@code false}
   *
   * @see #isTrue(Logger, boolean, String, long)
   * @see #isTrue(Logger, boolean, String, double)
   * @see #isTrue(Logger, boolean, String, Object...)
   */
  public static void isTrue(Logger logger, Level level, boolean expression) {
    try {
      org.apache.commons.lang3.Validate.isTrue(expression);
    } catch (RuntimeException e) {
      throw logger.throwing(level, e);
    }
  }

  /**
   * <p>Validate that the specified argument is not {@code null};
   * otherwise throwing an exception.
   *
   * <pre>Validate.notNull(myObject, "The object must not be null");</pre>
   *
   * <p>The message of the exception is &quot;The validated object is
   * null&quot;.</p>
   *
   * @param <T> the object type
   * @param logger  the logger via which to log the exception
   * @param object  the object to check
   * @return the validated object (never {@code null} for method chaining)
   * @throws NullPointerException if the object is {@code null}
   *
   * @see #notNull(Logger, Object, String, Object...)
   */
  public static <T> T notNull(Logger logger, T object) {
    try {
      return org.apache.commons.lang3.Validate.notNull(object);
    } catch (RuntimeException e) {
      throw logger.throwing(e);
    }
  }
  /**
   * <p>Validate that the specified argument is not {@code null};
   * otherwise throwing an exception.
   *
   * <pre>Validate.notNull(myObject, "The object must not be null");</pre>
   *
   * <p>The message of the exception is &quot;The validated object is
   * null&quot;.</p>
   *
   * @param <T> the object type
   * @param logger  the logger via which to log the exception
   * @param level  the level at which to log the exception
   * @param object  the object to check
   * @return the validated object (never {@code null} for method chaining)
   * @throws NullPointerException if the object is {@code null}
   *
   * @see #notNull(Logger, Object, String, Object...)
   */
  public static <T> T notNull(Logger logger, Level level, T object) {
    try {
      return org.apache.commons.lang3.Validate.notNull(object);
    } catch (RuntimeException e) {
      throw logger.throwing(level, e);
    }
  }

  /**
   * <p>Validate that the specified argument is not {@code null};
   * otherwise throwing an exception with the specified message.
   *
   * <pre>Validate.notNull(myObject, "The object must not be null");</pre>
   *
   * @param <T> the object type
   * @param logger  the logger via which to log the exception
   * @param object  the object to check
   * @param message  the {@link String#format(String, Object...)} exception message if invalid, not null
   * @param values  the optional values for the formatted exception message
   * @return the validated object (never {@code null} for method chaining)
   * @throws NullPointerException if the object is {@code null}
   *
   * @see #notNull(Logger, Object)
   */
  public static <T> T notNull(Logger logger, T object, String message, Object... values) {
    try {
      return org.apache.commons.lang3.Validate.notNull(object, message, values);
    } catch (RuntimeException e) {
      throw logger.throwing(e);
    }
  }
  /**
   * <p>Validate that the specified argument is not {@code null};
   * otherwise throwing an exception with the specified message.
   *
   * <pre>Validate.notNull(myObject, "The object must not be null");</pre>
   *
   * @param <T> the object type
   * @param logger  the logger via which to log the exception
   * @param level  the level at which to log the exception
   * @param object  the object to check
   * @param message  the {@link String#format(String, Object...)} exception message if invalid, not null
   * @param values  the optional values for the formatted exception message
   * @return the validated object (never {@code null} for method chaining)
   * @throws NullPointerException if the object is {@code null}
   *
   * @see #notNull(Logger, Object)
   */
  public static <T> T notNull(Logger logger, Level level, T object, String message, Object... values) {
    try {
      return org.apache.commons.lang3.Validate.notNull(object, message, values);
    } catch (RuntimeException e) {
      throw logger.throwing(level, e);
    }
  }

  /**
   * <p>Validate that the specified argument array is neither {@code null}
   * nor a length of zero (no elements); otherwise throwing an exception
   * with the specified message.
   *
   * <pre>Validate.notEmpty(myArray, "The array must not be empty");</pre>
   *
   * @param <T> the array type
   * @param logger  the logger via which to log the exception
   * @param array  the array to check, validated not null by this method
   * @param message  the {@link String#format(String, Object...)} exception message if invalid, not null
   * @param values  the optional values for the formatted exception message, null array not recommended
   * @return the validated array (never {@code null} method for chaining)
   * @throws NullPointerException if the array is {@code null}
   * @throws IllegalArgumentException if the array is empty
   *
   * @see #notEmpty(Logger, Object[])
   */
  public static <T> T[] notEmpty(Logger logger, T[] array, String message, Object... values) {
    try {
      return org.apache.commons.lang3.Validate.notEmpty(array, message, values);
    } catch (RuntimeException e) {
      throw logger.throwing(e);
    }
  }
  /**
   * <p>Validate that the specified argument array is neither {@code null}
   * nor a length of zero (no elements); otherwise throwing an exception
   * with the specified message.
   *
   * <pre>Validate.notEmpty(myArray, "The array must not be empty");</pre>
   *
   * @param <T> the array type
   * @param logger  the logger via which to log the exception
   * @param level  the level at which to log the exception
   * @param array  the array to check, validated not null by this method
   * @param message  the {@link String#format(String, Object...)} exception message if invalid, not null
   * @param values  the optional values for the formatted exception message, null array not recommended
   * @return the validated array (never {@code null} method for chaining)
   * @throws NullPointerException if the array is {@code null}
   * @throws IllegalArgumentException if the array is empty
   *
   * @see #notEmpty(Logger, Object[])
   */
  public static <T> T[] notEmpty(Logger logger, Level level, T[] array, String message, Object... values) {
    try {
      return org.apache.commons.lang3.Validate.notEmpty(array, message, values);
    } catch (RuntimeException e) {
      throw logger.throwing(level, e);
    }
  }

  /**
   * <p>Validate that the specified argument array is neither {@code null}
   * nor a length of zero (no elements); otherwise throwing an exception.
   *
   * <pre>Validate.notEmpty(myArray);</pre>
   *
   * <p>The message in the exception is &quot;The validated array is
   * empty&quot;.
   *
   * @param <T> the array type
   * @param logger  the logger via which to log the exception
   * @param array  the array to check, validated not null by this method
   * @return the validated array (never {@code null} method for chaining)
   * @throws NullPointerException if the array is {@code null}
   * @throws IllegalArgumentException if the array is empty
   *
   * @see #notEmpty(Logger, Object[], String, Object...)
   */
  public static <T> T[] notEmpty(Logger logger, T[] array) {
    try {
      return org.apache.commons.lang3.Validate.notEmpty(array);
    } catch (RuntimeException e) {
      throw logger.throwing(e);
    }
  }
  /**
   * <p>Validate that the specified argument array is neither {@code null}
   * nor a length of zero (no elements); otherwise throwing an exception.
   *
   * <pre>Validate.notEmpty(myArray);</pre>
   *
   * <p>The message in the exception is &quot;The validated array is
   * empty&quot;.
   *
   * @param <T> the array type
   * @param logger  the logger via which to log the exception
   * @param level  the level at which to log the exception
   * @param array  the array to check, validated not null by this method
   * @return the validated array (never {@code null} method for chaining)
   * @throws NullPointerException if the array is {@code null}
   * @throws IllegalArgumentException if the array is empty
   *
   * @see #notEmpty(Logger, Object[], String, Object...)
   */
  public static <T> T[] notEmpty(Logger logger, Level level, T[] array) {
    try {
      return org.apache.commons.lang3.Validate.notEmpty(array);
    } catch (RuntimeException e) {
      throw logger.throwing(level, e);
    }
  }

  /**
   * <p>Validate that the specified argument collection is neither {@code null}
   * nor a size of zero (no elements); otherwise throwing an exception
   * with the specified message.
   *
   * <pre>Validate.notEmpty(myCollection, "The collection must not be empty");</pre>
   *
   * @param <T> the collection type
   * @param logger  the logger via which to log the exception
   * @param collection  the collection to check, validated not null by this method
   * @param message  the {@link String#format(String, Object...)} exception message if invalid, not null
   * @param values  the optional values for the formatted exception message, null array not recommended
   * @return the validated collection (never {@code null} method for chaining)
   * @throws NullPointerException if the collection is {@code null}
   * @throws IllegalArgumentException if the collection is empty
   *
   * @see #notEmpty(Logger, Object[])
   */
  public static <T extends Collection<?>> T notEmpty(Logger logger, T collection, String message, Object... values) {
    try {
      return org.apache.commons.lang3.Validate.notEmpty(collection, message, values);
    } catch (RuntimeException e) {
      throw logger.throwing(e);
    }
  }
  /**
   * <p>Validate that the specified argument collection is neither {@code null}
   * nor a size of zero (no elements); otherwise throwing an exception
   * with the specified message.
   *
   * <pre>Validate.notEmpty(myCollection, "The collection must not be empty");</pre>
   *
   * @param <T> the collection type
   * @param logger  the logger via which to log the exception
   * @param level  the level at which to log the exception
   * @param collection  the collection to check, validated not null by this method
   * @param message  the {@link String#format(String, Object...)} exception message if invalid, not null
   * @param values  the optional values for the formatted exception message, null array not recommended
   * @return the validated collection (never {@code null} method for chaining)
   * @throws NullPointerException if the collection is {@code null}
   * @throws IllegalArgumentException if the collection is empty
   *
   * @see #notEmpty(Logger, Object[])
   */
  public static <T extends Collection<?>> T notEmpty(Logger logger, Level level, T collection, String message, Object... values) {
    try {
      return org.apache.commons.lang3.Validate.notEmpty(collection, message, values);
    } catch (RuntimeException e) {
      throw logger.throwing(level, e);
    }
  }

  /**
   * <p>Validate that the specified argument collection is neither {@code null}
   * nor a size of zero (no elements); otherwise throwing an exception.
   *
   * <pre>Validate.notEmpty(myCollection);</pre>
   *
   * <p>The message in the exception is &quot;The validated collection is
   * empty&quot;.</p>
   *
   * @param <T> the collection type
   * @param logger  the logger via which to log the exception
   * @param collection  the collection to check, validated not null by this method
   * @return the validated collection (never {@code null} method for chaining)
   * @throws NullPointerException if the collection is {@code null}
   * @throws IllegalArgumentException if the collection is empty
   *
   * @see #notEmpty(Logger, Collection, String, Object...)
   */
  public static <T extends Collection<?>> T notEmpty(Logger logger, T collection) {
    try {
      return org.apache.commons.lang3.Validate.notEmpty(collection);
    } catch (RuntimeException e) {
      throw logger.throwing(e);
    }
  }
  /**
   * <p>Validate that the specified argument collection is neither {@code null}
   * nor a size of zero (no elements); otherwise throwing an exception.
   *
   * <pre>Validate.notEmpty(myCollection);</pre>
   *
   * <p>The message in the exception is &quot;The validated collection is
   * empty&quot;.</p>
   *
   * @param <T> the collection type
   * @param logger  the logger via which to log the exception
   * @param level  the level at which to log the exception
   * @param collection  the collection to check, validated not null by this method
   * @return the validated collection (never {@code null} method for chaining)
   * @throws NullPointerException if the collection is {@code null}
   * @throws IllegalArgumentException if the collection is empty
   *
   * @see #notEmpty(Logger, Collection, String, Object...)
   */
  public static <T extends Collection<?>> T notEmpty(Logger logger, Level level, T collection) {
    try {
      return org.apache.commons.lang3.Validate.notEmpty(collection);
    } catch (RuntimeException e) {
      throw logger.throwing(level, e);
    }
  }

  /**
   * <p>Validate that the specified argument map is neither {@code null}
   * nor a size of zero (no elements); otherwise throwing an exception
   * with the specified message.
   *
   * <pre>Validate.notEmpty(myMap, "The map must not be empty");</pre>
   *
   * @param <T> the map type
   * @param logger  the logger via which to log the exception
   * @param map  the map to check, validated not null by this method
   * @param message  the {@link String#format(String, Object...)} exception message if invalid, not null
   * @param values  the optional values for the formatted exception message, null array not recommended
   * @return the validated map (never {@code null} method for chaining)
   * @throws NullPointerException if the map is {@code null}
   * @throws IllegalArgumentException if the map is empty
   *
   * @see #notEmpty(Logger, Object[])
   */
  public static <T extends Map<?, ?>> T notEmpty(Logger logger, T map, String message, Object... values) {
    try {
      return org.apache.commons.lang3.Validate.notEmpty(map, message, values);
    } catch (RuntimeException e) {
      throw logger.throwing(e);
    }
  }
  /**
   * <p>Validate that the specified argument map is neither {@code null}
   * nor a size of zero (no elements); otherwise throwing an exception
   * with the specified message.
   *
   * <pre>Validate.notEmpty(myMap, "The map must not be empty");</pre>
   *
   * @param <T> the map type
   * @param logger  the logger via which to log the exception
   * @param level  the level at which to log the exception
   * @param map  the map to check, validated not null by this method
   * @param message  the {@link String#format(String, Object...)} exception message if invalid, not null
   * @param values  the optional values for the formatted exception message, null array not recommended
   * @return the validated map (never {@code null} method for chaining)
   * @throws NullPointerException if the map is {@code null}
   * @throws IllegalArgumentException if the map is empty
   *
   * @see #notEmpty(Logger, Object[])
   */
  public static <T extends Map<?, ?>> T notEmpty(Logger logger, Level level, T map, String message, Object... values) {
    try {
      return org.apache.commons.lang3.Validate.notEmpty(map, message, values);
    } catch (RuntimeException e) {
      throw logger.throwing(level, e);
    }
  }

  /**
   * <p>Validate that the specified argument map is neither {@code null}
   * nor a size of zero (no elements); otherwise throwing an exception.
   *
   * <pre>Validate.notEmpty(myMap);</pre>
   *
   * <p>The message in the exception is &quot;The validated map is
   * empty&quot;.</p>
   *
   * @param <T> the map type
   * @param logger  the logger via which to log the exception
   * @param map  the map to check, validated not null by this method
   * @return the validated map (never {@code null} method for chaining)
   * @throws NullPointerException if the map is {@code null}
   * @throws IllegalArgumentException if the map is empty
   *
   * @see #notEmpty(Logger, Map, String, Object...)
   */
  public static <T extends Map<?, ?>> T notEmpty(Logger logger, T map) {
    try {
      return org.apache.commons.lang3.Validate.notEmpty(map);
    } catch (RuntimeException e) {
      throw logger.throwing(e);
    }
  }
  /**
   * <p>Validate that the specified argument map is neither {@code null}
   * nor a size of zero (no elements); otherwise throwing an exception.
   *
   * <pre>Validate.notEmpty(myMap);</pre>
   *
   * <p>The message in the exception is &quot;The validated map is
   * empty&quot;.</p>
   *
   * @param <T> the map type
   * @param logger  the logger via which to log the exception
   * @param level  the level at which to log the exception
   * @param map  the map to check, validated not null by this method
   * @return the validated map (never {@code null} method for chaining)
   * @throws NullPointerException if the map is {@code null}
   * @throws IllegalArgumentException if the map is empty
   *
   * @see #notEmpty(Logger, Map, String, Object...)
   */
  public static <T extends Map<?, ?>> T notEmpty(Logger logger, Level level, T map) {
    try {
      return org.apache.commons.lang3.Validate.notEmpty(map);
    } catch (RuntimeException e) {
      throw logger.throwing(level, e);
    }
  }

  /**
   * <p>Validate that the specified argument character sequence is
   * neither {@code null} nor a length of zero (no characters);
   * otherwise throwing an exception with the specified message.
   *
   * <pre>Validate.notEmpty(myString, "The string must not be empty");</pre>
   *
   * @param <T> the character sequence type
   * @param logger  the logger via which to log the exception
   * @param chars  the character sequence to check, validated not null by this method
   * @param message  the {@link String#format(String, Object...)} exception message if invalid, not null
   * @param values  the optional values for the formatted exception message, null array not recommended
   * @return the validated character sequence (never {@code null} method for chaining)
   * @throws NullPointerException if the character sequence is {@code null}
   * @throws IllegalArgumentException if the character sequence is empty
   *
   * @see #notEmpty(Logger, CharSequence)
   */
  public static <T extends CharSequence> T notEmpty(Logger logger, T chars, String message, Object... values) {
    try {
      return org.apache.commons.lang3.Validate.notEmpty(chars, message, values);
    } catch (RuntimeException e) {
      throw logger.throwing(e);
    }
  }
  /**
   * <p>Validate that the specified argument character sequence is
   * neither {@code null} nor a length of zero (no characters);
   * otherwise throwing an exception with the specified message.
   *
   * <pre>Validate.notEmpty(myString, "The string must not be empty");</pre>
   *
   * @param <T> the character sequence type
   * @param logger  the logger via which to log the exception
   * @param level  the level at which to log the exception
   * @param chars  the character sequence to check, validated not null by this method
   * @param message  the {@link String#format(String, Object...)} exception message if invalid, not null
   * @param values  the optional values for the formatted exception message, null array not recommended
   * @return the validated character sequence (never {@code null} method for chaining)
   * @throws NullPointerException if the character sequence is {@code null}
   * @throws IllegalArgumentException if the character sequence is empty
   *
   * @see #notEmpty(Logger, CharSequence)
   */
  public static <T extends CharSequence> T notEmpty(Logger logger, Level level, T chars, String message, Object... values) {
    try {
      return org.apache.commons.lang3.Validate.notEmpty(chars, message, values);
    } catch (RuntimeException e) {
      throw logger.throwing(level, e);
    }
  }

  /**
   * <p>Validate that the specified argument character sequence is
   * neither {@code null} nor a length of zero (no characters);
   * otherwise throwing an exception with the specified message.
   *
   * <pre>Validate.notEmpty(myString);</pre>
   *
   * <p>The message in the exception is &quot;The validated
   * character sequence is empty&quot;.</p>
   *
   * @param <T> the character sequence type
   * @param logger  the logger via which to log the exception
   * @param chars  the character sequence to check, validated not null by this method
   * @return the validated character sequence (never {@code null} method for chaining)
   * @throws NullPointerException if the character sequence is {@code null}
   * @throws IllegalArgumentException if the character sequence is empty
   *
   * @see #notEmpty(Logger, CharSequence, String, Object...)
   */
  public static <T extends CharSequence> T notEmpty(Logger logger, T chars) {
    try {
      return org.apache.commons.lang3.Validate.notEmpty(chars);
    } catch (RuntimeException e) {
      throw logger.throwing(e);
    }
  }
  /**
   * <p>Validate that the specified argument character sequence is
   * neither {@code null} nor a length of zero (no characters);
   * otherwise throwing an exception with the specified message.
   *
   * <pre>Validate.notEmpty(myString);</pre>
   *
   * <p>The message in the exception is &quot;The validated
   * character sequence is empty&quot;.</p>
   *
   * @param <T> the character sequence type
   * @param logger  the logger via which to log the exception
   * @param level  the level at which to log the exception
   * @param chars  the character sequence to check, validated not null by this method
   * @return the validated character sequence (never {@code null} method for chaining)
   * @throws NullPointerException if the character sequence is {@code null}
   * @throws IllegalArgumentException if the character sequence is empty
   *
   * @see #notEmpty(Logger, CharSequence, String, Object...)
   */
  public static <T extends CharSequence> T notEmpty(Logger logger, Level level, T chars) {
    try {
      return org.apache.commons.lang3.Validate.notEmpty(chars);
    } catch (RuntimeException e) {
      throw logger.throwing(level, e);
    }
  }

  /**
   * <p>Validate that the specified argument character sequence is
   * neither {@code null}, a length of zero (no characters), empty
   * nor whitespace; otherwise throwing an exception with the specified
   * message.
   *
   * <pre>Validate.notBlank(myString, "The string must not be blank");</pre>
   *
   * @param <T> the character sequence type
   * @param logger  the logger via which to log the exception
   * @param chars  the character sequence to check, validated not null by this method
   * @param message  the {@link String#format(String, Object...)} exception message if invalid, not null
   * @param values  the optional values for the formatted exception message, null array not recommended
   * @return the validated character sequence (never {@code null} method for chaining)
   * @throws NullPointerException if the character sequence is {@code null}
   * @throws IllegalArgumentException if the character sequence is blank
   *
   * @see #notBlank(Logger, CharSequence)
   */
  public static <T extends CharSequence> T notBlank(Logger logger, T chars, String message, Object... values) {
    try {
      return org.apache.commons.lang3.Validate.notBlank(chars, message, values);
    } catch (RuntimeException e) {
      throw logger.throwing(e);
    }
  }
  /**
   * <p>Validate that the specified argument character sequence is
   * neither {@code null}, a length of zero (no characters), empty
   * nor whitespace; otherwise throwing an exception with the specified
   * message.
   *
   * <pre>Validate.notBlank(myString, "The string must not be blank");</pre>
   *
   * @param <T> the character sequence type
   * @param logger  the logger via which to log the exception
   * @param level  the level at which to log the exception
   * @param chars  the character sequence to check, validated not null by this method
   * @param message  the {@link String#format(String, Object...)} exception message if invalid, not null
   * @param values  the optional values for the formatted exception message, null array not recommended
   * @return the validated character sequence (never {@code null} method for chaining)
   * @throws NullPointerException if the character sequence is {@code null}
   * @throws IllegalArgumentException if the character sequence is blank
   *
   * @see #notBlank(Logger, CharSequence)
   */
  public static <T extends CharSequence> T notBlank(Logger logger, Level level, T chars, String message, Object... values) {
    try {
      return org.apache.commons.lang3.Validate.notBlank(chars, message, values);
    } catch (RuntimeException e) {
      throw logger.throwing(level, e);
    }
  }

  /**
   * <p>Validate that the specified argument character sequence is
   * neither {@code null}, a length of zero (no characters), empty
   * nor whitespace; otherwise throwing an exception.
   *
   * <pre>Validate.notBlank(myString);</pre>
   *
   * <p>The message in the exception is &quot;The validated character
   * sequence is blank&quot;.</p>
   *
   * @param <T> the character sequence type
   * @param logger  the logger via which to log the exception
   * @param chars  the character sequence to check, validated not null by this method
   * @return the validated character sequence (never {@code null} method for chaining)
   * @throws NullPointerException if the character sequence is {@code null}
   * @throws IllegalArgumentException if the character sequence is blank
   *
   * @see #notBlank(Logger, CharSequence, String, Object...)
   */
  public static <T extends CharSequence> T notBlank(Logger logger, T chars) {
    try {
      return org.apache.commons.lang3.Validate.notBlank(chars);
    } catch (RuntimeException e) {
      throw logger.throwing(e);
    }
  }
  /**
   * <p>Validate that the specified argument character sequence is
   * neither {@code null}, a length of zero (no characters), empty
   * nor whitespace; otherwise throwing an exception.
   *
   * <pre>Validate.notBlank(myString);</pre>
   *
   * <p>The message in the exception is &quot;The validated character
   * sequence is blank&quot;.</p>
   *
   * @param <T> the character sequence type
   * @param logger  the logger via which to log the exception
   * @param level  the level at which to log the exception
   * @param chars  the character sequence to check, validated not null by this method
   * @return the validated character sequence (never {@code null} method for chaining)
   * @throws NullPointerException if the character sequence is {@code null}
   * @throws IllegalArgumentException if the character sequence is blank
   *
   * @see #notBlank(Logger, CharSequence, String, Object...)
   */
  public static <T extends CharSequence> T notBlank(Logger logger, Level level, T chars) {
    try {
      return org.apache.commons.lang3.Validate.notBlank(chars);
    } catch (RuntimeException e) {
      throw logger.throwing(level, e);
    }
  }

  /**
   * <p>Validate that the specified argument array is neither
   * {@code null} nor contains any elements that are {@code null};
   * otherwise throwing an exception with the specified message.
   *
   * <pre>Validate.noNullElements(myArray, "The array contain null at position %d");</pre>
   *
   * <p>If the array is {@code null}, then the message in the exception
   * is &quot;The validated object is null&quot;.</p>
   *
   * <p>If the array has a {@code null} element, then the iteration
   * index of the invalid element is appended to the {@code values}
   * argument.</p>
   *
   * @param <T> the array type
   * @param logger  the logger via which to log the exception
   * @param array  the array to check, validated not null by this method
   * @param message  the {@link String#format(String, Object...)} exception message if invalid, not null
   * @param values  the optional values for the formatted exception message, null array not recommended
   * @return the validated array (never {@code null} method for chaining)
   * @throws NullPointerException if the array is {@code null}
   * @throws IllegalArgumentException if an element is {@code null}
   *
   * @see #noNullElements(Logger, Object[])
   */
  public static <T> T[] noNullElements(Logger logger, T[] array, String message, Object... values) {
    try {
      return org.apache.commons.lang3.Validate.noNullElements(array, message, values);
    } catch (RuntimeException e) {
      throw logger.throwing(e);
    }
  }
  /**
   * <p>Validate that the specified argument array is neither
   * {@code null} nor contains any elements that are {@code null};
   * otherwise throwing an exception with the specified message.
   *
   * <pre>Validate.noNullElements(myArray, "The array contain null at position %d");</pre>
   *
   * <p>If the array is {@code null}, then the message in the exception
   * is &quot;The validated object is null&quot;.</p>
   *
   * <p>If the array has a {@code null} element, then the iteration
   * index of the invalid element is appended to the {@code values}
   * argument.</p>
   *
   * @param <T> the array type
   * @param logger  the logger via which to log the exception
   * @param level  the level at which to log the exception
   * @param array  the array to check, validated not null by this method
   * @param message  the {@link String#format(String, Object...)} exception message if invalid, not null
   * @param values  the optional values for the formatted exception message, null array not recommended
   * @return the validated array (never {@code null} method for chaining)
   * @throws NullPointerException if the array is {@code null}
   * @throws IllegalArgumentException if an element is {@code null}
   *
   * @see #noNullElements(Logger, Object[])
   */
  public static <T> T[] noNullElements(Logger logger, Level level, T[] array, String message, Object... values) {
    try {
      return org.apache.commons.lang3.Validate.noNullElements(array, message, values);
    } catch (RuntimeException e) {
      throw logger.throwing(level, e);
    }
  }

  /**
   * <p>Validate that the specified argument array is neither
   * {@code null} nor contains any elements that are {@code null};
   * otherwise throwing an exception.
   *
   * <pre>Validate.noNullElements(myArray);</pre>
   *
   * <p>If the array is {@code null}, then the message in the exception
   * is &quot;The validated object is null&quot;.</p>
   *
   * <p>If the array has a {@code null} element, then the message in the
   * exception is &quot;The validated array contains null element at index:
   * &quot; followed by the index.</p>
   *
   * @param <T> the array type
   * @param logger  the logger via which to log the exception
   * @param array  the array to check, validated not null by this method
   * @return the validated array (never {@code null} method for chaining)
   * @throws NullPointerException if the array is {@code null}
   * @throws IllegalArgumentException if an element is {@code null}
   *
   * @see #noNullElements(Logger, Object[], String, Object...)
   */
  public static <T> T[] noNullElements(Logger logger, T[] array) {
    try {
      return org.apache.commons.lang3.Validate.noNullElements(array);
    } catch (RuntimeException e) {
      throw logger.throwing(e);
    }
  }
  /**
   * <p>Validate that the specified argument array is neither
   * {@code null} nor contains any elements that are {@code null};
   * otherwise throwing an exception.
   *
   * <pre>Validate.noNullElements(myArray);</pre>
   *
   * <p>If the array is {@code null}, then the message in the exception
   * is &quot;The validated object is null&quot;.</p>
   *
   * <p>If the array has a {@code null} element, then the message in the
   * exception is &quot;The validated array contains null element at index:
   * &quot; followed by the index.</p>
   *
   * @param <T> the array type
   * @param logger  the logger via which to log the exception
   * @param level  the level at which to log the exception
   * @param array  the array to check, validated not null by this method
   * @return the validated array (never {@code null} method for chaining)
   * @throws NullPointerException if the array is {@code null}
   * @throws IllegalArgumentException if an element is {@code null}
   *
   * @see #noNullElements(Logger, Object[], String, Object...)
   */
  public static <T> T[] noNullElements(Logger logger, Level level, T[] array) {
    try {
      return org.apache.commons.lang3.Validate.noNullElements(array);
    } catch (RuntimeException e) {
      throw logger.throwing(level, e);
    }
  }

  /**
   * <p>Validate that the specified argument iterable is neither
   * {@code null} nor contains any elements that are {@code null};
   * otherwise throwing an exception with the specified message.
   *
   * <pre>Validate.noNullElements(myCollection, "The collection contains null at position %d");</pre>
   *
   * <p>If the iterable is {@code null}, then the message in the exception
   * is &quot;The validated object is null&quot;.</p>
   *
   * <p>If the iterable has a {@code null} element, then the iteration
   * index of the invalid element is appended to the {@code values}
   * argument.</p>
   *
   * @param <T> the iterable type
   * @param logger  the logger via which to log the exception
   * @param iterable  the iterable to check, validated not null by this method
   * @param message  the {@link String#format(String, Object...)} exception message if invalid, not null
   * @param values  the optional values for the formatted exception message, null array not recommended
   * @return the validated iterable (never {@code null} method for chaining)
   * @throws NullPointerException if the array is {@code null}
   * @throws IllegalArgumentException if an element is {@code null}
   *
   * @see #noNullElements(Logger, Iterable)
   */
  public static <T extends Iterable<?>> T noNullElements(Logger logger, T iterable, String message, Object... values) {
    try {
      return org.apache.commons.lang3.Validate.noNullElements(iterable, message, values);
    } catch (RuntimeException e) {
      throw logger.throwing(e);
    }
  }
  /**
   * <p>Validate that the specified argument iterable is neither
   * {@code null} nor contains any elements that are {@code null};
   * otherwise throwing an exception with the specified message.
   *
   * <pre>Validate.noNullElements(myCollection, "The collection contains null at position %d");</pre>
   *
   * <p>If the iterable is {@code null}, then the message in the exception
   * is &quot;The validated object is null&quot;.</p>
   *
   * <p>If the iterable has a {@code null} element, then the iteration
   * index of the invalid element is appended to the {@code values}
   * argument.</p>
   *
   * @param <T> the iterable type
   * @param level  the level at which to log the exception
   * @param logger  the logger via which to log the exception
   * @param iterable  the iterable to check, validated not null by this method
   * @param message  the {@link String#format(String, Object...)} exception message if invalid, not null
   * @param values  the optional values for the formatted exception message, null array not recommended
   * @return the validated iterable (never {@code null} method for chaining)
   * @throws NullPointerException if the array is {@code null}
   * @throws IllegalArgumentException if an element is {@code null}
   *
   * @see #noNullElements(Logger, Iterable)
   */
  public static <T extends Iterable<?>> T noNullElements(Logger logger, Level level, T iterable, String message, Object... values) {
    try {
      return org.apache.commons.lang3.Validate.noNullElements(iterable, message, values);
    } catch (RuntimeException e) {
      throw logger.throwing(level, e);
    }
  }

  /**
   * <p>Validate that the specified argument iterable is neither
   * {@code null} nor contains any elements that are {@code null};
   * otherwise throwing an exception.
   *
   * <pre>Validate.noNullElements(myCollection);</pre>
   *
   * <p>If the iterable is {@code null}, then the message in the exception
   * is &quot;The validated object is null&quot;.</p>
   *
   * <p>If the array has a {@code null} element, then the message in the
   * exception is &quot;The validated iterable contains null element at index:
   * &quot; followed by the index.</p>
   *
   * @param <T> the iterable type
   * @param logger  the logger via which to log the exception
   * @param iterable  the iterable to check, validated not null by this method
   * @return the validated iterable (never {@code null} method for chaining)
   * @throws NullPointerException if the array is {@code null}
   * @throws IllegalArgumentException if an element is {@code null}
   *
   * @see #noNullElements(Logger, Iterable, String, Object...)
   */
  public static <T extends Iterable<?>> T noNullElements(Logger logger, T iterable) {
    try {
      return org.apache.commons.lang3.Validate.noNullElements(iterable);
    } catch (RuntimeException e) {
      throw logger.throwing(e);
    }
  }
  /**
   * <p>Validate that the specified argument iterable is neither
   * {@code null} nor contains any elements that are {@code null};
   * otherwise throwing an exception.
   *
   * <pre>Validate.noNullElements(myCollection);</pre>
   *
   * <p>If the iterable is {@code null}, then the message in the exception
   * is &quot;The validated object is null&quot;.</p>
   *
   * <p>If the array has a {@code null} element, then the message in the
   * exception is &quot;The validated iterable contains null element at index:
   * &quot; followed by the index.</p>
   *
   * @param <T> the iterable type
   * @param level  the level at which to log the exception
   * @param logger  the logger via which to log the exception
   * @param iterable  the iterable to check, validated not null by this method
   * @return the validated iterable (never {@code null} method for chaining)
   * @throws NullPointerException if the array is {@code null}
   * @throws IllegalArgumentException if an element is {@code null}
   *
   * @see #noNullElements(Logger, Iterable, String, Object...)
   */
  public static <T extends Iterable<?>> T noNullElements(Logger logger, Level level, T iterable) {
    try {
      return org.apache.commons.lang3.Validate.noNullElements(iterable);
    } catch (RuntimeException e) {
      throw logger.throwing(level, e);
    }
  }

  /**
   * <p>Validates that the index is within the bounds of the argument
   * array; otherwise throwing an exception with the specified message.</p>
   *
   * <pre>Validate.validIndex(myArray, 2, "The array index is invalid: ");</pre>
   *
   * <p>If the array is {@code null}, then the message of the exception
   * is &quot;The validated object is null&quot;.</p>
   *
   * @param <T> the array type
   * @param logger  the logger via which to log the exception
   * @param array  the array to check, validated not null by this method
   * @param index  the index to check
   * @param message  the {@link String#format(String, Object...)} exception message if invalid, not null
   * @param values  the optional values for the formatted exception message, null array not recommended
   * @return the validated array (never {@code null} for method chaining)
   * @throws NullPointerException if the array is {@code null}
   * @throws IndexOutOfBoundsException if the index is invalid
   *
   * @see #validIndex(Logger, Object[], int)
   */
  public static <T> T[] validIndex(Logger logger, T[] array, int index, String message, Object... values) {
    try {
      return org.apache.commons.lang3.Validate.validIndex(array, index, message, values);
    } catch (RuntimeException e) {
      throw logger.throwing(e);
    }
  }
  /**
   * <p>Validates that the index is within the bounds of the argument
   * array; otherwise throwing an exception with the specified message.</p>
   *
   * <pre>Validate.validIndex(myArray, 2, "The array index is invalid: ");</pre>
   *
   * <p>If the array is {@code null}, then the message of the exception
   * is &quot;The validated object is null&quot;.</p>
   *
   * @param <T> the array type
   * @param logger  the logger via which to log the exception
   * @param level  the level at which to log the exception
   * @param array  the array to check, validated not null by this method
   * @param index  the index to check
   * @param message  the {@link String#format(String, Object...)} exception message if invalid, not null
   * @param values  the optional values for the formatted exception message, null array not recommended
   * @return the validated array (never {@code null} for method chaining)
   * @throws NullPointerException if the array is {@code null}
   * @throws IndexOutOfBoundsException if the index is invalid
   *
   * @see #validIndex(Logger, Object[], int)
   */
  public static <T> T[] validIndex(Logger logger, Level level, T[] array, int index, String message, Object... values) {
    try {
      return org.apache.commons.lang3.Validate.validIndex(array, index, message, values);
    } catch (RuntimeException e) {
      throw logger.throwing(level, e);
    }
  }

  /**
   * <p>Validates that the index is within the bounds of the argument
   * array; otherwise throwing an exception.</p>
   *
   * <pre>Validate.validIndex(myArray, 2);</pre>
   *
   * <p>If the array is {@code null}, then the message of the exception
   * is &quot;The validated object is null&quot;.</p>
   *
   * <p>If the index is invalid, then the message of the exception is
   * &quot;The validated array index is invalid: &quot; followed by the
   * index.</p>
   *
   * @param <T> the array type
   * @param logger  the logger via which to log the exception
   * @param array  the array to check, validated not null by this method
   * @param index  the index to check
   * @return the validated array (never {@code null} for method chaining)
   * @throws NullPointerException if the array is {@code null}
   * @throws IndexOutOfBoundsException if the index is invalid
   *
   * @see #validIndex(Logger, Object[], int, String, Object...)
   */
  public static <T> T[] validIndex(Logger logger, T[] array, int index) {
    try {
      return org.apache.commons.lang3.Validate.validIndex(array, index);
    } catch (RuntimeException e) {
      throw logger.throwing(e);
    }
  }
  /**
   * <p>Validates that the index is within the bounds of the argument
   * array; otherwise throwing an exception.</p>
   *
   * <pre>Validate.validIndex(myArray, 2);</pre>
   *
   * <p>If the array is {@code null}, then the message of the exception
   * is &quot;The validated object is null&quot;.</p>
   *
   * <p>If the index is invalid, then the message of the exception is
   * &quot;The validated array index is invalid: &quot; followed by the
   * index.</p>
   *
   * @param <T> the array type
   * @param logger  the logger via which to log the exception
   * @param level  the level at which to log the exception
   * @param array  the array to check, validated not null by this method
   * @param index  the index to check
   * @return the validated array (never {@code null} for method chaining)
   * @throws NullPointerException if the array is {@code null}
   * @throws IndexOutOfBoundsException if the index is invalid
   *
   * @see #validIndex(Logger, Object[], int, String, Object...)
   */
  public static <T> T[] validIndex(Logger logger, Level level, T[] array, int index) {
    try {
      return org.apache.commons.lang3.Validate.validIndex(array, index);
    } catch (RuntimeException e) {
      throw logger.throwing(level, e);
    }
  }

  /**
   * <p>Validates that the index is within the bounds of the argument
   * collection; otherwise throwing an exception with the specified message.</p>
   *
   * <pre>Validate.validIndex(myCollection, 2, "The collection index is invalid: ");</pre>
   *
   * <p>If the collection is {@code null}, then the message of the
   * exception is &quot;The validated object is null&quot;.</p>
   *
   * @param <T> the collection type
   * @param logger  the logger via which to log the exception
   * @param collection  the collection to check, validated not null by this method
   * @param index  the index to check
   * @param message  the {@link String#format(String, Object...)} exception message if invalid, not null
   * @param values  the optional values for the formatted exception message, null array not recommended
   * @return the validated collection (never {@code null} for chaining)
   * @throws NullPointerException if the collection is {@code null}
   * @throws IndexOutOfBoundsException if the index is invalid
   *
   * @see #validIndex(Logger, Collection, int)
   */
  public static <T extends Collection<?>> T validIndex(Logger logger, T collection, int index, String message, Object... values) {
    try {
      return org.apache.commons.lang3.Validate.validIndex(collection, index, message, values);
    } catch (RuntimeException e) {
      throw logger.throwing(e);
    }
  }
  /**
   * <p>Validates that the index is within the bounds of the argument
   * collection; otherwise throwing an exception with the specified message.</p>
   *
   * <pre>Validate.validIndex(myCollection, 2, "The collection index is invalid: ");</pre>
   *
   * <p>If the collection is {@code null}, then the message of the
   * exception is &quot;The validated object is null&quot;.</p>
   *
   * @param <T> the collection type
   * @param logger  the logger via which to log the exception
   * @param level  the level at which to log the exception
   * @param collection  the collection to check, validated not null by this method
   * @param index  the index to check
   * @param message  the {@link String#format(String, Object...)} exception message if invalid, not null
   * @param values  the optional values for the formatted exception message, null array not recommended
   * @return the validated collection (never {@code null} for chaining)
   * @throws NullPointerException if the collection is {@code null}
   * @throws IndexOutOfBoundsException if the index is invalid
   *
   * @see #validIndex(Logger, Collection, int)
   */
  public static <T extends Collection<?>> T validIndex(Logger logger, Level level, T collection, int index, String message, Object... values) {
    try {
      return org.apache.commons.lang3.Validate.validIndex(collection, index, message, values);
    } catch (RuntimeException e) {
      throw logger.throwing(level, e);
    }
  }

  /**
   * <p>Validates that the index is within the bounds of the argument
   * collection; otherwise throwing an exception.</p>
   *
   * <pre>Validate.validIndex(myCollection, 2);</pre>
   *
   * <p>If the index is invalid, then the message of the exception
   * is &quot;The validated collection index is invalid: &quot;
   * followed by the index.</p>
   *
   * @param <T> the collection type
   * @param logger  the logger via which to log the exception
   * @param collection  the collection to check, validated not null by this method
   * @param index  the index to check
   * @return the validated collection (never {@code null} for method chaining)
   * @throws NullPointerException if the collection is {@code null}
   * @throws IndexOutOfBoundsException if the index is invalid
   *
   * @see #validIndex(Logger, Collection, int, String, Object...)
   */
  public static <T extends Collection<?>> T validIndex(Logger logger, T collection, int index) {
    try {
      return org.apache.commons.lang3.Validate.validIndex(collection, index);
    } catch (RuntimeException e) {
      throw logger.throwing(e);
    }
  }
  /**
   * <p>Validates that the index is within the bounds of the argument
   * collection; otherwise throwing an exception.</p>
   *
   * <pre>Validate.validIndex(myCollection, 2);</pre>
   *
   * <p>If the index is invalid, then the message of the exception
   * is &quot;The validated collection index is invalid: &quot;
   * followed by the index.</p>
   *
   * @param <T> the collection type
   * @param logger  the logger via which to log the exception
   * @param level  the level at which to log the exception
   * @param collection  the collection to check, validated not null by this method
   * @param index  the index to check
   * @return the validated collection (never {@code null} for method chaining)
   * @throws NullPointerException if the collection is {@code null}
   * @throws IndexOutOfBoundsException if the index is invalid
   *
   * @see #validIndex(Logger, Collection, int, String, Object...)
   */
  public static <T extends Collection<?>> T validIndex(Logger logger, Level level, T collection, int index) {
    try {
      return org.apache.commons.lang3.Validate.validIndex(collection, index);
    } catch (RuntimeException e) {
      throw logger.throwing(level, e);
    }
  }

  /**
   * <p>Validates that the index is within the bounds of the argument
   * character sequence; otherwise throwing an exception with the
   * specified message.</p>
   *
   * <pre>Validate.validIndex(myStr, 2, "The string index is invalid: ");</pre>
   *
   * <p>If the character sequence is {@code null}, then the message
   * of the exception is &quot;The validated object is null&quot;.</p>
   *
   * @param <T> the character sequence type
   * @param logger  the logger via which to log the exception
   * @param chars  the character sequence to check, validated not null by this method
   * @param index  the index to check
   * @param message  the {@link String#format(String, Object...)} exception message if invalid, not null
   * @param values  the optional values for the formatted exception message, null array not recommended
   * @return the validated character sequence (never {@code null} for method chaining)
   * @throws NullPointerException if the character sequence is {@code null}
   * @throws IndexOutOfBoundsException if the index is invalid
   *
   * @see #validIndex(Logger, CharSequence, int)
   */
  public static <T extends CharSequence> T validIndex(Logger logger, T chars, int index, String message, Object... values) {
    try {
      return org.apache.commons.lang3.Validate.validIndex(chars, index, message, values);
    } catch (RuntimeException e) {
      throw logger.throwing(e);
    }
  }
  /**
   * <p>Validates that the index is within the bounds of the argument
   * character sequence; otherwise throwing an exception with the
   * specified message.</p>
   *
   * <pre>Validate.validIndex(myStr, 2, "The string index is invalid: ");</pre>
   *
   * <p>If the character sequence is {@code null}, then the message
   * of the exception is &quot;The validated object is null&quot;.</p>
   *
   * @param <T> the character sequence type
   * @param logger  the logger via which to log the exception
   * @param level  the level at which to log the exception
   * @param chars  the character sequence to check, validated not null by this method
   * @param index  the index to check
   * @param message  the {@link String#format(String, Object...)} exception message if invalid, not null
   * @param values  the optional values for the formatted exception message, null array not recommended
   * @return the validated character sequence (never {@code null} for method chaining)
   * @throws NullPointerException if the character sequence is {@code null}
   * @throws IndexOutOfBoundsException if the index is invalid
   *
   * @see #validIndex(Logger, CharSequence, int)
   */
  public static <T extends CharSequence> T validIndex(Logger logger, Level level, T chars, int index, String message, Object... values) {
    try {
      return org.apache.commons.lang3.Validate.validIndex(chars, index, message, values);
    } catch (RuntimeException e) {
      throw logger.throwing(level, e);
    }
  }

  /**
   * <p>Validates that the index is within the bounds of the argument
   * character sequence; otherwise throwing an exception.</p>
   *
   * <pre>Validate.validIndex(myStr, 2);</pre>
   *
   * <p>If the character sequence is {@code null}, then the message
   * of the exception is &quot;The validated object is
   * null&quot;.</p>
   *
   * <p>If the index is invalid, then the message of the exception
   * is &quot;The validated character sequence index is invalid: &quot;
   * followed by the index.</p>
   *
   * @param <T> the character sequence type
   * @param logger  the logger via which to log the exception
   * @param chars  the character sequence to check, validated not null by this method
   * @param index  the index to check
   * @return the validated character sequence (never {@code null} for method chaining)
   * @throws NullPointerException if the character sequence is {@code null}
   * @throws IndexOutOfBoundsException if the index is invalid
   *
   * @see #validIndex(Logger, CharSequence, int, String, Object...)
   */
  public static <T extends CharSequence> T validIndex(Logger logger, T chars, int index) {
    try {
      return org.apache.commons.lang3.Validate.validIndex(chars, index);
    } catch (RuntimeException e) {
      throw logger.throwing(e);
    }
  }
  /**
   * <p>Validates that the index is within the bounds of the argument
   * character sequence; otherwise throwing an exception.</p>
   *
   * <pre>Validate.validIndex(myStr, 2);</pre>
   *
   * <p>If the character sequence is {@code null}, then the message
   * of the exception is &quot;The validated object is
   * null&quot;.</p>
   *
   * <p>If the index is invalid, then the message of the exception
   * is &quot;The validated character sequence index is invalid: &quot;
   * followed by the index.</p>
   *
   * @param <T> the character sequence type
   * @param logger  the logger via which to log the exception
   * @param level  the level at which to log the exception
   * @param chars  the character sequence to check, validated not null by this method
   * @param index  the index to check
   * @return the validated character sequence (never {@code null} for method chaining)
   * @throws NullPointerException if the character sequence is {@code null}
   * @throws IndexOutOfBoundsException if the index is invalid
   *
   * @see #validIndex(Logger, CharSequence, int, String, Object...)
   */
  public static <T extends CharSequence> T validIndex(Logger logger, Level level, T chars, int index) {
    try {
      return org.apache.commons.lang3.Validate.validIndex(chars, index);
    } catch (RuntimeException e) {
      throw logger.throwing(level, e);
    }
  }

  /**
   * <p>Validate that the stateful condition is {@code true}; otherwise
   * throwing an exception. This method is useful when validating according
   * to an arbitrary boolean expression, such as validating a
   * primitive number or using your own custom validation expression.</p>
   *
   * <pre>
   * Validate.validState(field &gt; 0);
   * Validate.validState(this.isOk());</pre>
   *
   * <p>The message of the exception is &quot;The validated state is
   * false&quot;.</p>
   *
   * @param logger  the logger via which to log the exception
   * @param expression  the boolean expression to check
   * @throws IllegalStateException if expression is {@code false}
   *
   * @see #validState(Logger, boolean, String, Object...)
   */
  public static void validState(Logger logger, boolean expression) {
    try {
      org.apache.commons.lang3.Validate.validState(expression);
    } catch (RuntimeException e) {
      throw logger.throwing(e);
    }
  }
  /**
   * <p>Validate that the stateful condition is {@code true}; otherwise
   * throwing an exception. This method is useful when validating according
   * to an arbitrary boolean expression, such as validating a
   * primitive number or using your own custom validation expression.</p>
   *
   * <pre>
   * Validate.validState(field &gt; 0);
   * Validate.validState(this.isOk());</pre>
   *
   * <p>The message of the exception is &quot;The validated state is
   * false&quot;.</p>
   *
   * @param logger  the logger via which to log the exception
   * @param level  the level at which to log the exception
   * @param expression  the boolean expression to check
   * @throws IllegalStateException if expression is {@code false}
   *
   * @see #validState(Logger, boolean, String, Object...)
   */
  public static void validState(Logger logger, Level level, boolean expression) {
    try {
      org.apache.commons.lang3.Validate.validState(expression);
    } catch (RuntimeException e) {
      throw logger.throwing(level, e);
    }
  }

  /**
   * <p>Validate that the stateful condition is {@code true}; otherwise
   * throwing an exception with the specified message. This method is useful when
   * validating according to an arbitrary boolean expression, such as validating a
   * primitive number or using your own custom validation expression.</p>
   *
   * <pre>Validate.validState(this.isOk(), "The state is not OK: %s", myObject);</pre>
   *
   * @param logger  the logger via which to log the exception
   * @param expression  the boolean expression to check
   * @param message  the {@link String#format(String, Object...)} exception message if invalid, not null
   * @param values  the optional values for the formatted exception message, null array not recommended
   * @throws IllegalStateException if expression is {@code false}
   *
   * @see #validState(Logger, boolean)
   */
  public static void validState(Logger logger, boolean expression, String message, Object... values) {
    try {
      org.apache.commons.lang3.Validate.validState(expression, message, values);
    } catch (RuntimeException e) {
      throw logger.throwing(e);
    }
  }
  /**
   * <p>Validate that the stateful condition is {@code true}; otherwise
   * throwing an exception with the specified message. This method is useful when
   * validating according to an arbitrary boolean expression, such as validating a
   * primitive number or using your own custom validation expression.</p>
   *
   * <pre>Validate.validState(this.isOk(), "The state is not OK: %s", myObject);</pre>
   *
   * @param logger  the logger via which to log the exception
   * @param expression  the boolean expression to check
   * @param level  the level at which to log the exception
   * @param message  the {@link String#format(String, Object...)} exception message if invalid, not null
   * @param values  the optional values for the formatted exception message, null array not recommended
   * @throws IllegalStateException if expression is {@code false}
   *
   * @see #validState(Logger, boolean)
   */
  public static void validState(Logger logger, Level level, boolean expression, String message, Object... values) {
    try {
      org.apache.commons.lang3.Validate.validState(expression, message, values);
    } catch (RuntimeException e) {
      throw logger.throwing(level, e);
    }
  }

  /**
   * <p>Validate that the specified argument character sequence matches the specified regular
   * expression pattern; otherwise throwing an exception.</p>
   *
   * <pre>Validate.matchesPattern("hi", "[a-z]*");</pre>
   *
   * <p>The syntax of the pattern is the one used in the {@link Pattern} class.</p>
   *
   * @param logger  the logger via which to log the exception
   * @param input  the character sequence to validate, not null
   * @param pattern  the regular expression pattern, not null
   * @throws IllegalArgumentException if the character sequence does not match the pattern
   *
   * @see #matchesPattern(Logger, CharSequence, String, String, Object...)
   */
  public static void matchesPattern(Logger logger, CharSequence input, String pattern) {
    try {
      org.apache.commons.lang3.Validate.matchesPattern(input, pattern);
    } catch (RuntimeException e) {
      throw logger.throwing(e);
    }
  }
  /**
   * <p>Validate that the specified argument character sequence matches the specified regular
   * expression pattern; otherwise throwing an exception.</p>
   *
   * <pre>Validate.matchesPattern("hi", "[a-z]*");</pre>
   *
   * <p>The syntax of the pattern is the one used in the {@link Pattern} class.</p>
   *
   * @param logger  the logger via which to log the exception
   * @param level  the level at which to log the exception
   * @param input  the character sequence to validate, not null
   * @param pattern  the regular expression pattern, not null
   * @throws IllegalArgumentException if the character sequence does not match the pattern
   *
   * @see #matchesPattern(Logger, CharSequence, String, String, Object...)
   */
  public static void matchesPattern(Logger logger, Level level, CharSequence input, String pattern) {
    try {
      org.apache.commons.lang3.Validate.matchesPattern(input, pattern);
    } catch (RuntimeException e) {
      throw logger.throwing(level, e);
    }
  }

  /**
   * <p>Validate that the specified argument character sequence matches the specified regular
   * expression pattern; otherwise throwing an exception with the specified message.</p>
   *
   * <pre>Validate.matchesPattern("hi", "[a-z]*", "%s does not match %s", "hi" "[a-z]*");</pre>
   *
   * <p>The syntax of the pattern is the one used in the {@link Pattern} class.</p>
   *
   * @param logger  the logger via which to log the exception
   * @param input  the character sequence to validate, not null
   * @param pattern  the regular expression pattern, not null
   * @param message  the {@link String#format(String, Object...)} exception message if invalid, not null
   * @param values  the optional values for the formatted exception message, null array not recommended
   * @throws IllegalArgumentException if the character sequence does not match the pattern
   *
   * @see #matchesPattern(Logger, CharSequence, String)
   */
  public static void matchesPattern(Logger logger, CharSequence input, String pattern, String message, Object... values) {
    try {
      org.apache.commons.lang3.Validate.matchesPattern(input, pattern, message, values);
    } catch (RuntimeException e) {
      throw logger.throwing(e);
    }
  }
  /**
   * <p>Validate that the specified argument character sequence matches the specified regular
   * expression pattern; otherwise throwing an exception with the specified message.</p>
   *
   * <pre>Validate.matchesPattern("hi", "[a-z]*", "%s does not match %s", "hi" "[a-z]*");</pre>
   *
   * <p>The syntax of the pattern is the one used in the {@link Pattern} class.</p>
   *
   * @param logger  the logger via which to log the exception
   * @param level  the level at which to log the exception
   * @param input  the character sequence to validate, not null
   * @param pattern  the regular expression pattern, not null
   * @param message  the {@link String#format(String, Object...)} exception message if invalid, not null
   * @param values  the optional values for the formatted exception message, null array not recommended
   * @throws IllegalArgumentException if the character sequence does not match the pattern
   *
   * @see #matchesPattern(Logger, CharSequence, String)
   */
  public static void matchesPattern(Logger logger, Level level, CharSequence input, String pattern, String message, Object... values) {
    try {
      org.apache.commons.lang3.Validate.matchesPattern(input, pattern, message, values);
    } catch (RuntimeException e) {
      throw logger.throwing(level, e);
    }
  }

  /**
   * <p>Validate that the specified argument object fall between the two
   * inclusive values specified; otherwise, throws an exception.</p>
   *
   * <pre>Validate.inclusiveBetween(0, 2, 1);</pre>
   *
   * @param <T> the type of the argument object
   * @param logger  the logger via which to log the exception
   * @param start  the inclusive start value, not null
   * @param end  the inclusive end value, not null
   * @param value  the object to validate, not null
   * @throws IllegalArgumentException if the value falls out of the boundaries
   *
   * @see #inclusiveBetween(Logger, Object, Object, Comparable, String, Object...)
   */
  public static <T> void inclusiveBetween(Logger logger, T start, T end, Comparable<T> value) {
    try {
      org.apache.commons.lang3.Validate.inclusiveBetween(start, end, value);
    } catch (RuntimeException e) {
      throw logger.throwing(e);
    }
  }
  /**
   * <p>Validate that the specified argument object fall between the two
   * inclusive values specified; otherwise, throws an exception.</p>
   *
   * <pre>Validate.inclusiveBetween(0, 2, 1);</pre>
   *
   * @param <T> the type of the argument object
   * @param logger  the logger via which to log the exception
   * @param level  the level at which to log the exception
   * @param start  the inclusive start value, not null
   * @param end  the inclusive end value, not null
   * @param value  the object to validate, not null
   * @throws IllegalArgumentException if the value falls out of the boundaries
   *
   * @see #inclusiveBetween(Logger, Object, Object, Comparable, String, Object...)
   */
  public static <T> void inclusiveBetween(Logger logger, Level level, T start, T end, Comparable<T> value) {
    try {
      org.apache.commons.lang3.Validate.inclusiveBetween(start, end, value);
    } catch (RuntimeException e) {
      throw logger.throwing(level, e);
    }
  }

  /**
   * <p>Validate that the specified argument object fall between the two
   * inclusive values specified; otherwise, throws an exception with the
   * specified message.</p>
   *
   * <pre>Validate.inclusiveBetween(0, 2, 1, "Not in boundaries");</pre>
   *
   * @param <T> the type of the argument object
   * @param logger  the logger via which to log the exception
   * @param start  the inclusive start value, not null
   * @param end  the inclusive end value, not null
   * @param value  the object to validate, not null
   * @param message  the {@link String#format(String, Object...)} exception message if invalid, not null
   * @param values  the optional values for the formatted exception message, null array not recommended
   * @throws IllegalArgumentException if the value falls out of the boundaries
   *
   * @see #inclusiveBetween(Logger, Object, Object, Comparable)
   */
  public static <T> void inclusiveBetween(Logger logger, T start, T end, Comparable<T> value, String message, Object... values) {
    try {
      org.apache.commons.lang3.Validate.inclusiveBetween(start, end, value, message, values);
    } catch (RuntimeException e) {
      throw logger.throwing(e);
    }
  }
  /**
   * <p>Validate that the specified argument object fall between the two
   * inclusive values specified; otherwise, throws an exception with the
   * specified message.</p>
   *
   * <pre>Validate.inclusiveBetween(0, 2, 1, "Not in boundaries");</pre>
   *
   * @param <T> the type of the argument object
   * @param logger  the logger via which to log the exception
   * @param level  the level at which to log the exception
   * @param start  the inclusive start value, not null
   * @param end  the inclusive end value, not null
   * @param value  the object to validate, not null
   * @param message  the {@link String#format(String, Object...)} exception message if invalid, not null
   * @param values  the optional values for the formatted exception message, null array not recommended
   * @throws IllegalArgumentException if the value falls out of the boundaries
   *
   * @see #inclusiveBetween(Logger, Object, Object, Comparable)
   */
  public static <T> void inclusiveBetween(Logger logger, Level level, T start, T end, Comparable<T> value, String message, Object... values) {
    try {
      org.apache.commons.lang3.Validate.inclusiveBetween(start, end, value, message, values);
    } catch (RuntimeException e) {
      throw logger.throwing(level, e);
    }
  }

  /**
   * <p>Validate that the specified argument object fall between the two
   * exclusive values specified; otherwise, throws an exception.</p>
   *
   * <pre>Validate.inclusiveBetween(0, 2, 1);</pre>
   *
   * @param <T> the type of the argument object
   * @param logger  the logger via which to log the exception
   * @param start  the exclusive start value, not null
   * @param end  the exclusive end value, not null
   * @param value  the object to validate, not null
   * @throws IllegalArgumentException if the value falls out of the boundaries
   *
   * @see #exclusiveBetween(Logger, Object, Object, Comparable, String, Object...)
   */
  public static <T> void exclusiveBetween(Logger logger, T start, T end, Comparable<T> value) {
    try {
      org.apache.commons.lang3.Validate.exclusiveBetween(start, end, value);
    } catch (RuntimeException e) {
      throw logger.throwing(e);
    }
  }
  /**
   * <p>Validate that the specified argument object fall between the two
   * exclusive values specified; otherwise, throws an exception.</p>
   *
   * <pre>Validate.inclusiveBetween(0, 2, 1);</pre>
   *
   * @param <T> the type of the argument object
   * @param logger  the logger via which to log the exception
   * @param level  the level at which to log the exception
   * @param start  the exclusive start value, not null
   * @param end  the exclusive end value, not null
   * @param value  the object to validate, not null
   * @throws IllegalArgumentException if the value falls out of the boundaries
   *
   * @see #exclusiveBetween(Logger, Object, Object, Comparable, String, Object...)
   */
  public static <T> void exclusiveBetween(Logger logger, Level level, T start, T end, Comparable<T> value) {
    try {
      org.apache.commons.lang3.Validate.exclusiveBetween(start, end, value);
    } catch (RuntimeException e) {
      throw logger.throwing(level, e);
    }
  }

  /**
   * <p>Validate that the specified argument object fall between the two
   * exclusive values specified; otherwise, throws an exception with the
   * specified message.</p>
   *
   * <pre>Validate.inclusiveBetween(0, 2, 1, "Not in boundaries");</pre>
   *
   * @param <T> the type of the argument object
   * @param logger  the logger via which to log the exception
   * @param start  the exclusive start value, not null
   * @param end  the exclusive end value, not null
   * @param value  the object to validate, not null
   * @param message  the {@link String#format(String, Object...)} exception message if invalid, not null
   * @param values  the optional values for the formatted exception message, null array not recommended
   * @throws IllegalArgumentException if the value falls out of the boundaries
   *
   * @see #exclusiveBetween(Logger, Object, Object, Comparable)
   */
  public static <T> void exclusiveBetween(Logger logger, T start, T end, Comparable<T> value, String message, Object... values) {
    try {
      org.apache.commons.lang3.Validate.exclusiveBetween(start, end, value, message, values);
    } catch (RuntimeException e) {
      throw logger.throwing(e);
    }
  }
  /**
   * <p>Validate that the specified argument object fall between the two
   * exclusive values specified; otherwise, throws an exception with the
   * specified message.</p>
   *
   * <pre>Validate.inclusiveBetween(0, 2, 1, "Not in boundaries");</pre>
   *
   * @param <T> the type of the argument object
   * @param logger  the logger via which to log the exception
   * @param level  the level at which to log the exception
   * @param start  the exclusive start value, not null
   * @param end  the exclusive end value, not null
   * @param value  the object to validate, not null
   * @param message  the {@link String#format(String, Object...)} exception message if invalid, not null
   * @param values  the optional values for the formatted exception message, null array not recommended
   * @throws IllegalArgumentException if the value falls out of the boundaries
   *
   * @see #exclusiveBetween(Logger, Object, Object, Comparable)
   */
  public static <T> void exclusiveBetween(Logger logger, Level level, T start, T end, Comparable<T> value, String message, Object... values) {
    try {
      org.apache.commons.lang3.Validate.exclusiveBetween(start, end, value, message, values);
    } catch (RuntimeException e) {
      throw logger.throwing(level, e);
    }
  }

  /**
   * Validates that the argument is an instance of the specified class, if not throws an exception.
   *
   * <p>This method is useful when validating according to an arbitrary class</p>
   *
   * <pre>Validate.isInstanceOf(OkClass.class, object);</pre>
   *
   * <p>The message of the exception is &quot;Expected type: {type}, actual: {obj_type}&quot;</p>
   *
   * @param logger  the logger via which to log the exception
   * @param type  the class the object must be validated against, not null
   * @param obj  the object to check, null throws an exception
   * @throws IllegalArgumentException if argument is not of specified class
   *
   * @see #isInstanceOf(Logger, Class, Object, String, Object...)
   */
  public static void isInstanceOf(Logger logger, Class<?> type, Object obj) {
    try {
      org.apache.commons.lang3.Validate.isInstanceOf(type, obj);
    } catch (RuntimeException e) {
      throw logger.throwing(e);
    }
  }
  /**
   * Validates that the argument is an instance of the specified class, if not throws an exception.
   *
   * <p>This method is useful when validating according to an arbitrary class</p>
   *
   * <pre>Validate.isInstanceOf(OkClass.class, object);</pre>
   *
   * <p>The message of the exception is &quot;Expected type: {type}, actual: {obj_type}&quot;</p>
   *
   * @param logger  the logger via which to log the exception
   * @param level  the level at which to log the exception
   * @param type  the class the object must be validated against, not null
   * @param obj  the object to check, null throws an exception
   * @throws IllegalArgumentException if argument is not of specified class
   *
   * @see #isInstanceOf(Logger, Class, Object, String, Object...)
   */
  public static void isInstanceOf(Logger logger, Level level, Class<?> type, Object obj) {
    try {
      org.apache.commons.lang3.Validate.isInstanceOf(type, obj);
    } catch (RuntimeException e) {
      throw logger.throwing(level, e);
    }
  }

  /**
   * <p>Validate that the argument is an instance of the specified class; otherwise
   * throwing an exception with the specified message. This method is useful when
   * validating according to an arbitrary class</p>
   *
   * <pre>Validate.isInstanceOf(OkClass.classs, object, "Wrong class, object is of class %s",
   *   object.getClass().getName());</pre>
   *
   * @param logger  the logger via which to log the exception
   * @param type  the class the object must be validated against, not null
   * @param obj  the object to check, null throws an exception
   * @param message  the {@link String#format(String, Object...)} exception message if invalid, not null
   * @param values  the optional values for the formatted exception message, null array not recommended
   * @throws IllegalArgumentException if argument is not of specified class
   *
   * @see #isInstanceOf(Logger, Class, Object)
   */
  public static void isInstanceOf(Logger logger, Class<?> type, Object obj, String message, Object... values) {
    try {
      org.apache.commons.lang3.Validate.isInstanceOf(type, obj, message, values);
    } catch (RuntimeException e) {
      throw logger.throwing(e);
    }
  }
  /**
   * <p>Validate that the argument is an instance of the specified class; otherwise
   * throwing an exception with the specified message. This method is useful when
   * validating according to an arbitrary class</p>
   *
   * <pre>Validate.isInstanceOf(OkClass.classs, object, "Wrong class, object is of class %s",
   *   object.getClass().getName());</pre>
   *
   * @param logger  the logger via which to log the exception
   * @param level  the level at which to log the exception
   * @param type  the class the object must be validated against, not null
   * @param obj  the object to check, null throws an exception
   * @param message  the {@link String#format(String, Object...)} exception message if invalid, not null
   * @param values  the optional values for the formatted exception message, null array not recommended
   * @throws IllegalArgumentException if argument is not of specified class
   *
   * @see #isInstanceOf(Logger, Class, Object)
   */
  public static void isInstanceOf(Logger logger, Level level, Class<?> type, Object obj, String message, Object... values) {
    try {
      org.apache.commons.lang3.Validate.isInstanceOf(type, obj, message, values);
    } catch (RuntimeException e) {
      throw logger.throwing(level, e);
    }
  }

  /**
   * Validates that the argument can be converted to the specified class, if not, throws an exception.
   *
   * <p>This method is useful when validating that there will be no casting errors.</p>
   *
   * <pre>Validate.isAssignableFrom(SuperClass.class, object.getClass());</pre>
   *
   * <p>The message format of the exception is &quot;Cannot assign {type} to {superType}&quot;</p>
   *
   * @param logger  the logger via which to log the exception
   * @param superType  the class the class must be validated against, not null
   * @param type  the class to check, not null
   * @throws IllegalArgumentException if type argument is not assignable to the specified superType
   *
   * @see #isAssignableFrom(Logger, Class, Class, String, Object...)
   */
  public static void isAssignableFrom(Logger logger, Class<?> superType, Class<?> type) {
    try {
      org.apache.commons.lang3.Validate.isAssignableFrom(superType, type);
    } catch (RuntimeException e) {
      throw logger.throwing(e);
    }
  }
  /**
   * Validates that the argument can be converted to the specified class, if not, throws an exception.
   *
   * <p>This method is useful when validating that there will be no casting errors.</p>
   *
   * <pre>Validate.isAssignableFrom(SuperClass.class, object.getClass());</pre>
   *
   * <p>The message format of the exception is &quot;Cannot assign {type} to {superType}&quot;</p>
   *
   * @param logger  the logger via which to log the exception
   * @param level  the level at which to log the exception
   * @param superType  the class the class must be validated against, not null
   * @param type  the class to check, not null
   * @throws IllegalArgumentException if type argument is not assignable to the specified superType
   *
   * @see #isAssignableFrom(Logger, Class, Class, String, Object...)
   */
  public static void isAssignableFrom(Logger logger, Level level, Class<?> superType, Class<?> type) {
    try {
      org.apache.commons.lang3.Validate.isAssignableFrom(superType, type);
    } catch (RuntimeException e) {
      throw logger.throwing(level, e);
    }
  }

  /**
   * Validates that the argument can be converted to the specified class, if not throws an exception.
   *
   * <p>This method is useful when validating if there will be no casting errors.</p>
   *
   * <pre>Validate.isAssignableFrom(SuperClass.class, object.getClass());</pre>
   *
   * <p>The message of the exception is &quot;The validated object can not be converted to the&quot;
   * followed by the name of the class and &quot;class&quot;</p>
   *
   * @param logger  the logger via which to log the exception
   * @param superType  the class the class must be validated against, not null
   * @param type  the class to check, not null
   * @param message  the {@link String#format(String, Object...)} exception message if invalid, not null
   * @param values  the optional values for the formatted exception message, null array not recommended
   * @throws IllegalArgumentException if argument can not be converted to the specified class
   *
   * @see #isAssignableFrom(Logger, Class, Class)
   */
  public static void isAssignableFrom(Logger logger, Class<?> superType, Class<?> type, String message, Object... values) {
    try {
      org.apache.commons.lang3.Validate.isAssignableFrom(superType, type, message, values);
    } catch (RuntimeException e) {
      throw logger.throwing(e);
    }
  }
  /**
   * Validates that the argument can be converted to the specified class, if not throws an exception.
   *
   * <p>This method is useful when validating if there will be no casting errors.</p>
   *
   * <pre>Validate.isAssignableFrom(SuperClass.class, object.getClass());</pre>
   *
   * <p>The message of the exception is &quot;The validated object can not be converted to the&quot;
   * followed by the name of the class and &quot;class&quot;</p>
   *
   * @param logger  the logger via which to log the exception
   * @param level  the level at which to log the exception
   * @param superType  the class the class must be validated against, not null
   * @param type  the class to check, not null
   * @param message  the {@link String#format(String, Object...)} exception message if invalid, not null
   * @param values  the optional values for the formatted exception message, null array not recommended
   * @throws IllegalArgumentException if argument can not be converted to the specified class
   *
   * @see #isAssignableFrom(Logger, Class, Class)
   */
  public static void isAssignableFrom(Logger logger, Level level, Class<?> superType, Class<?> type, String message, Object... values) {
    try {
      org.apache.commons.lang3.Validate.isAssignableFrom(superType, type, message, values);
    } catch (RuntimeException e) {
      throw logger.throwing(level, e);
    }
  }
}

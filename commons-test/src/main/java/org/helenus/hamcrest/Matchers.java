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
package org.helenus.hamcrest;

import java.util.Objects;
import java.util.function.Function;

import mockit.Deencapsulation;

import org.hamcrest.Description;
import org.hamcrest.DiagnosingMatcher;
import org.hamcrest.Matcher;

/**
 * The <code>Matchers</code> class provides a factory for matchers.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jul 2, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
public class Matchers {
  /**
   * Applies a function to the matched object in order to retrieve another
   * and starts matching this new object with the specified matcher.
   *
   * @author paouelle
   *
   * @param  function the function to apply to the object currently being matched
   *         in order to retrieve another
   * @param  describer a describer string for the function
   * @param  matcher the matcher to use for the retrieved object from the function
   * @return the corresponding matcher
   */
  public static <T, S> Matcher<T> that(
    Function<T, S> function, String describer, Matcher<S> matcher
  ) {
    return new DiagnosingMatcher<T>() {
      @SuppressWarnings("unchecked")
      @Override
      public boolean matches(java.lang.Object obj, Description mismatch) {
        final S s = function.apply((T)obj);

        if (!matcher.matches(s)) {
          matcher.describeMismatch(s, mismatch);
          return false;
        }
        return true;
      }
      @Override
      public void describeTo(Description description) {
        description
          .appendText("'")
          .appendText(describer)
          .appendText("' ")
          .appendDescriptionOf(matcher);
      }
    };
  }

  /**
   * Applies a function to the matched object and the provided one in order to
   * retrieve values to check if they are the same as determined by calling
   * the {@link java.util.Objects#deepEquals} method.
   *
   * @author paouelle
   *
   * @param  function the function to apply to the object currently being matched
   *         and to the specified operand in order to retrieve values to be compared
   * @param  describer a describer string for the function
   * @param  operand the object to retrieve a value using the specified function
   *         to compare with
   * @return the corresponding matcher
   */
  public static <T, S> Matcher<T> equalTo(
    Function<T, S> function, String describer, T operand
  ) {
    final S os = function.apply(operand);

    return new DiagnosingMatcher<T>() {
      @SuppressWarnings("unchecked")
      @Override
      public boolean matches(java.lang.Object obj, Description mismatch) {
        final S s = function.apply((T)obj);

        if (!Objects.deepEquals(os, s)) {
          mismatch
            .appendText("however '")
            .appendText(describer)
            .appendText("' was ")
            .appendValue(s)
            .appendText(" instead of ")
            .appendValue(os);
          return false;
        }
        return true;
      }
      @Override
      public void describeTo(Description description) {
        description
          .appendText("'")
          .appendText(describer)
          .appendText("' is equal to ")
          .appendValue(os);
      }
    };
  }

  /**
   * Applies a function to the matched object and the provided one in order to
   * retrieve values to check if they are the same with a tolerable error as
   * determined by calling the {@link org.helenus.util.Objects#deepEquals} method.
   *
   * @author paouelle
   *
   * @param  function the function to apply to the object currently being matched
   *         and to the specified operand in order to retrieve values to be compared
   * @param  describer a describer string for the function
   * @param  operand the object to retrieve a value using the specified function
   *         to compare with
   * @param  epsilon the tolerable error
   * @return the corresponding matcher
   */
  public static <T, S> Matcher<T> closeTo(
    Function<T, S> function, String describer, T operand, double epsilon
  ) {
    final S os = function.apply(operand);

    return new DiagnosingMatcher<T>() {
      @SuppressWarnings("unchecked")
      @Override
      public boolean matches(java.lang.Object obj, Description mismatch) {
        final S s = function.apply((T)obj);

        if (!org.helenus.util.Objects.deepEquals(os, s, epsilon)) {
          mismatch
            .appendText("however '")
            .appendText(describer)
            .appendText("' was ")
            .appendValue(s)
            .appendText(" instead of ")
            .appendValue(os);
          return false;
        }
        return true;
      }
      @Override
      public void describeTo(Description description) {
        description
          .appendText("'")
          .appendText(describer)
          .appendText("' is close within ")
          .appendText(String.valueOf(epsilon))
          .appendText(" to ")
          .appendValue(os);
      }
    };
  }

  /**
   * Retrieves a field from the matched object and starts matching this new
   * object with the specified matcher.
   *
   * @author paouelle
   *
   * @param  field the name of the field to retrieve from the object currently
   *         being matched
   * @param  matcher the matcher to use for the retrieved object from the field
   * @return the corresponding matcher
   */
  public static <T, S> Matcher<T> fieldEqualTo(String field, Matcher<S> matcher) {
    return new DiagnosingMatcher<T>() {
      @SuppressWarnings("unchecked")
      @Override
      public boolean matches(java.lang.Object obj, Description mismatch) {
        final S s = Deencapsulation.getField(obj,  field);

        if (!matcher.matches(s)) {
          matcher.describeMismatch(s, mismatch);
          return false;
        }
        return true;
      }
      @Override
      public void describeTo(Description description) {
        description
          .appendText("field '")
          .appendText(field)
          .appendText("' ")
          .appendDescriptionOf(matcher);
      }
    };
  }
}

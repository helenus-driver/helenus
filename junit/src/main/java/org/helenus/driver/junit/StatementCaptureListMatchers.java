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
package org.helenus.driver.junit;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;
import org.helenus.driver.GenericStatement;

/**
 * The <code>StatementCaptureListMatchers</code> abstract class defines static
 * factory method for a statement capture list.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jun 30, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
public abstract class StatementCaptureListMatchers {
  /**
   * Asserts that the given matcher matches the actual value.
   *
   * @author paouelle
   *
   * @param <T> the type accepted by the matcher
   *
   * @param  actual the value to match against
   * @param  matcher the matcher
   * @throws AssertionError if the matcher doesn't match the actual value
   */
  static <T> void assertThat(T actual, Matcher<T> matcher) {
    StatementCaptureListMatchers.assertThat("", actual, matcher);
  }

  /**
   * Asserts that the given matcher matches the actual value.
   *
   * @author paouelle
   *
   * @param <T> the type accepted by the matcher
   *
   * @param  reason the additional information about the error
   * @param  actual the value to match against
   * @param  matcher the matcher
   * @throws AssertionError if the matcher doesn't match the actual value
   */
  static <T> void assertThat(String reason, T actual, Matcher<T> matcher) {
    if (!matcher.matches(actual)) {
      final Description description = new StringDescription();

      if (!reason.isEmpty()) {
        description.appendText(reason).appendText("\n");
      }
      description
        .appendText("Expected: ")
        .appendDescriptionOf(matcher)
        .appendText("\n     but: ");
      matcher.describeMismatch(actual, description);
      throw new AssertionError(description.toString());
    }
  }

  /**
   * Accesses the size assertions with a Hamcrest match for asserting the
   * the number of captured statements for a statement capture list.
   *
   * @author paouelle
   *
   * @param <T> the type of statements captured
   *
   * @param  matcher a matcher for the size expected for the statement capture
   *         list
   * @return the corresponding statement matcher
   */
  @SuppressWarnings("rawtypes")
  public static <T extends GenericStatement> StatementCaptureListMatcher<T> size(
    Matcher<Integer> matcher
  ) {
    return new StatementCaptureListMatcher<T>() {
      @Override
      public void match(StatementCaptureList<T> list) throws Exception {
        StatementCaptureListMatchers.assertThat(
          "Size", list.size(), matcher
        );
      }
    };
  }

  /**
   * Creates a matcher that matches if the captured list is empty.
   *
   * @param <T> the type of statements captured
   *
   * @return a corresponding matcher
   */
  @SuppressWarnings("rawtypes")
  public static <T extends GenericStatement> Matcher<StatementCaptureList<T>> isEmpty() {
    return new BaseMatcher<StatementCaptureList<T>>() {
      @SuppressWarnings("unchecked")
      @Override
      public boolean matches(java.lang.Object obj) {
         return ((StatementCaptureList<T>)obj).isEmpty();
      }
      @Override
      public void describeMismatch(java.lang.Object obj, Description description) {
        description.appendText("not empty");
      }
      @Override
      public void describeTo(Description description) {
        description.appendValue("is empty");
      }
    };
  }

  /**
   * Creates a matcher that matches if the captured list is not empty.
   *
   * @param <T> the type of statements captured
   *
   * @return a corresponding matcher
   */
  @SuppressWarnings("rawtypes")
  public static <T extends GenericStatement> Matcher<StatementCaptureList<T>> isNotEmpty() {
    return new BaseMatcher<StatementCaptureList<T>>() {
      @SuppressWarnings("unchecked")
      @Override
      public boolean matches(java.lang.Object obj) {
         return !((StatementCaptureList<T>)obj).isEmpty();
      }
      @Override
      public void describeMismatch(java.lang.Object obj, Description description) {
        description.appendText("empty");
      }
      @Override
      public void describeTo(Description description) {
        description.appendValue("is not empty");
      }
    };
  }
}

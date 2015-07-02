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

import java.util.function.Function;

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
   * @param  matcher the matcher to use for the retrieved object from the function
   * @return the corresponding matcher
   */
  public static <T, S> Matcher<T> that(Function<T, S> function, Matcher<S> matcher) {
    return new DiagnosingMatcher<T>() {
      @SuppressWarnings("unchecked")
      @Override
      public boolean matches(java.lang.Object obj, Description mismatch) {
        if (!matcher.matches(function.apply((T)obj))) {
          mismatch.appendDescriptionOf(matcher).appendText(" ");
          matcher.describeMismatch(obj, mismatch);
          return false;
        }
        return true;
      }
      @Override
      public void describeTo(Description description) {
        matcher.describeTo(description);
      }
    };
  }
}

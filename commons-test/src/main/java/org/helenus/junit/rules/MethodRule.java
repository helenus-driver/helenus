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
package org.helenus.junit.rules;

import java.util.ArrayList;
import java.util.List;

import org.helenus.junit.Tag;
import org.helenus.junit.Tags;
import org.helenus.util.function.EConsumer;
import org.helenus.util.function.ERunnable;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;

/**
 * The <code>MethodRule</code> class provides a JUnit4 method rule which
 * enables additional useful functionalities for test cases.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jul 1, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
public class MethodRule implements org.junit.rules.MethodRule {
  /**
   * Holds a list of registered start handlers.
   *
   * @author paouelle
   */
  final EConsumer<MethodRule, Exception>[] shandlers;

  /**
   * Holds a list of registered error handlers.
   *
   * @author paouelle
   */
  final List<ERunnable<Exception>> ehandlers = new ArrayList<>(2);

  /**
   * Holds the test method currently running.
   *
   * @author paouelle
   */
  private volatile FrameworkMethod method = null;

  /**
   * Instantiates a new <code>MethodRule</code> object.
   *
   * @author paouelle
   *
   * @param handlers optional start handlers to invoke before the test case
   *        starts
   */
  @SafeVarargs
  public MethodRule(EConsumer<MethodRule, Exception>... handlers) {
    this.shandlers = handlers;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.junit.rules.MethodRule#apply(org.junit.runners.model.Statement, org.junit.runners.model.FrameworkMethod, java.lang.Object)
   */
  @Override
  public Statement apply(Statement base, FrameworkMethod method, Object target) {
    this.method = method;
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        try {
          if (shandlers != null) {
            for (final EConsumer<MethodRule, Exception> h: shandlers) {
              h.accept(MethodRule.this);
            }
          }
          base.evaluate();
        } catch (ThreadDeath|StackOverflowError|OutOfMemoryError e) {
          throw e;
        } catch (RuntimeException|Error e) {
          for (final ERunnable<Exception> h: ehandlers) {
            h.run();
          }
          throw e;
        } finally {
          ehandlers.clear();
        }
      }
    };
  }

  /**
   * Gets the name of the test method being executed.
   *
   * @author paouelle
   *
   * @return the name of the test method being executed
   */
  public String getName() {
    final FrameworkMethod m = method;

    return (m != null) ? m.getName() : null;
  }

  /**
   * Checks if the method is annotated with the specified tag.
   *
   * @author paouelle
   *
   * @param  tag the tag to check if the current test method is annotated with
   * @return <code>true</code> if the test method is annotated with the
   *         specified tag; <code>false</code> otherwise
   *
   * @see Tag
   */
  public boolean hasTag(String tag) {
    final FrameworkMethod m = method;

    if (m != null) {
      // first check for the repeatable container since framework method doesn't support
      // repeatable tags
      final Tags ts = method.getAnnotation(Tags.class);

      if (ts != null) {
        for (final Tag t: ts.value()) {
          if (t.value().equals(tag)) {
            return true;
          }
        }
      } else {
        final Tag t = method.getAnnotation(Tag.class);

        if (t != null) {
          return t.value().equals(tag);
        }
      }
    }
    return false;
  }

  /**
   * Register an error handler to be called on failure of the current test
   * case.
   *
   * @author paouelle
   *
   * @param  handler the error handler to be called on failure of the test case
   * @return this for chaining
   */
  public MethodRule onFailure(ERunnable<Exception> handler) {
    if (handler != null) {
      ehandlers.add(handler);
    }
    return this;
  }
}

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

import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;

import org.helenus.junit.Tag;
import org.helenus.junit.Tags;
import org.helenus.util.function.EConsumer;
import org.helenus.util.function.ERunnable;
import org.junit.Test;
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
   * Holds a list of registered error runnable handlers.
   *
   * @author paouelle
   */
  final List<ERunnable<Exception>> erhandlers = new ArrayList<>(2);

  /**
   * Holds a list of registered error consumer handlers.
   *
   * @author paouelle
   */
  final List<EConsumer<Throwable, Exception>> echandlers = new ArrayList<>(2);

  /**
   * Holds the test method currently running.
   *
   * @author paouelle
   */
  private volatile FrameworkMethod method = null;

  /**
   * Holds the Javassist reference to the test class.
   *
   * @author paouelle
   */
  private volatile CtClass ctclass = null;

  /**
   * Holds the Javassist reference to the test method.
   *
   * @author paouelle
   */
  private volatile CtMethod ctmethod = null;

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
    try {
      final ClassPool pool = ClassPool.getDefault();

      this.ctclass = pool.get(method.getDeclaringClass().getCanonicalName());
      this.ctmethod = ctclass.getDeclaredMethod(method.getName());
    } catch (Exception e) { // ignore and continue without javassist info
    }
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
        } catch (Throwable t) {
          // check if that error was expected because if it is ... that is not a failure
          final Test test = method.getAnnotation(Test.class);

          if ((test == null) || !test.expected().isAssignableFrom(t.getClass())) {
            for (final ERunnable<Exception> h: erhandlers) {
              h.run();
            }
            for (final EConsumer<Throwable, Exception> h: echandlers) {
              h.accept(t);
            }
          }
          throw t;
        } finally {
          erhandlers.clear();
          echandlers.clear();
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
   * Gets the source file for the test method if available.
   *
   * @author paouelle
   *
   * @return the source file where the test method is defined or <code>null</code>
   *         if not available
   */
  public String getSourceFile() {
    final CtClass c = ctclass;

    if (c != null) {
      return c.getClassFile2().getSourceFile();
    }
    return null;
  }

  /**
   * Gets the line number for the test method if available.
   *
   * @author paouelle
   *
   * @return the line number of the source line corresponding to the test method
   *         or <code>-1</code> if not available
   */
  public int getLineNumber() {
    final CtMethod m = ctmethod;

    if (m != null) {
      return m.getMethodInfo2().getLineNumber(0);
    }
    return -1;
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
      erhandlers.add(handler);
    }
    return this;
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
  public MethodRule onFailure(EConsumer<Throwable, Exception> handler) {
    if (handler != null) {
      echandlers.add(handler);
    }
    return this;
  }
}

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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.MutablePair;

import org.hamcrest.Matcher;
import org.helenus.driver.ObjectStatement;
import org.helenus.driver.impl.ParentStatementImpl;
import org.helenus.driver.impl.StatementImpl;

/**
 * The <code>EnumCaptureList</code> class defines a class capable of
 * capturing processed enums.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jun 30, 2015 - paouelle - Creation
 *
 * @param <T> the type of statements to capture
 *
 * @since 1.0
 */
public class StatementCaptureList<T extends ObjectStatement<?>> {
  /**
   * Holds the list of captured statements.
   *
   * @author paouelle
   */
  private final List<? extends ObjectStatement<T>> list = new ArrayList<>(8);

  /**
   * Holds the list of interceptors with their intercepting counter.
   *
   * @author paouelle
   */
  private final List<MutablePair<Consumer<? extends ObjectStatement<T>>, Integer>> interceptors
    = new ArrayList<>(4);

  /**
   * Holds the class of object statements to capture
   *
   * @author paouelle
   */
  private final Class<T> clazz;

  /**
   * Instantiates a new <code>EnumCaptureList</code> object.
   *
   * @author paouelle
   *
   * @param  clazz the class of object statements to capture
   */
  StatementCaptureList(Class<T> clazz) {
    this.clazz = clazz;
  }

  /**
   * Called when a statement is being executed.
   *
   * @author paouelle
   *
   * @param statement being processed
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  synchronized void executing(StatementImpl<?, ?, ?> statement) {
    final Stream<ObjectStatement<?>> s;

    // handle batch and sequences by capturing all internal statements
    if (statement instanceof ParentStatementImpl) {
      s = ((ParentStatementImpl)statement).objectStatements();
    } else if (statement instanceof ObjectStatement) {
      s = Stream.of((ObjectStatement<?>)(ObjectStatement)statement); // cast is required for cmdline compilation
    } else { // nothing to capture or intercept
      return;
    }
    s.forEachOrdered(os -> {
      if (clazz.isInstance(os)) {
        // first intercept
        for (final Iterator<MutablePair<Consumer<? extends ObjectStatement<T>>, Integer>> i = interceptors.iterator(); i.hasNext(); ) {
          final MutablePair<Consumer<? extends ObjectStatement<T>>, Integer> m = i.next();
          final int num = m.getRight().intValue() - 1;

          if (num <= 0) { // done with this interceptor; this was the last time
            i.remove();
          }
          ((Consumer<ObjectStatement<?>>)(Consumer)m.getLeft()).accept(os);
        }
        // then capture
        ((List<ObjectStatement<?>>)(List)list).add(os); // cast required to compile on cmdline
      }
    });
  }

  /**
   * Stops capturing and intercepting object statements with this capture list.
   *
   * @author paouelle
   *
   * @return this for chaining
   */
  public StatementCaptureList<T> stop() {
    synchronized (HelenusJUnit.class) {
      for (final Iterator<StatementCaptureList<? extends ObjectStatement<?>>> i = HelenusJUnit.captures.iterator(); i.hasNext(); ) {
        if (i.next() == this) {
          i.remove();
        }
      }
    }
    return this;
  }

  /**
   * Gets the number of statements captured.
   *
   * @author paouelle
   *
   * @return the number of statements captured
   */
  public int size() {
    return list.size();
  }

  /**
   * Checks if this list did not capture any statements.
   *
   * @author paouelle
   *
   * @return <code>true</code> if this list did not capture any statements;
   *         <code>false</code> otherwise
   */
  public boolean isEmpty() {
    return list.isEmpty();
  }

  /**
   * Registers a consumer to intercept all executing object statement. An
   * interceptor gets called just before the statement is submitted to Cassandra.
   * Upon returning control from the interceptor, the object statement will be
   * submitted to Cassandra unless an exception was thrown out
   *
   * @author paouelle
   *
   * @param  consumer the consumer to call in order to intercept the next statement
   * @return this for chaining
   * @throws NullPointerException if <code>consumer</code> is <code>null</code>
   */
  public StatementCaptureList<T> intercept(
    Consumer<? extends ObjectStatement<? extends T>> consumer
  ) {
    return intercept(Integer.MAX_VALUE, consumer);
  }

  /**
   * Registers a consumer to intercept the next <code>num</code> executing object
   * statement. An interceptor gets called just before the statement is submitted
   * to Cassandra. Upon returning control from the interceptor, the object
   * statement will be submitted to Cassandra unless an exception was thrown out
   *
   * @author paouelle
   *
   * @param  num the number of times this interceptor should remain active
   * @param  consumer the consumer to call in order to intercept the next statement
   * @return this for chaining
   * @throws NullPointerException if <code>consumer</code> is <code>null</code>
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public StatementCaptureList<T> intercept(
    int num, Consumer<? extends ObjectStatement<? extends T>> consumer
  ) {
    org.apache.commons.lang3.Validate.notNull(consumer, "invalid null consumer");
    if (num > 0) {
      ((List)interceptors).add(MutablePair.of(num, consumer));
    } // else - nothing to intercept
    return this;
  }

  /**
   * Stops all registered interceptors.
   *
   * @author paouelle
   *
   * @return this for chaining
   */
  public StatementCaptureList<T> stopIntercepting() {
    interceptors.clear();
    return this;
  }

  /**
   * Gets the ith captured statement if available.
   *
   * @author paouelle
   *
   * @param  i the captured statements (0-based)
   * @return the corresponding non-<code>null</code> statement
   * @throws AssertionError if less statements were captured
   */
  public ObjectStatement<T> on(int i) {
    if (list.size() <= i) {
      throw new AssertionError(
        "not enough captured object statements; only "
        + list.size()
        + " statements were captured and at least "
        + (i + 1)
        + " was expected"
      );
    }
    return list.get(0);
  }

  /**
   * Provides an expectation for the statement capture list.
   *
   * @author paouelle
   *
   * @param  matcher the statement capture list matcher to use
   * @return this for chaining
   * @throws Exception if an error occurs
   */
  public StatementCaptureList<T> andExpect(StatementCaptureListMatcher<T> matcher) throws Exception {
    matcher.match(this);
    return this;
  }

  /**
   * Provides an expectation for the statement capture list.
   *
   * @author paouelle
   *
   * @param  matcher the statement capture list matcher to use
   * @return this for chaining
   * @throws Exception if an error occurs
   */
  public StatementCaptureList<T> andExpect(
    Matcher<StatementCaptureList<T>> matcher
  ) throws Exception {
    matcher.matches(this);
    return this;
  }
}

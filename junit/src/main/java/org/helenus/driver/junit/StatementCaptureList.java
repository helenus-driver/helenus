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

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;

import org.hamcrest.Matcher;
import org.helenus.driver.Delete;
import org.helenus.driver.GenericStatement;
import org.helenus.driver.Insert;
import org.helenus.driver.Select;
import org.helenus.driver.Update;
import org.helenus.driver.impl.ParentStatementImpl;
import org.helenus.driver.impl.StatementImpl;

/**
 * The <code>StatementCaptureList</code> class defines a class capable of
 * capturing executing statements.
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
@SuppressWarnings("rawtypes")
public class StatementCaptureList<T extends GenericStatement> {
  /**
   * Gets a type string that corresponds to a given statement.
   *
   * @author paouelle
   *
   * @param  statement the statement to get a type string for
   * @return the correspond non-<code>null</code> type string
   */
  private static String getType(GenericStatement statement) {
    if (statement instanceof Update) {
      return "Update";
    } else if (statement instanceof Insert) {
      return "Insert";
    } else if (statement instanceof Delete) {
      return "Delete";
    } else if (statement instanceof Select) {
      return "Select";
    }
    return StringUtils.removeEnd(
      StringUtils.removeEnd(statement.getClass().getSimpleName(), "Impl"),
      "Statement"
    );
  }

  /**
   * Holds the information where this capture list was created.
   *
   * @author paouelle
   */
  private final StackTraceElement created;

  /**
   * Holds the list of captured statements.
   *
   * @author paouelle
   */
  @SuppressWarnings("rawtypes")
  private final List<? extends GenericStatement> list = new ArrayList<>(8);

  /**
   * Holds the list of interceptors with their intercepting counter.
   *
   * @author paouelle
   */
  @SuppressWarnings("rawtypes")
  private final List<MutablePair<Consumer<? extends GenericStatement>, Integer>> interceptors
    = new ArrayList<>(4);

  /**
   * Holds the class of statements to capture
   *
   * @author paouelle
   */
  private final Class<T> clazz;

  /**
   * Holds a flag indicating if we are capturing or not with this list.
   *
   * @author paouelle
   */
  private volatile boolean capturing = true;

  /**
   * Instantiates a new <code>StatementCaptureList</code> object.
   *
   * @author paouelle
   *
   * @param created the information about where this capture list was created
   * @param clazz the class of statements to capture
   */
  StatementCaptureList(StackTraceElement created, Class<T> clazz) {
    this.created = created;
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
    if (capturing) {
      final Stream<? extends GenericStatement> s;

      // handle batch and sequences by capturing all internal statements
      // cast to StatementImpl required to compile on cmdline
      if (((StatementImpl)statement) instanceof ParentStatementImpl) {
        s = ((ParentStatementImpl)((StatementImpl)statement)).objectStatements();
      } else {
        s = Stream.of(statement);
      }
      s.forEachOrdered(os -> {
        if (clazz.isInstance(os)) {
          // first check if we already received this statement in which case we
          // want to skip processing it a second time
          if (list.stream().anyMatch(ls -> ls == os)) { // identity check
            return;
          }
          // now intercept
          for (final Iterator<MutablePair<Consumer<? extends GenericStatement>, Integer>> i = interceptors.iterator(); i.hasNext(); ) {
            final MutablePair<Consumer<? extends GenericStatement>, Integer> m = i.next();
            final int num = m.getRight().intValue() - 1;

            if (num <= 0) { // done with this interceptor; this was the last time
              i.remove();
            }
            ((Consumer)m.getLeft()).accept(os);
          }
          // then capture
          ((List)list).add(os);
        }
      });
    }
  }

  /**
   * Stops capturing and intercepting statements with this capture list.
   *
   * @author paouelle
   *
   * @return this for chaining
   */
  public StatementCaptureList<T> stop() {
    this.capturing = false;
    return this;
  }

  /**
   * Dumps the content of the capture list.
   *
   * @author paouelle
   *
   * @param  logger the logger where to dump the content of this capture list
   * @param  level the log level to use when dumping
   * @return this for chaining
   */
  public StatementCaptureList<T> dump(Logger logger, Level level) {
    if (logger.isEnabled(level)) {
      final int s = size();

      logger.log(level, "");
      logger.log(level, "%20s = %s", "Created at", created);
      logger.log(level, "%20s = %s", "Statement class", clazz.getSimpleName());
      if (s == list.size()) {
        logger.log(level, "%20s = %d", "Size", s);
      } else {
        logger.log(level, "%20s = %d (Collected: %d)", "Size", s, list.size());
      }
      logger.log(level, "%20s:", "Content");
      int j = -1;

      for (final GenericStatement gs: list) {
        if (gs.isEnabled()) {
          logger.log(level, "%20s = %10s -> %s", "[" + (++j) + "]", StatementCaptureList.getType(gs), gs);
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
    return (int)list.stream().filter(GenericStatement::isEnabled).count();
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
    return size() == 0;
  }

  /**
   * Registers a consumer to intercept all executing statement. An interceptor
   * gets called just before the statement is submitted to Cassandra. Upon
   * returning control from the interceptor, the statement will be submitted to
   * Cassandra unless an exception was thrown out.
   *
   * @author paouelle
   *
   * @param  consumer the consumer to call in order to intercept all statements
   * @return this for chaining
   * @throws NullPointerException if <code>consumer</code> is <code>null</code>
   */
  public StatementCaptureList<T> intercept(Consumer<T> consumer) {
    return intercept(Integer.MAX_VALUE, consumer);
  }

  /**
   * Registers a consumer to intercept the next <code>num</code> executing
   * statements. An interceptor gets called just before the statement is submitted
   * to Cassandra. Upon returning control from the interceptor, the statement
   * will be submitted to Cassandra unless an exception was thrown out.
   *
   * @author paouelle
   *
   * @param  num the number of times this interceptor should remain active
   * @param  consumer the consumer to call in order to intercept the next statements
   * @return this for chaining
   * @throws NullPointerException if <code>consumer</code> is <code>null</code>
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public StatementCaptureList<T> intercept(int num, Consumer<T> consumer) {
    org.apache.commons.lang3.Validate.notNull(consumer, "invalid null consumer");
    if (num > 0) {
      ((List)interceptors).add(MutablePair.of(consumer, num));
    } // else - nothing to intercept
    return this;
  }

  /**
   * Registers an interceptor to intercept all executing statement to throw back
   * the specified error. An interceptor gets called just before the statement
   * is submitted to Cassandra.
   * <p>
   * <i>Note:</i> The same exception is throw back each time a statement is
   * intercepted and will have its stack trace re-filled every time.
   *
   * @author paouelle
   *
   * @param  e the error to throw back when intercepting all statements
   * @return this for chaining
   * @throws NullPointerException if <code>e</code> is <code>null</code>
   */
  public StatementCaptureList<T> fail(RuntimeException e) {
    return fail(Integer.MAX_VALUE, e);
  }

  /**
   * Registers an interceptor to intercept the next <code>num</code> executing
   * statements to throw back the specified error. An interceptor gets called
   * just before the statement is submitted to Cassandra.
   * <p>
   * <i>Note:</i> The same exception is throw back each time a statement is
   * intercepted and will have its stack trace re-filled every time.
   *
   * @author paouelle
   *
   * @param  num the number of times this interceptor should remain active
   * @param  e the error to throw back when intercepting the next statements
   * @return this for chaining
   * @throws NullPointerException if <code>e</code> is <code>null</code>
   */
  public StatementCaptureList<T> fail(int num, RuntimeException e) {
    org.apache.commons.lang3.Validate.notNull(e, "invalid null error");
    return intercept(num, s -> { e.fillInStackTrace(); throw e; });
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
  @SuppressWarnings("unchecked")
  public T on(int i) {
    int j = -1;

    for (final GenericStatement<?, ?> s: list) {
      if (s.isEnabled() && (++j == i)) {
        return (T)s;
      }
    }
    throw new AssertionError(
      "not enough captured statements; only "
      + (j + 1)
      + " statements were captured and at least "
      + (i + 1)
      + " was expected"
    );
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
    StatementCaptureListMatchers.assertThat("Capture List", this, matcher);
    return this;
  }
}

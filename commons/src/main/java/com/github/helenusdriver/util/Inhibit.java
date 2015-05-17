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
package com.github.helenusdriver.util;

import java.lang.reflect.InvocationTargetException;

import java.io.InputStream;
import java.io.Reader;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;

import com.github.helenusdriver.commons.lang3.ExceptionUtils;
import com.github.helenusdriver.util.function.E2Runnable;
import com.github.helenusdriver.util.function.E2Supplier;
import com.github.helenusdriver.util.function.EConsumer;
import com.github.helenusdriver.util.function.EFunction;
import com.github.helenusdriver.util.function.ERunnable;
import com.github.helenusdriver.util.function.ESupplier;

/**
 * The <code>Inhibit</code> class defines methods that can be used for properly
 * inhibiting errors and/or conditions in specific situations based on standard
 * algorithms for doing so.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 15, 2015 - paouelle - Creation
 *
 * @since 2.0
 */
public class Inhibit {
  /**
   * Invokes {@link Thread#sleep} while inhibiting interruptions while
   * propagating them properly for the next level.
   * <p>
   * <i>Note:</i> If interrupted, the control is returned right away after
   * having propagated the interruption via the thread's interrupted flag.
   *
   * @author paouelle
   *
   * @param delay the number of milliseconds to sleep for
   */
  public static void interruptionsWhileSleeping(long delay) {
    try {
      Thread.sleep(delay);
    } catch (InterruptedException e) {
      // propagate interruption
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Invokes {@link Thread#sleep} while inhibiting interruptions while
   * propagating them properly for the next level. The specified exception is
   * thrown in place of the interruption.
   * <p>
   * <i>Note:</i> If interrupted, the control is returned right away via the
   * specified exception after having propagated the interruption via the thread's
   * interrupted flag.
   *
   * @author paouelle
   *
   * @param <E> the type of error to throw in case of interruptions
   *
   * @param  e the exception to throw if interrupted
   * @param  delay the number of milliseconds to sleep for
   * @throws E as <code>e</code> if an interruption is detected which will be attached
   *         as the root cause
   */
  public static <E extends Exception> void interruptionsAndThrowOtherWhileSleeping(
    long delay, E e
  ) throws E {
    try {
      Thread.sleep(delay);
    } catch (InterruptedException ie) {
      // propagate interruption
      Thread.currentThread().interrupt();
      // append the interruption to the root cause of the specified exception
      throw ExceptionUtils.withRootCause(e, ie);
    }
  }

  /**
   * Invokes {@link Lock#tryLock} while inhibiting Java interruptions
   * and propagating them after.
   * <p>
   * <i>Note:</i> If interrupted, the control is returned right away after
   * having propagated the interruption properly and <code>false</code> is
   * returned.
   *
   * @author paouelle
   *
   * @param  lock the lock to try to acquire
   * @param  time the maximum time to wait for the lock
   * @param  unit the units of time
   * @return <code>true</code> if the lock was acquired and <code>false</code>
   *         if the waiting time elapsed before the lock was acquired or if the
   *         thread was interrupted
   * @throws NullPointerException if <code>lock</code> is <code>null</code>
   */
  public static final boolean interruptionsWhileTryingLock(
    Lock lock, long time, TimeUnit unit
  ) {
    org.apache.commons.lang3.Validate.notNull(lock, "invalid null lock");
    try {
      return lock.tryLock(time, unit);
    } catch (InterruptedException e) {
      // propagate interruption
      Thread.currentThread().interrupt();
      return false;
    }
  }

  /**
   * Invokes {@link Object#wait} while inhibiting Java interruptions
   * and propagating them after.
   * <p>
   * <i>Note:</i> If interrupted, the control is returned right away after
   * having propagated the interruption properly and <code>null</code> is
   * returned.
   *
   * @author paouelle
   *
   * @param  obj the object to wait for a notification
   * @throws NullPointerException if <code>obj</code> is <code>null</code>
   */
  public static final void interruptionsWhileWaitingForNotificationFrom(
    Object obj
  ) {
    org.apache.commons.lang3.Validate.notNull(obj, "invalid null object");
    try {
      synchronized (obj) {
        obj.wait();
      }
    } catch (InterruptedException e) {
      // propagate interruption
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Invokes {@link Future#get} while inhibiting Java interruptions
   * and propagating them after.
   * <p>
   * <i>Note:</i> If interrupted, the control is returned right away after
   * having propagated the interruption properly and <code>null</code> is
   * returned.
   *
   * @author paouelle
   *
   * @param <T> the type of result computed by the future
   *
   * @param  future the future to try to get
   * @return the computed result if the future returned and <code>null</code>
   *         if the waiting time elapsed before the future returned or if the
   *         thread was interrupted
   * @throws ExecutionException if the computation threw an exception
   * @throws NullPointerException if <code>future</code> is <code>null</code>
   */
  public static final <T> T interruptionsWhileWaitingFor(
    Future<T> future
  ) throws ExecutionException {
    org.apache.commons.lang3.Validate.notNull(future, "invalid null future");
    try {
      return future.get();
    } catch (InterruptedException e) {
      // propagate interruption
      Thread.currentThread().interrupt();
      return null;
    }
  }

  /**
   * Invokes {@link Future#get} while inhibiting Java interruptions
   * and propagating them after.
   * <p>
   * <i>Note:</i> If interrupted, the control is returned right away after
   * having propagated the interruption properly and <code>null</code> is
   * returned.
   *
   * @author paouelle
   *
   * @param <T> the type of result computed by the future
   *
   * @param  future the future to try to get
   * @param  time the maximum time to wait for the future
   * @param  unit the units of time
   * @return the computed result if the future returned and <code>null</code>
   *         if the waiting time elapsed before the future returned or if the
   *         thread was interrupted
   * @throws ExecutionException if the computation threw an exception
   * @throws TimeoutException if the wait timed out
   * @throws NullPointerException if <code>future</code> is <code>null</code>
   */
  public static final <T> T interruptionsWhileWaitingFor(
    Future<T> future, long time, TimeUnit unit
  ) throws ExecutionException, TimeoutException {
    org.apache.commons.lang3.Validate.notNull(future, "invalid null future");
    try {
      return future.get(time, unit);
    } catch (InterruptedException e) {
      // propagate interruption
      Thread.currentThread().interrupt();
      return null;
    }
  }

  /**
   * Invokes the specified operation while inhibiting Java interruptions and
   * propagating them after.
   * <p>
   * Interruptions from the operation are handled by calling the handle function
   * and then propagated properly.
   * <p>
   * <i>Note:</i> Any runtime exceptions or errors thrown out of the
   * handle function will be automatically thrown out.
   *
   * @author paouelle
   *
   * @param <T> the type of result returned by <code>cmd</code>
   * @param <E> the type of exceptions that can be thrown out
   *
   * @param  cmd the non-<code>null</code> operation to inhibit interruptions for
   * @param  handle the non-<code>null</code> function to handle any interruptions
   *         that occurs when calling the operation
   * @return the result returned by <code>c</code> or by <code>handle</code> if
   *         an interruption occurs
   * @throws E if an error occurs while executing the operation
   * @throws RuntimeException if <code>cmd</code> throws a runtime exception
   * @throws Error if <code>cmd</code> throws an error
   * @throws NullPointerException if <code>cmd</code> or <code>handle</code> is
   *         <code>null</code>
   */
  public static final <T, E extends Throwable> T interruptionsAndReturn(
    E2Supplier<T, E, InterruptedException> cmd,
    EFunction<InterruptedException, T, E> handle
  ) throws E {
    org.apache.commons.lang3.Validate.notNull(cmd, "invalid null operation");
    org.apache.commons.lang3.Validate.notNull(handle, "invalid null handle function");
    try {
      return cmd.get();
    } catch (InterruptedException e) {
      try {
        return handle.apply(e);
      } finally {
        // propagate interruption
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Invokes the specified command while inhibiting Java interruptions and
   * propagating them after.
   * <p>
   * Interruptions from the operation are handled by calling the handle consumer
   * and then propagated properly.
   * <p>
   * <i>Note:</i> Any runtime exceptions or errors thrown out of the
   * handle consumer will be automatically thrown out.
   *
   * @author paouelle
   *
   * @param <E> the type of exceptions that can be thrown out
   *
   * @param  cmd the non-<code>null</code> operation to inhibit interruptions for
   * @param  handle the non-<code>null</code> consumer to handle any interruptions
   *         that occurs when calling the operation
   * @throws E if an error occurs while executing the operation
   * @throws RuntimeException if <code>cmd</code> throws a runtime exception
   * @throws Error if <code>cmd</code> throws an error
   * @throws NullPointerException if <code>cmd</code> or <code>handle</code> is
   *         <code>null</code>
   */
  public static final <T, E extends Throwable> void interruptions(
    E2Runnable<E, InterruptedException> cmd,
    EConsumer<InterruptedException, E> handle
  ) throws E {
    org.apache.commons.lang3.Validate.notNull(cmd, "invalid null command");
    org.apache.commons.lang3.Validate.notNull(handle, "invalid null handle consumer");
    try {
      cmd.run();
    } catch (InterruptedException e) {
      try {
        handle.accept(e);
      } finally {
        // propagate interruption
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Invokes the specified operation while inhibiting Java exceptions and
   * interruptions and propagating the interruptions properly after; runtime
   * exceptions are still propagated as expected.
   * <p>
   * Non-runtime exceptions from the operation are handled by calling
   * the handle function.
   * <p>
   * <i>Note:</i> Any runtime exceptions or errors thrown out of the
   * handle function will be automatically thrown out.
   *
   * @author paouelle
   *
   * @param <T> the type of result returned by <code>cmd</code>
   * @param <E> the type of exceptions that can be thrown out
   *
   * @param  cmd the non-<code>null</code> operation to inhibit interruptions and
   *         exceptions for
   * @param  handle the non-<code>null</code> function to handle any exceptions
   *         that occurs when calling the operation
   * @return the result returned by <code>cmd</code> or from the <code>handle</code>
   *         if a non runtime exception or an interruption occurs
   * @throws E if thrown by the handle function
   * @throws RuntimeException if <code>cmd</code> or <code>handle</code> throws
   *         a runtime exception
   * @throws Error if <code>cmd</code> or <code>handle</code> throws an error
   * @throws NullPointerException if <code>cmd</code> or <code>handle</code> is
   *         <code>null</code>
   */
  public static final <T, E extends Exception> T interruptionsAndExceptionsAndReturn(
    ESupplier<T, Exception> cmd, EFunction<Exception, T, E> handle
  ) throws E {
    org.apache.commons.lang3.Validate.notNull(cmd, "invalid null operation");
    org.apache.commons.lang3.Validate.notNull(handle, "invalid null handle function");
    try {
      return cmd.get();
    } catch (InterruptedException e) {
      try {
        return handle.apply(e);
      } finally {
        // propagate interruption
        Thread.currentThread().interrupt();
      }
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      return handle.apply(e);
    }
  }

  /**
   * Invokes the specified command while inhibiting Java exceptions and
   * interruptions and propagating the interruptions properly after; runtime
   * exceptions are still propagated as expected.
   * <p>
   * Non-runtime exceptions from the operation are handled by calling
   * the handle consumer.
   * <p>
   * <i>Note:</i> Any runtime exceptions or errors thrown out of the
   * handle consumer will be automatically thrown out.
   *
   * @author paouelle
   *
   * @param <E> the type of exceptions that can be thrown out
   *
   * @param  cmd the non-<code>null</code> command to inhibit interruptions and
   *         exceptions for
   * @param  handle the non-<code>null</code> consumer to handle any exceptions
   *         that occurs when calling the operation
   * @throws E if thrown by the handle function
   * @throws RuntimeException if <code>cmd</code> or <code>handle</code> throws
   *         a runtime exception
   * @throws Error if <code>cmd</code> or <code>handle</code> throws an error
   * @throws NullPointerException if <code>cmd</code> or <code>handle</code> is
   *         <code>null</code>
   */
  public static final <E extends Exception> void interruptionsAndExceptions(
    ERunnable<Exception> cmd, EConsumer<Exception, E> handle
  ) throws E {
    org.apache.commons.lang3.Validate.notNull(cmd, "invalid null command");
    org.apache.commons.lang3.Validate.notNull(handle, "invalid null handle consumer");
    try {
      cmd.run();
    } catch (InterruptedException e) {
      try {
        handle.accept(e);
      } finally {
        // propagate interruption
        Thread.currentThread().interrupt();
      }
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      handle.accept(e);
    }
  }

  /**
   * Invoked the specified operation object while inhibiting Java interruptions
   * and propagating them after. This version of the method will retry calling
   * the operation if an interruption occurs.
   *
   * @author paouelle
   *
   * @param <T> the type of result returned by <code>cmd</code>
   * @param <E> the type of exceptions that can be thrown out
   *
   * @param  cmd the non-<code>null</code> operation to inhibit interruptions for
   * @return the result returned by <code>cmd</code>
   * @throws E if an error occurs while executing the operation
   * @throws RuntimeException if <code>cmd</code> throws a runtime exception
   * @throws Error if <code>cmd</code> throws an error
   * @throws NullPointerException if <code>cmd</code> is <code>null</code>
   */
  public static final <T, E extends Throwable> T interruptionsWhileRetryingAndReturn(
    E2Supplier<T, E, InterruptedException> cmd
  ) throws E {
    org.apache.commons.lang3.Validate.notNull(cmd, "invalid null operation");
    boolean interrupted = false;

    try {
      while (true) {
        try {
          return cmd.get();
        } catch (InterruptedException e) {
          // remember and continue;
          interrupted = true;
        }
      }
    } finally {
      if (interrupted) {
        // propagate interruptions
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Invoked the specified command object while inhibiting Java interruptions
   * and propagating them after. This version of the method will retry calling
   * the operation if an interruption occurs.
   *
   * @author paouelle
   *
   * @param <E> the type of exceptions that can be thrown out
   *
   * @param  cmd the non-<code>null</code> command to inhibit interruptions for
   * @throws E if an error occurs while executing the command
   * @throws RuntimeException if <code>cmd</code> throws a runtime exception
   * @throws Error if <code>cmd</code> throws an error
   * @throws NullPointerException if <code>cmd</code> is <code>null</code>
   */
  public static final <E extends Throwable> void interruptionsWhileRetrying(
    E2Runnable<E, InterruptedException> cmd
  ) throws E {
    org.apache.commons.lang3.Validate.notNull(cmd, "invalid null command");
    boolean interrupted = false;

    try {
      while (true) {
        try {
          cmd.run();
        } catch (InterruptedException e) {
          // remember and continue;
          interrupted = true;
        }
      }
    } finally {
      if (interrupted) {
        // propagate interruptions
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Invokes {@link Lock#tryLock} while inhibiting Java interruptions
   * and propagating them after. This version of the method will
   * retry trying the lock if an interruption occurs.
   *
   * @author paouelle
   *
   * @param  lock the lock to try to acquire
   * @param  time the maximum time to wait for the lock
   * @param  unit the units of time
   * @return <code>true</code> if the lock was acquired and <code>false</code>
   *         if the waiting time elapsed before the lock was acquired or if the
   *         thread was interrupted
   * @throws NullPointerException if <code>lock</code> is <code>null</code>
   */
  public static final boolean interruptionsWhileRetryingTryingLock(
    Lock lock, long time, TimeUnit unit
  ) {
    long end = System.nanoTime() + unit.toNanos(time);
    boolean interrupted = false;

    try {
      while (true) {
        long nanos = end - System.nanoTime();

        if (nanos < 0L) { // no more time left
          return false;
        }
        try {
          return lock.tryLock(nanos, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
          // remember and continue;
          interrupted = true;
        }
      }
    } finally {
      if (interrupted) {
        // propagate interruptions
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Invokes {@link Future#get} while inhibiting Java interruptions
   * and propagating them after. This version of the method will
   * retry waiting for the future result if an interruption occurs.
   *
   * @author paouelle
   *
   * @param <T> the type of result computed by the future
   *
   * @param  future the future to try to get
   * @return the computed result if the future returned and <code>null</code>
   *         if the waiting time elapsed before the future returned or if the
   *         thread was interrupted
   * @throws ExecutionException if the computation threw an exception
   * @throws NullPointerException if <code>future</code> is <code>null</code>
   */
  public static final <T> T interruptionsWhileRetryingWaitingFor(
    Future<T> future
  ) throws ExecutionException {
    org.apache.commons.lang3.Validate.notNull(future, "invalid null future");
    boolean interrupted = false;

    try {
      while (true) {
        try {
          return future.get();
        } catch (InterruptedException e) {
          // remember and continue;
          interrupted = true;
        }
      }
    } finally {
      if (interrupted) {
        // propagate interruptions
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Invokes {@link Future#get} while inhibiting Java interruptions
   * and propagating them after. This version of the method will
   * retry waiting for the future result if an interruption occurs.
   *
   * @author paouelle
   *
   * @param <T> the type of result computed by the future
   *
   * @param  future the future to try to get
   * @param  time the maximum time to wait for the future
   * @param  unit the units of time
   * @return the computed result if the future returned and <code>null</code>
   *         if the waiting time elapsed before the future returned or if the
   *         thread was interrupted
   * @throws ExecutionException if the computation threw an exception
   * @throws TimeoutException if the wait timed out
   * @throws NullPointerException if <code>future</code> is <code>null</code>
   */
  public static final <T> T interruptionsWhileRetryingWaitingFor(
    Future<T> future, long time, TimeUnit unit
  ) throws ExecutionException, TimeoutException {
    org.apache.commons.lang3.Validate.notNull(future, "invalid null future");
    long end = System.nanoTime() + unit.toNanos(time);
    boolean interrupted = false;

    try {
      while (true) {
        long nanos = end - System.nanoTime();

        if (nanos < 0L) { // no more time left
          throw new TimeoutException();
        }
        try {
          return future.get(nanos, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
          // remember and continue;
          interrupted = true;
        }
      }
    } finally {
      if (interrupted) {
        // propagate interruptions
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Invokes {@link Object#wait} while inhibiting Java interruptions
   * and propagating them after. This version of the method will
   * retry waiting for notifications from the object if an interruption occurs.
   *
   * @author paouelle
   *
   * @param  obj the object to wait for a notification
   * @throws NullPointerException if <code>obj</code> is <code>null</code>
   */
  public static final void interruptionsWhileRetryingWaitingForNotificationFrom(
    Object obj
  ) {
    org.apache.commons.lang3.Validate.notNull(obj, "invalid null object");
    boolean interrupted = false;

    try {
      synchronized (obj) {
        while (true) {
          try {
            obj.wait();
            return;
          } catch (InterruptedException e) {
            // remember and continue;
            interrupted = true;
          }
        }
      }
    } finally {
      if (interrupted) {
        // propagate interruption
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Invokes the specified operation while inhibiting Java exceptions and
   * interruptions and propagating the interruptions properly after. This
   * version of the method will retry executing the operation if an interruption
   * occurs.
   * <p>
   * Non-runtime exceptions from the operation are handled by calling
   * the handle function.
   * <p>
   * <i>Note:</i> Any runtime exceptions or errors thrown out of the
   * handle function will be automatically thrown out.
   *
   * @author paouelle
   *
   * @param <T> the type of result returned by <code>cmd</code>
   * @param <E> the type of exceptions that can be thrown out
   *
   * @param  cmd the non-<code>null</code> operation to inhibit interruptions
   *         and exceptions for
   * @param  handle the non-<code>null</code> function to handle any exceptions
   *         that occurs when calling the operation
   * @return the result returned by <code>cmd</code> or from the <code>handle</code>
   *         if a non runtime exception occurs
   * @throws E if thrown by the handle function
   * @throws RuntimeException if <code>cmd</code> or <code>handle</code> throws
   *         a runtime exception
   * @throws Error if <code>cmd</code> or <code>handle</code> throws an error
   * @throws NullPointerException if <code>cmd</code> or <code>handle</code> is
   *         <code>null</code>
   */
  public static final <T, E extends Exception> T interruptionsWhileRetryingAndExceptionsAndReturn(
    ESupplier<T, Exception> cmd, EFunction<Exception, T, E> handle
  ) throws E {
    org.apache.commons.lang3.Validate.notNull(cmd, "invalid null operation");
    org.apache.commons.lang3.Validate.notNull(handle, "invalid null handle function");
    try {
      return Inhibit.interruptionsWhileRetryingAndReturn(
        () -> cmd.get()
      );
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      return handle.apply(e);
    }
  }

  /**
   * Invokes the specified command while inhibiting Java exceptions and
   * interruptions and propagating the interruptions properly after. This
   * version of the method will retry executing the operation if an interruption
   * occurs.
   * <p>
   * Non-runtime exceptions from the operation are handled by calling
   * the handle consumer.
   * <p>
   * <i>Note:</i> Any runtime exceptions or errors thrown out of the
   * handle consumer will be automatically thrown out.
   *
   * @author paouelle
   *
   * @param <E> the type of exceptions that can be thrown out
   *
   * @param  cmd the non-<code>null</code> command to inhibit interruptions
   *         and exceptions for
   * @param  handle the non-<code>null</code> consumer to handle any exceptions
   *         that occurs when calling the operation
   * @throws E if thrown by the handle consumer
   * @throws RuntimeException if <code>cmd</code> or <code>handle</code> throws
   *         a runtime exception
   * @throws Error if <code>cmd</code> or <code>handle</code> throws an error
   * @throws NullPointerException if <code>cmd</code> or <code>handle</code> is
   *         <code>null</code>
   */
  public static final <E extends Exception> void interruptionsWhileRetryingAndExceptions(
    ERunnable<Exception> cmd, EConsumer<Exception, E> handle
  ) throws E {
    org.apache.commons.lang3.Validate.notNull(cmd, "invalid null command");
    org.apache.commons.lang3.Validate.notNull(handle, "invalid null handle consumer");
    try {
      Inhibit.interruptionsWhileRetrying(
        () -> cmd.run()
      );
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      handle.accept(e);
    }
  }

  /**
   * Inhibits exceptions while closing the specified reader.
   *
   * @author paouelle
   *
   * @param r the reader to be closed
   */
  public static final void exceptionsWhileClosing(Reader r) {
    if (r != null) {
      try {
        r.close();
      } catch (Exception e) { // ignore all exception
      }
    }
  }

  /**
   * Inhibits exceptions while closing the specified input stream.
   *
   * @author paouelle
   *
   * @param is the input stream to be closed
   */
  public static final void exceptionsWhileClosing(InputStream is) {
    if (is != null) {
      try {
        is.close();
      } catch (Exception e) { // ignore all exception
      }
    }
  }

  /**
   * Invokes the specified operation while inhibiting Java errors and
   * exceptions. This is done in a controlled way where:
   *
   * - {@link OutOfMemoryError} are not intercepted; nor are they logged. These
   *   are thrown back as they should propagate up. Attempting to do anything
   *   else can cause additional memory issues and corruption.
   * - {@link StackOverflowError} are not intercepted; nor are they logged. These
   *   are thrown back as they should propagate up.
   * - {@link AssertionError} are not intercepted; nor are they logged. These
   *   are thrown back as they should propagate up so JUnit test case can
   *   stopped when an assertion fails.
   * - {@link ThreadDeath} are not intercepted; nor are they logged. These
   *   are thrown back as they should propagate up. These errors are part of Java's
   *   mechanism for stopping threads.
   * <p>
   * All other errors and exceptions from the operation are handled by calling
   * the handle function.
   *
   * @author paouelle
   *
   * @param <T> the type of result returned by <code>cmd</code>
   * @param <E> the type of exceptions that can be thrown out
   *
   * @param  cmd the non-<code>null</code> operation to execute and inhibit
   *         errors and exceptions for
   * @param  handle the non-<code>null</code> function to handle any other exceptions
   *         or errors that occurs when calling the operation
   * @return the result returned by <code>cmd</code> or <code>handle</code>
   * @throws E if thrown by the handle function
   * @throws NullPointerException if <code>cmd</code> or <code>handle</code> is
   *         <code>null</code>
   */
  public static final <T, E extends Throwable> T throwablesAndReturn(
    ESupplier<T, Throwable> cmd, EFunction<Throwable, T, E> handle
  ) throws E {
    org.apache.commons.lang3.Validate.notNull(cmd, "invalid null operation");
    org.apache.commons.lang3.Validate.notNull(handle, "invalid null handle function");
    try {
      return cmd.get();
    } catch (OutOfMemoryError e) {
      throw e;
    } catch (StackOverflowError e) {
      throw e;
    } catch (AssertionError e) {
      throw e;
    } catch (ThreadDeath t) {
      throw t;
    } catch (Throwable t) {
      return handle.apply(t);
    }
  }

  /**
   * Invokes the specified command while inhibiting Java errors and
   * exceptions. This is done in a controlled way where:
   *
   * - {@link OutOfMemoryError} are not intercepted; nor are they logged. These
   *   are thrown back as they should propagate up. Attempting to do anything
   *   else can cause additional memory issues and corruption.
   * - {@link StackOverflowError} are not intercepted; nor are they logged. These
   *   are thrown back as they should propagate up.
   * - {@link AssertionError} are not intercepted; nor are they logged. These
   *   are thrown back as they should propagate up so JUnit test case can
   *   stopped when an assertion fails.
   * - {@link ThreadDeath} are not intercepted; nor are they logged. These
   *   are thrown back as they should propagate up. These errors are part of Java's
   *   mechanism for stopping threads.
   * <p>
   * All other errors and exceptions from the command are handled by calling
   * the handle consumer.
   *
   * @author paouelle
   *
   * @param <E> the type of exceptions that can be thrown out
   *
   * @param  cmd the non-<code>null</code> command to execute and inhibit
   *         errors and exceptions for
   * @param  handle the non-<code>null</code> consumer to handle any other exceptions
   *         or errors that occurs when calling the operation
   * @throws E if thrown by the handle consumer
   * @throws NullPointerException if <code>cmd</code> or <code>handle</code> is
   *         <code>null</code>
   */
  public static final <E extends Throwable> void throwables(
    ERunnable<Throwable> cmd, EConsumer<Throwable, E> handle
  ) throws E {
    org.apache.commons.lang3.Validate.notNull(cmd, "invalid null command");
    org.apache.commons.lang3.Validate.notNull(handle, "invalid null handle consumer");
    try {
      cmd.run();
    } catch (OutOfMemoryError e) {
      throw e;
    } catch (StackOverflowError e) {
      throw e;
    } catch (AssertionError e) {
      throw e;
    } catch (ThreadDeath t) {
      throw t;
    } catch (Throwable t) {
      handle.accept(t);
    }
  }

  /**
   * Invokes the specified command while inhibiting Java errors and
   * exceptions. This is done in a controlled way where:
   *
   * - {@link OutOfMemoryError} are not intercepted; nor are they logged. These
   *   are thrown back as they should propagate up. Attempting to do anything
   *   else can cause additional memory issues and corruption.
   * - {@link StackOverflowError} are not intercepted; nor are they logged. These
   *   are thrown back as they should propagate up.
   * - {@link AssertionError} are not intercepted; nor are they logged. These
   *   are thrown back as they should propagate up so JUnit test case can
   *   stopped when an assertion fails.
   * - {@link ThreadDeath} are not intercepted; nor are they logged. These
   *   are thrown back as they should propagate up. These errors are part of Java's
   *   mechanism for stopping threads.
   * <p>
   * All other errors and exceptions are simply ignored.
   *
   * @author paouelle
   *
   * @param  cmd the non-<code>null</code> command to execute and inhibit
   *         errors and exceptions for
   * @throws NullPointerException if <code>cmd</code> or <code>handle</code> is
   *         <code>null</code>
   */
  public static final void throwables(ERunnable<Throwable> cmd) {
    org.apache.commons.lang3.Validate.notNull(cmd, "invalid null command");
    try {
      cmd.run();
    } catch (OutOfMemoryError e) {
      throw e;
    } catch (StackOverflowError e) {
      throw e;
    } catch (AssertionError e) {
      throw e;
    } catch (ThreadDeath t) {
      throw t;
    } catch (Throwable t) {
    }
  }

  /**
   * Invokes the specified function while inhibiting Java errors and
   * exceptions. This is done in a controlled way where:
   *
   * - {@link OutOfMemoryError} are not intercepted; nor are they logged. These
   *   are thrown back as they should propagate up. Attempting to do anything
   *   else can cause additional memory issues and corruption.
   * - {@link StackOverflowError} are not intercepted; nor are they logged. These
   *   are thrown back as they should propagate up.
   * - {@link AssertionError} are not intercepted; nor are they logged. These
   *   are thrown back as they should propagate up so JUnit test case can
   *   stopped when an assertion fails.
   * - {@link ThreadDeath} are not intercepted; nor are they logged. These
   *   are thrown back as they should propagate up. These errors are part of Java's
   *   mechanism for stopping threads.
   * <p>
   * All other errors and exceptions from the operation are handled by calling
   * the handle function.
   *
   * @author paouelle
   *
   * @param <T> the type of argument passed to <code>cmd</code>
   * @param <R> the type of result returned by <code>cmd</code>
   * @param <E> the type of exceptions that can be thrown out
   *
   * @param  arg the argument to pass to the function
   * @param  cmd the non-<code>null</code> function to execute and inhibit
   *         errors and exceptions for
   * @param  handle the non-<code>null</code> function to handle any other exceptions
   *         or errors that occurs when calling the operation
   * @return the result returned by <code>cmd</code> or <code>handle</code>
   * @throws E if thrown by the handle function
   * @throws NullPointerException if <code>cmd</code> or <code>handle</code> is
   *         <code>null</code>
   */
  public static final <T, R, E extends Throwable> R throwablesAndReturn(
    T arg, EFunction<T, R, Throwable> cmd, EFunction<Throwable, R, E> handle
  ) throws E {
    org.apache.commons.lang3.Validate.notNull(cmd, "invalid null operation");
    org.apache.commons.lang3.Validate.notNull(handle, "invalid null handle function");
    try {
      return cmd.apply(arg);
    } catch (OutOfMemoryError e) {
      throw e;
    } catch (StackOverflowError e) {
      throw e;
    } catch (AssertionError e) {
      throw e;
    } catch (ThreadDeath t) {
      throw t;
    } catch (Throwable t) {
      return handle.apply(t);
    }
  }

  /**
   * Invokes the specified consumer while inhibiting Java errors and
   * exceptions. This is done in a controlled way where:
   *
   * - {@link OutOfMemoryError} are not intercepted; nor are they logged. These
   *   are thrown back as they should propagate up. Attempting to do anything
   *   else can cause additional memory issues and corruption.
   * - {@link StackOverflowError} are not intercepted; nor are they logged. These
   *   are thrown back as they should propagate up.
   * - {@link AssertionError} are not intercepted; nor are they logged. These
   *   are thrown back as they should propagate up so JUnit test case can
   *   stopped when an assertion fails.
   * - {@link ThreadDeath} are not intercepted; nor are they logged. These
   *   are thrown back as they should propagate up. These errors are part of Java's
   *   mechanism for stopping threads.
   * <p>
   * All other errors and exceptions from the consumer are handled by calling
   * the handle consumer.
   *
   * @author paouelle
   *
   * @param <T> the type of argument passed to <code>cmd</code>
   * @param <E> the type of exceptions that can be thrown out
   *
   * @param  arg the argument to pass to the consumer
   * @param  cmd the non-<code>null</code> consumer to execute and inhibit
   *         errors and exceptions for
   * @param  handle the non-<code>null</code> consumer to handle any other exceptions
   *         or errors that occurs when calling the consumer
   * @throws E if thrown by the handle consumer
   * @throws NullPointerException if <code>cmd</code> or <code>handle</code> is
   *         <code>null</code>
   */
  public static final <T, E extends Throwable> void throwables(
    T arg, EConsumer<T, Throwable> cmd, EConsumer<Throwable, E> handle
  ) throws E {
    org.apache.commons.lang3.Validate.notNull(cmd, "invalid null consumer");
    org.apache.commons.lang3.Validate.notNull(handle, "invalid null handle consumer");
    try {
      cmd.accept(arg);
    } catch (OutOfMemoryError e) {
      throw e;
    } catch (StackOverflowError e) {
      throw e;
    } catch (AssertionError e) {
      throw e;
    } catch (ThreadDeath t) {
      throw t;
    } catch (Throwable t) {
      handle.accept(t);
    }
  }

  /**
   * Invokes the specified operation while inhibiting Java errors, exceptions,
   * and unwrapping target exceptions. This is done in a controlled way where:
   *
   * - {@link InvocationTargetException} are first unwrapped by calling
   *   {@link InvocationTargetException#getTargetException}.
   * - {@link ExceptionInInitializerError} are first unwrapped by calling
   *   {@link ExceptionInInitializerError#getException()}.
   * - {@link OutOfMemoryError} are not intercepted; nor are they logged. These
   *   are thrown back as they should propagate up. Attempting to do anything
   *   else can cause additional memory issues and corruption.
   * - {@link StackOverflowError} are not intercepted; nor are they logged. These
   *   are thrown back as they should propagate up.
   * - {@link AssertionError} are not intercepted; nor are they logged. These
   *   are thrown back as they should propagate up so JUnit test case can
   *   stopped when an assertion fails.
   * - {@link ThreadDeath} are not intercepted; nor are they logged. These
   *   are thrown back as they should propagate up. These errors are part of Java's
   *   mechanism for stopping threads.
   * <p>
   * All other errors and exceptions from the operation are handled by calling
   * the handle function.
   *
   * @author paouelle
   *
   * @param <T> the type of result returned by <code>cmd</code>
   * @param <E> the type of exceptions that can be thrown out
   *
   * @param  cmd the non-<code>null</code> operation to execute and inhibit
   *         errors and exceptions for
   * @param  handle the non-<code>null</code> function to handle any other exceptions
   *         or errors that occurs when calling the operation
   * @return the result returned by <code>cmd</code> or <code>handle</code>
   * @throws E if thrown by the handle function
   * @throws NullPointerException if <code>cmd</code> or <code>handle</code> is
   *         <code>null</code>
   */
  public static final <T, E extends Throwable> T unwrappedThrowablesAndReturn(
    ESupplier<T, Throwable> cmd, EFunction<Throwable, T, E> handle
  ) throws E {
    org.apache.commons.lang3.Validate.notNull(cmd, "invalid null operation");
    return Inhibit.throwablesAndReturn(
      () -> {
        try {
          return cmd.get();
        } catch (InvocationTargetException e) {
          final Throwable te = e.getTargetException();

          throw (te != null) ? te : e;
        } catch (ExceptionInInitializerError e) {
          final Throwable te = e.getException();

          throw (te != null) ? te : e;
        }
      },
      handle
    );
  }

  /**
   * Invokes the specified command while inhibiting Java errors, exceptions,
   * and unwrapping target exceptions. This is done in a controlled way where:
   *
   * - {@link InvocationTargetException} are first unwrapped by calling
   *   {@link InvocationTargetException#getTargetException}.
   * - {@link ExceptionInInitializerError} are first unwrapped by calling
   *   {@link ExceptionInInitializerError#getException()}.
   * - {@link OutOfMemoryError} are not intercepted; nor are they logged. These
   *   are thrown back as they should propagate up. Attempting to do anything
   *   else can cause additional memory issues and corruption.
   * - {@link StackOverflowError} are not intercepted; nor are they logged. These
   *   are thrown back as they should propagate up.
   * - {@link AssertionError} are not intercepted; nor are they logged. These
   *   are thrown back as they should propagate up so JUnit test case can
   *   stopped when an assertion fails.
   * - {@link ThreadDeath} are not intercepted; nor are they logged. These
   *   are thrown back as they should propagate up. These errors are part of Java's
   *   mechanism for stopping threads.
   * <p>
   * All other errors and exceptions from the command are handled by calling
   * the handle consumer.
   *
   * @author paouelle
   *
   * @param <E> the type of exceptions that can be thrown out
   *
   * @param  cmd the non-<code>null</code> command to execute and inhibit
   *         errors and exceptions for
   * @param  handle the non-<code>null</code> consumer to handle any other exceptions
   *         or errors that occurs when calling the operation
   * @throws E if thrown by the handle consumer
   * @throws NullPointerException if <code>cmd</code> or <code>handle</code> is
   *         <code>null</code>
   */
  public static final <E extends Throwable> void unwrappedThrowables(
    ERunnable<Throwable> cmd, EConsumer<Throwable, E> handle
  ) throws E {
    org.apache.commons.lang3.Validate.notNull(cmd, "invalid null command");
    Inhibit.unwrappedThrowables(
      () -> {
        try {
          cmd.run();
        } catch (InvocationTargetException e) {
          final Throwable te = e.getTargetException();

          throw (te != null) ? te : e;
        } catch (ExceptionInInitializerError e) {
          final Throwable te = e.getException();

          throw (te != null) ? te : e;
        }
      },
      handle
    );
  }

  /**
   * Invokes the specified function while inhibiting Java errors, exceptions,
   * and unwrapping target exceptions. This is done in a controlled way where:
   *
   * - {@link InvocationTargetException} are first unwrapped by calling
   *   {@link InvocationTargetException#getTargetException}.
   * - {@link ExceptionInInitializerError} are first unwrapped by calling
   *   {@link ExceptionInInitializerError#getException()}.
   * - {@link OutOfMemoryError} are not intercepted; nor are they logged. These
   *   are thrown back as they should propagate up. Attempting to do anything
   *   else can cause additional memory issues and corruption.
   * - {@link StackOverflowError} are not intercepted; nor are they logged. These
   *   are thrown back as they should propagate up.
   * - {@link AssertionError} are not intercepted; nor are they logged. These
   *   are thrown back as they should propagate up so JUnit test case can
   *   stopped when an assertion fails.
   * - {@link ThreadDeath} are not intercepted; nor are they logged. These
   *   are thrown back as they should propagate up. These errors are part of Java's
   *   mechanism for stopping threads.
   * <p>
   * All other errors and exceptions from the operation are handled by calling
   * the handle function.
   *
   * @author paouelle
   *
   * @param <T> the type of argument passed to <code>cmd</code>
   * @param <R> the type of result returned by <code>cmd</code>
   * @param <E> the type of exceptions that can be thrown out
   *
   * @param  arg the argument to pass to the function
   * @param  cmd the non-<code>null</code> function to execute and inhibit
   *         errors and exceptions for
   * @param  handle the non-<code>null</code> function to handle any other exceptions
   *         or errors that occurs when calling the operation
   * @return the result returned by <code>cmd</code> or <code>handle</code>
   * @throws E if thrown by the handle function
   * @throws NullPointerException if <code>cmd</code> or <code>handle</code> is
   *         <code>null</code>
   */
  public static final <T, R, E extends Throwable> R unwrappedThrowablesAndReturn(
    T arg, EFunction<T, R, Throwable> cmd, EFunction<Throwable, R, E> handle
  ) throws E {
    org.apache.commons.lang3.Validate.notNull(cmd, "invalid null operation");
    return Inhibit.throwablesAndReturn(
      arg,
      a -> {
        try {
          return cmd.apply(a);
        } catch (InvocationTargetException e) {
          final Throwable te = e.getTargetException();

          throw (te != null) ? te : e;
        } catch (ExceptionInInitializerError e) {
          final Throwable te = e.getException();

          throw (te != null) ? te : e;
        }
      },
      handle
    );
  }

  /**
   * Invokes the specified consumer while inhibiting Java errors, exceptions,
   * and unwrapping target exceptions. This is done in a controlled way where:
   *
   * - {@link InvocationTargetException} are first unwrapped by calling
   *   {@link InvocationTargetException#getTargetException}.
   * - {@link ExceptionInInitializerError} are first unwrapped by calling
   *   {@link ExceptionInInitializerError#getException()}.
   * - {@link OutOfMemoryError} are not intercepted; nor are they logged. These
   *   are thrown back as they should propagate up. Attempting to do anything
   *   else can cause additional memory issues and corruption.
   * - {@link StackOverflowError} are not intercepted; nor are they logged. These
   *   are thrown back as they should propagate up.
   * - {@link AssertionError} are not intercepted; nor are they logged. These
   *   are thrown back as they should propagate up so JUnit test case can
   *   stopped when an assertion fails.
   * - {@link ThreadDeath} are not intercepted; nor are they logged. These
   *   are thrown back as they should propagate up. These errors are part of Java's
   *   mechanism for stopping threads.
   * <p>
   * All other errors and exceptions from the consumer are handled by calling
   * the handle consumer.
   *
   * @author paouelle
   *
   * @param <T> the type of argument passed to <code>cmd</code>
   * @param <E> the type of exceptions that can be thrown out
   *
   * @param  arg the argument to pass to the consumer
   * @param  cmd the non-<code>null</code> consumer to execute and inhibit
   *         errors and exceptions for
   * @param  handle the non-<code>null</code> consumer to handle any other exceptions
   *         or errors that occurs when calling the consumer
   * @throws E if thrown by the handle consumer
   * @throws NullPointerException if <code>cmd</code> or <code>handle</code> is
   *         <code>null</code>
   */
  public static final <T, E extends Throwable> void unwrappedThrowables(
    T arg, EConsumer<T, Throwable> cmd, EConsumer<Throwable, E> handle
  ) throws E {
    org.apache.commons.lang3.Validate.notNull(cmd, "invalid null consumer");
    Inhibit.throwables(
      arg,
      a -> {
        try {
          cmd.accept(a);
        } catch (InvocationTargetException e) {
          final Throwable te = e.getTargetException();

          throw (te != null) ? te : e;
        } catch (ExceptionInInitializerError e) {
          final Throwable te = e.getException();

          throw (te != null) ? te : e;
        }
      },
      handle
    );
  }

  /**
   * Prevents instantiation.
   *
   * @author paouelle
   */
  private Inhibit() {}
}

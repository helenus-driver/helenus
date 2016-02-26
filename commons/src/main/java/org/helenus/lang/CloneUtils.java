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
package org.helenus.lang;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * The <code>CloneUtils</code> class provides cloning utility methods.
 *
 * @copyright 2015-2016 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Feb 12, 2016 - paouelle - Creation
 *
 * @since 1.0
 */
public class CloneUtils {
  /**
   * Clones the specified object.
   * <p>
   * <i>Note:</i> This implementation will convert a {@link CloneNotSupportedException}
   * exception into a {@link IllegalArgumentException}.
   *
   * @author paouelle
   *
   * @param  obj the object to be cloned
   * @return a cloned version of the object or <code>null</code> if
   *         <code>obj</code> was <code>null</code>
   * @throws IllegalArgumentException if cloning is not supported
   * @throws IllegalStateException if cloning throws an unexpected exception
   * @throws NoSuchMethodError if the object doesn't provide a public
   *         implementation for the {@link Object#clone} method
   * @throws IllegalAccessError if the object's <Code>clone()</code> cannot be
   *         accessed
   */
  @SuppressWarnings("unchecked")
  public static <T> T clone(T obj) {
    if (obj == null) {
      return null;
    }
    if (obj instanceof Cloneable) {
      final Class<?> clazz = obj.getClass();
      final Method m;

      try {
        m = clazz.getMethod("clone", (Class[])null);
      } catch (NoSuchMethodException e) {
        throw (NoSuchMethodError)new NoSuchMethodError(e.getMessage()).initCause(e);
      }
      try {
        return (T)m.invoke(obj, (Object[])null);
      } catch (InvocationTargetException e) {
        final Throwable cause = e.getCause();

        if (cause instanceof CloneNotSupportedException) {
          throw new IllegalArgumentException(cause.getMessage(), cause);
        }
        throw new IllegalStateException("unexpected exception", cause);
      } catch (IllegalAccessException e) {
        throw (IllegalAccessError)new IllegalAccessError(e.getMessage()).initCause(e);
      }
    }
    throw new IllegalArgumentException("clone not supported");
  }

  /**
   * Prevents instantiation.
   *
   * @author paouelle
   */
  private CloneUtils() {}
}

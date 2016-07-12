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
package org.helenus.lang.reflect;

/**
 * The <code>Array</code> class provides extends on Java's
 * {@link java.lang.reflect.Array} class.
 *
 * @copyright 2015-2016 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jul 12, 2016 - paouelle - Creation
 *
 * @since 1.0
 */
public class Array {
  /**
   * Gets a string representation of the contents of the specified array object.
   * If the array contains other arrays as elements, they are converted to
   * strings by the {@link Object#toString} method inherited from
   * <tt>Object</tt>, which describes their <i>identities</i> rather than
   * their contents.
   * <p>
   * The value returned by this method is equal to the value that would
   * be returned by <tt>Arrays.asList(a).toString()</tt>, unless <tt>a</tt>
   * is <tt>null</tt>, in which case <tt>"null"</tt> is returned.
   *
   * @param  array the array whose string representation to return
   * @return a string representation of the provided array
   */
  public static String toString(Object array) {
    if (array == null) {
      return "null";
    }
    final int iMax = java.lang.reflect.Array.getLength(array) - 1;

    if (iMax == -1) {
      return "[]";
    }
    final StringBuilder b = new StringBuilder();

    b.append('[');
    for (int i = 0; ; i++) {
      b.append(String.valueOf(java.lang.reflect.Array.get(array, i)));
      if (i == iMax) {
        return b.append(']').toString();
      }
      b.append(", ");
    }
  }

}

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
package org.helenus.util;

import org.helenus.lang.Tolerable;

/**
 * The <code>Arrays</code> class extends on {@link java.util.Arrays} to
 * provide support for {@link Tolerable} objects.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jul 20, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
public class Arrays {
  /**
   * Compares the two objects together while supporting deep equality with a
   * tolerable error and supporting arrays.
   *
   * @author <a href="mailto:paouelle@enlightedinc.com">paouelle</a>
   *
   * @param  e1 the first object to compare
   * @param  e2 the second one to compare with
   * @param  epsilon the tolerable error
   * @return <code>true</code> if both objects are deep equal with a tolerable
   *         error
   */
  static boolean deepEquals0(Object e1, Object e2, double epsilon) {
    assert e1 != null;
    if ((e1 instanceof Object[]) && (e2 instanceof Object[])) {
      return Arrays.deepEquals((Object[])e1, (Object[])e2, epsilon);
    } else if ((e1 instanceof byte[]) && (e2 instanceof byte[])) {
      return java.util.Arrays.equals((byte[])e1, (byte[])e2);
    } else if ((e1 instanceof short[]) && (e2 instanceof short[])) {
      return java.util.Arrays.equals((short[])e1, (short[])e2);
    } else if ((e1 instanceof int[]) && (e2 instanceof int[])) {
      return java.util.Arrays.equals((int[])e1, (int[])e2);
    } else if ((e1 instanceof long[]) && (e2 instanceof long[])) {
      return java.util.Arrays.equals((long[])e1, (long[])e2);
    } else if ((e1 instanceof char[]) && (e2 instanceof char[])) {
      return java.util.Arrays.equals((char[])e1, (char[])e2);
    } else if ((e1 instanceof float[]) && (e2 instanceof float[])) {
      return Arrays.equals((float[])e1, (float[])e2, epsilon);
    } else if ((e1 instanceof double[]) && (e2 instanceof double[])) {
      return Arrays.equals((double[])e1, (double[])e2, epsilon);
    } else if ((e1 instanceof boolean[]) && (e2 instanceof boolean[])) {
      return java.util.Arrays.equals((boolean[])e1, (boolean[])e2);
    }
    return Objects.equals(e1, e2, epsilon);
  }

  /**
   * Returns <code>true</code> if the two specified arrays of doubles are
   * <i>equal</i> to one another with a tolerable error. Two arrays are
   * considered equal if both arrays contain the same number of elements, and
   * all corresponding pairs of elements in the two arrays are equal with a
   * tolerable error. In other words, two arrays are equal if they contain the
   * same elements with a tolerable error in the same order. Also, two array
   * references are considered equal if both are <code>null</code>.
   *
   * @param  a one array to be tested for equality
   * @param  a2 the other array to be tested for equality
   * @param  epsilon the tolerable error
   * @return <code>true</code> if the two arrays are equal with a tolerable
   *         error
   */
  public static boolean equals(double[] a, double[] a2, double epsilon) {
    if (a == a2) {
      return true;
    }
    if ((a == null) || (a2 == null)) {
      return false;
    }
    final int length = a.length;

    if (a2.length != length) {
      return false;
    }
    for (int i = 0; i < length; i++) {
      if (Math.abs(a[i] - a2[i]) > epsilon) {
        // re-check to account for NaN and Infinite
        if (Double.doubleToLongBits(a[i]) == Double.doubleToLongBits(a2[i])) {
          continue;
        }
        return false;
      }
    }
    return true;
  }

  /**
   * Returns <code>true</code> if the two specified arrays of floats are
   * <i>equal</i> to one another with a tolerable error. Two arrays are
   * considered equal if both arrays contain the same number of elements, and
   * all corresponding pairs of elements in the two arrays are equal with a
   * tolerable error. In other words, two arrays are equal if they contain the
   * same elements with a tolerable error in the same order. Also, two array
   * references are considered equal if both are <code>null</code>.
   *
   * @param  a one array to be tested for equality
   * @param  a2 the other array to be tested for equality
   * @param  epsilon the tolerable error
   * @return <code>true</code> if the two arrays are equal with a tolerable
   *         error
   */
  public static boolean equals(float[] a, float[] a2, double epsilon) {
    if (a == a2) {
      return true;
    }
    if ((a == null) || (a2 == null)) {
      return false;
    }
    final int length = a.length;

    if (a2.length != length) {
      return false;
    }
    for (int i = 0; i < length; i++) {
      if (Math.abs(a[i] - a2[i]) > epsilon) {
        // re-check to account for NaN and Infinite
        if (Float.floatToIntBits(a[i]) == Float.floatToIntBits(a2[i])) {
          continue;
        }
        return false;
      }
    }
    return true;
  }

  /**
   * Returns <code>true</code> if the two specified arrays of Objects are
   * <i>equal</i> to one another with a tolerable error. The two arrays are
   * considered equal if both arrays contain the same number of elements, and
   * all corresponding pairs of elements in the two arrays are equal with a
   * tolerable. In other words, the two arrays are equal if they contain the
   * same elements with a tolerable error in the same order. Also, two array
   * references are considered equal if both are <code>null</code>.
   *
   * @param  a one array to be tested for equality
   * @param  a2 the other array to be tested for equality
   * @param  epsilon the tolerable error
   * @return <code>true</code> if the two arrays are equal with a tolerable error
   */
  public static boolean equals(Object[] a, Object[] a2, double epsilon) {
    if (a == a2) {
      return true;
    }
    if ((a == null) || (a2 == null)) {
      return false;
    }
    final int length = a.length;

    if (a2.length != length) {
      return false;
    }
    for (int i = 0; i < length; i++) {
      if (!Objects.equals(a[i], a2[i], epsilon)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns <code>true</code> if the two specified arrays are <i>deeply
   * equal</i> to one another with a tolerable error. Unlike the
   * {@link Arrays#equals(Object[],Object[], double)} method, this method is
   * appropriate for use with nested arrays of arbitrary depth.
   * <p>
   * Two array references are considered deeply equal if both
   * are <code>null</code>, or if they refer to arrays that contain the same
   * number of elements and all corresponding pairs of elements in the two
   * arrays are deeply equal with a tolerable error.
   * <p>
   * If either of the specified arrays contain themselves as elements
   * either directly or indirectly through one or more levels of arrays,
   * the behavior of this method is undefined.
   *
   * @param  a1 one array to be tested for equality
   * @param  a2 the other array to be tested for equality
   * @param  epsilon the tolerable error
   * @return <code>true</code> if the two arrays are equal with a tolerable error
   */
  public static boolean deepEquals(Object[] a1, Object[] a2, double epsilon) {
    if (a1 == a2) {
      return true;
    }
    if ((a1 == null) || (a2 == null)) {
      return false;
    }
    final int length = a1.length;

    if (a2.length != length) {
      return false;
    }
    for (int i = 0; i < length; i++) {
      final Object e1 = a1[i];
      final Object e2 = a2[i];

      if (e1 == e2) {
        continue;
      }
      if (e1 == null) {
        return false;
      }
      if (!Arrays.deepEquals0(e1, e2, epsilon)) {
        return false;
      }
    }
    return true;
  }
}

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

import java.math.BigDecimal;
import java.math.BigInteger;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import org.helenus.lang.Tolerable;

/**
 * The <code>Objects</code> class extends on {@link java.util.Objects} to
 * provide support for {@link Tolerable} objects.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jul 20, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
public class Objects {
  /**
   * Returns <code>true</code> if the arguments are equal to each other while
   * ignoring case when comparing their elements and <code>false</code> otherwise.
   * <p>
   * Consequently, if both arguments are <code>null</code>, <code>true</code>
   * is returned and if exactly one argument is <code>null</code>,
   * <code>false</code> is returned. Otherwise, equality is determined by converting
   * each element to its lower case representation and comparing.
   * <p>
   * <i>Note:</i> Only sets and lists are currently supported.
   *
   * @author paouelle
   *
   * @param  a a collection of strings
   * @param  b another collection of strings to be compared with <code>a</code>
   *         for equality while ignoring case
   * @return <code>true</code> if the arguments are equal to each other while
   *         ignoring case when comparing their elements and <code>false</code>
   *         otherwise
   */
  public static boolean equalsIgnoreCase(
    Collection<String> a, Collection<String> b
  ) {
    if (a == b) {
      return true;
    } else if ((a instanceof Set) && (b instanceof Set)) {
      return java.util.Objects.equals(
        a.stream()
          .map(StringUtils::lowerCase)
          .collect(Collectors.toSet()),
        b.stream()
          .map(StringUtils::lowerCase)
          .collect(Collectors.toSet())
      );
    } else if ((a instanceof List) && (b instanceof List)) {
      return java.util.Objects.equals(
        a.stream()
          .map(StringUtils::lowerCase)
          .collect(Collectors.toList()),
        b.stream()
          .map(StringUtils::lowerCase)
          .collect(Collectors.toList())
      );
    }
    return false;
  }

  /**
   * Returns <code>true</code> if the arguments are equal to each other with
   * a tolerable error and <code>false</code> otherwise.
   * <p>
   * Consequently, if both arguments are <code>null</code>, <code>true</code>
   * is returned and if exactly one argument is <code>null</code>,
   * <code>false</code> is returned. Otherwise, equality is determined by using
   * the {@link Object#equals} method or the {@link Tolerable#equals} method of
   * the first argument.
   *
   * @author paouelle
   *
   * @param  a an object
   * @param  b an object to be compared with <code>a</code> for equality
   * @param  epsilon the tolerable error
   * @return <code>true</code> if the arguments are equal to each other with a
   *         tolerable error and <code>false</code> otherwise
   *
   * @see Object#equals(Object)
   * @see Tolerable#equals
   */
  public static boolean equals(Object a, Object b, double epsilon) {
    if (a == b) {
      return true;
    } else if (a instanceof Tolerable) {
      return ((Tolerable)a).equals(b, epsilon);
    } else if (a instanceof Optional) {
      if (b instanceof Optional) {
        final Object ao = ((Optional<?>)a).orElse(null);
        final Object bo = ((Optional<?>)b).orElse(null);

        return Objects.equals(ao, bo, epsilon);
      }
      return false;
    } else if ((a instanceof Float) || (a instanceof Double)) {
      if (!(a instanceof Number)) {
        return false;
      }
      final float af = ((Number)a).floatValue();
      final float bf = ((Number)b).floatValue();

      if (Math.abs(af - bf) > epsilon) {
        // re-check to account for NaN and Infinite
        return (Float.floatToIntBits(af) == Float.floatToIntBits(bf));
      }
      return true;
    } else if ((a instanceof Double) || (a instanceof Double)) {
      if (!(a instanceof Number)) {
        return false;
      }
      final double ad = ((Number)a).doubleValue();
      final double bd = ((Number)b).doubleValue();

      if (Math.abs(ad - bd) > epsilon) {
        // re-check to account for NaN and Infinite
        return (Double.doubleToLongBits(ad) == Double.doubleToLongBits(bd));
      }
      return true;
    } else if (a instanceof BigDecimal) {
      if (!(b instanceof Number)) {
        return false;
      }
      final BigDecimal bd;

      if (b instanceof BigDecimal) {
        bd = (BigDecimal)b;
      } else if (b instanceof BigInteger) {
        bd = new BigDecimal(((BigInteger)b));
      } else {
        bd = new BigDecimal(((Number)b).doubleValue());
      }
      return ((BigDecimal)a).subtract(bd).abs().compareTo(new BigDecimal(epsilon)) <= 0;
    }
    return (a != null) && a.equals(b);
  }

  /**
   * Returns <code>true</code> if the arguments are deeply equal with a tolerable
   * error to each other and <code>false</code> otherwise.
   * <p>
   * Two <code>null</code> values are deeply equal. If both arguments are
   * arrays, the algorithm in {@link Arrays#deepEquals(Object[], Object[], double)}
   * is used to determine equality. Otherwise, equality is determined by using
   * the {@link Object#equals} or {@link Tolerable#equals} method of the first
   * argument.
   *
   * @author paouelle
   *
   * @param  a an object
   * @param  b an object to be compared with <code>a</code> for deep equality
   * @param  epsilon the tolerable error
   * @return <code>true</code> if the arguments are deeply equal to each other
   *         with a tolerable error and <code>false</code> otherwise
   *
   * @see Arrays#deepEquals(Object[], Object[], double)
   * @see Objects#equals(Object, Object, double)
   */
   public static boolean deepEquals(Object a, Object b, double epsilon) {
     if (a == b) {
       return true;
     } else if ((a == null) || (b == null)) {
       return false;
     }
     return Arrays.deepEquals0(a, b, epsilon);
   }
}

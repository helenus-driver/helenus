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
package org.helenus.driver.persistence;

import java.lang.reflect.Field;

import java.io.IOException;

import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

/**
 * The <code>SetStringTokenizer</code> class provides an implementation for the
 * {@link Persister} interface that converts a set of strings into a string
 * where all elements of the set are separated by a separator string.
 * <p>
 * The <code>SetStringTokenizer</code> expects one argument configured on the
 * {@link Persisted} annotation. This argument is the separator string to be
 * used when joining and splitting strings.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 15, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
@SuppressWarnings("unchecked")
public class SetStringTokenizer implements Persister<Set<String>, String> {
  /**
   * Holds a dummy field of type Set<String>.
   *
   * @author paouelle
   */
  @SuppressWarnings("unused")
  private static final Set<String> SET = null;

  /**
   * Holds the the class for Set<String>.
   *
   * @author paouelle
   */
  private static final Class<Set<String>> SET_CLASS;

  static {
    // we use some magic to extract the class object for Set<String>
    try {
      final Field f = SetStringTokenizer.class.getDeclaredField("SET");

      SET_CLASS = (Class<Set<String>>)f.getType();
    } catch (Exception e) {
      throw new Error(e);
    }
  }

  /**
   * Holds the separator string.
   *
   * @author paouelle
   */
  private final String separator;

  /**
   * Instantiates a new <code>SetStringTokenizer</code> object.
   *
   * @author paouelle
   *
   * @param  args the list of arguments
   * @throws NullPointerException if <code>args</code> is <code>null</code>
   * @throws IllegalArgumentException if the arguments are not composed of a
   *         single argument
   */
  public SetStringTokenizer(String... args) {
    org.apache.commons.lang3.Validate.notNull(args, "invalid null args");
    org.apache.commons.lang3.Validate.isTrue(
      args.length == 1,
      "invalid number of arguments provided: %d",
      args.length
    );
    this.separator = args[0];
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.persistence.Persister#getDecodedClass()
   */
  @Override
  public Class<Set<String>> getDecodedClass() {
    return SetStringTokenizer.SET_CLASS;

  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.persistence.Persister#getPersistedClass()
   */
  @Override
  public Class<String> getPersistedClass() {
    return String.class;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.persistence.Persister#encode(java.lang.Object)
   */
  @Override
  public String encode(Set<String> set) {
    return StringUtils.join(set, separator);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.persistence.Persister#decode(java.lang.Object)
   */
  @Override
  public Set<String> decode(String s) throws IOException {
    final String[] as = StringUtils.splitByWholeSeparator(s, separator);

    if (as == null) {
      return null;
    }
    final Set<String> set = new LinkedHashSet<>(as.length); // to maintain found order

    for (final String se: as) {
      set.add(se);
    }
    return set;
  }
}

/*
 * Copyright (C) 2015-2017 The Helenus Driver Project Authors.
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
package org.helenus.driver.impl;

import java.util.List;
import java.util.Objects;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.TypeCodec;

import org.helenus.driver.Using;

/**
 * The <code>UsingImpl</code> class extends Cassandra's
 * {@link com.datastax.driver.core.querybuilder.Using} to provide support for
 * POJOs.
 *
 * @copyright 2015-2017 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 19, 2015 - paouelle - Creation
 *
 * @param <T> the type of the option
 *
 * @since 1.0
 */
public class UsingImpl<T>
  extends Utils.Appendeable
  implements Using<T> {
  /**
   * Holds the statement this option applies to. The value is set at the time
   * the option is added to a statement
   *
   * @author paouelle
   */
  private volatile StatementImpl<?, ?, ?> statement = null;

  /**
   * Holds the option name.
   *
   * @author paouelle
   */
  private final String optionName;

  /**
   * Holds the option's value.
   *
   * @author paouelle
   */
  private T value;

  /**
   * Instantiates a new <code>UsingImpl</code> object.
   *
   * @author paouelle
   *
   * @param optionName the option name
   * @param value the option's value
   */
  UsingImpl(String optionName, T value) {
    this.optionName = optionName;
    this.value = value;
  }

  /**
   * Instantiates a new <code>UsingImpl</code> object.
   *
   * @author paouelle
   *
   * @param using the option we are copying
   * @param statement the statement we are assigning this new option to
   */
  UsingImpl(UsingImpl<T> using, StatementImpl<?, ?, ?> statement) {
    this.optionName = using.optionName;
    this.value = using.value;
    this.statement = statement;
  }

  /**
   * Associate this option to a statement if not already associated.
   *
   * @author paouelle
   *
   * @param  statement the statement to associate with this option
   * @return this option if it was not already associate with a statement or a
   *         copy if it was
   */
  UsingImpl<T> setStatement(StatementImpl<?, ?, ?> statement) {
    if (this.statement == null) {
      this.statement = statement;
      return this;
    }
    // clone these options since they are already assigned to another statement
    return new UsingImpl<>(this, statement);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.Utils.Appendeable#appendTo(org.helenus.driver.impl.TableInfoImpl, com.datastax.driver.core.TypeCodec, com.datastax.driver.core.CodecRegistry, java.lang.StringBuilder, java.util.List)
   */
  @Override
  void appendTo(
    TableInfoImpl<?> tinfo,
    TypeCodec<?> codec,
    CodecRegistry codecRegistry,
    StringBuilder sb,
    List<Object> variables
  ) {
    sb.append(optionName).append(' ').append(value);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.Utils.Appendeable#containsBindMarker()
   */
  @Override
  boolean containsBindMarker() {
    return Utils.containsBindMarker(value);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.Using#getName()
   */
  @Override
  public String getName() {
    return optionName;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.Using#getValue()
   */
  @Override
  public T getValue() {
    return value;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.Using#setValue(java.lang.Object)
   */
  @Override
  public void setValue(T value) {
    org.apache.commons.lang3.Validate.notNull(value, "invalid null value");
    if (!Objects.equals(this.value, value)) {
      this.value = value;
      final StatementImpl<?, ?, ?> s = statement;

      if (s != null) { // dirty the statement now that we changed an option
        s.setDirty();
      }
    }
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    return 31 ^ optionName.hashCode() ^ value.hashCode();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj instanceof UsingImpl) {
      final UsingImpl<?> u = (UsingImpl<?>)obj;

      return value.equals(u.getValue()) && optionName.equals(u.getName());
    }
    return false;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return optionName + "=" + value;
  }
}

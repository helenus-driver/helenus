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
package org.helenus.driver.codecs.provider;

import java.util.Locale;

import java.time.DateTimeException;
import java.time.ZoneId;

import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.CodecNotFoundException;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.driver.extras.codecs.ParsingCodec;
import com.datastax.driver.extras.codecs.enums.EnumNameCodec;
import com.google.common.reflect.TypeToken;

import org.helenus.commons.lang3.LocaleUtils;
import org.helenus.driver.persistence.DataType;

/**
 * The <code>AsciiCodecProvider</code> class defines a provider suitable to
 * decode the {@link DataType#ASCII} data type.
 *
 * @copyright 2015-2017 The Helenus Driver Project Authors
 *
 * @author The Helenus Driver Project Authors
 * @version 1 - Jan 9, 2017 - paouelle - Creation
 *
 * @since 3.0
 *
 * @see com.datastax.driver.core.CodecRegistry
 */
public final class AsciiCodecProvider implements CodecProvider {
  /**
   * Holds the single instance of this provider.
   *
   * @author paouelle
   */
  public final static CodecProvider INSTANCE = new AsciiCodecProvider();

  /**
   * Gets a codec to decode {@link Locale} objects.
   *
   * @author paouelle
   *
   * @return a non-<code>null</code> codec to decode {@link Locale} objects
   */
  @SuppressWarnings("synthetic-access")
  public static TypeCodec<Locale> locale() {
    return LocaleCodec.instance;
  }

  /**
   * Gets a codec to decode {@link ZoneId} objects.
   *
   * @author paouelle
   *
   * @return a non-<code>null</code> codec to decode {@link ZoneId} objects
   */
  @SuppressWarnings("synthetic-access")
  public static TypeCodec<ZoneId> zoneId() {
    return ZoneIdCodec.instance;
  }

  /**
   * Instantiates a new <code>AsciiCodecProvider</code> object.
   *
   * @author paouelle
   */
  private AsciiCodecProvider() {}

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.codecs.provider.CodecProvider#codecFor(java.lang.Class)
   */
  @Override
  @SuppressWarnings({"rawtypes", "unchecked"})
  public <T> TypeCodec<T> codecFor(Class<T> clazz)
    throws CodecNotFoundException {
    if (clazz.isEnum()) {
      return new EnumNameCodec(TypeCodec.ascii(), clazz);
    } else if (Class.class.isAssignableFrom(clazz)) {
      return new ClassCodec(clazz);
    } else if (Locale.class.isAssignableFrom(clazz)) {
      return (TypeCodec<T>)AsciiCodecProvider.locale();
    } else if (ZoneId.class.isAssignableFrom(clazz)) {
      return (TypeCodec<T>)AsciiCodecProvider.zoneId();
    } else if (String.class == clazz) {
      return (TypeCodec<T>)TypeCodec.ascii();
    }
    throw new CodecNotFoundException(
      "unsupported Helenus codec from '"
      + DataType.ASCII.toCQL()
      + "' to class: "
      + clazz.getName(),
      com.datastax.driver.core.DataType.ascii(),
      TypeToken.of(clazz)
    );
  }

  /**
   * The <code>ClassCodec</code> class defines a type codec for {@link Class}
   * objects.
   *
   * @copyright 2015-2017 The Helenus Driver Project Authors
   *
   * @author The Helenus Driver Project Authors
   * @version 1 - Jan 9, 2017 - paouelle - Creation
   *
   * @param <T> the Java type to decode to its class
   *
   * @since 3.0
   */
  private static class ClassCodec<T> extends ParsingCodec<Class<T>> {
    /**
     * Instantiates a new <code>ClassCodec</code> object.
     *
     * @author paouelle
     *
     * @param javaType the java type to decode to
     */
    ClassCodec(Class<Class<T>> javaType) {
      super(TypeCodec.ascii(), javaType);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.datastax.driver.extras.codecs.ParsingCodec#toString(java.lang.Object)
     */
    @Override
    protected String toString(Class<T> value) {
      return (value != null) ? value.getName() : null;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.datastax.driver.extras.codecs.ParsingCodec#fromString(java.lang.String)
     */
    @SuppressWarnings("unchecked")
    @Override
    protected Class<T> fromString(String value) {
      try {
        return (value != null) ? (Class<T>)Class.forName(value) : null;
      } catch (ClassNotFoundException e) {
        throw new InvalidTypeException(
          "cannot parse class value from \"" + value + '"', e
        );
      }
    }
  }

  /**
   * The <code>LocaleCodec</code> class defines a type codec for {@link Locale}
   * objects.
   *
   * @copyright 2015-2017 The Helenus Driver Project Authors
   *
   * @author The Helenus Driver Project Authors
   * @version 1 - Jan 9, 2017 - paouelle - Creation
   *
   * @since 3.0
   */
  private static class LocaleCodec extends ParsingCodec<Locale> {
    /**
     * Holds the instance for this codec.
     *
     * @author paouelle
     */
    private static LocaleCodec instance = new LocaleCodec();

    /**
     * Instantiates a new <code>LocaleCodec</code> object.
     *
     * @author paouelle
     */
    private LocaleCodec() {
      super(TypeCodec.ascii(), Locale.class);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.datastax.driver.extras.codecs.ParsingCodec#toString(java.lang.Object)
     */
    @Override
    protected String toString(Locale value) {
      return (value != null) ? value.toString() : null;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.datastax.driver.extras.codecs.ParsingCodec#fromString(java.lang.String)
     */
    @Override
    protected Locale fromString(String value) {
      try {
        return (value != null) ? LocaleUtils.toLocale(value) : null;
      } catch (IllegalArgumentException e) {
        throw new InvalidTypeException(
          "cannot parse locale value from \"" + value + '"', e
        );
      }
    }
  }

  /**
   * The <code>ZoneIdCodec</code> class defines a type codec for {@link ZoneId}
   * objects.
   *
   * @copyright 2015-2017 The Helenus Driver Project Authors
   *
   * @author The Helenus Driver Project Authors
   * @version 1 - Jan 9, 2017 - paouelle - Creation
   *
   * @since 3.0
   */
  private static class ZoneIdCodec extends ParsingCodec<ZoneId> {
    /**
     * Holds the instance for this codec.
     *
     * @author paouelle
     */
    private static ZoneIdCodec instance = new ZoneIdCodec();

    /**
     * Instantiates a new <code>ZoneIdCodec</code> object.
     *
     * @author paouelle
     */
    private ZoneIdCodec() {
      super(TypeCodec.ascii(), ZoneId.class);
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.datastax.driver.extras.codecs.ParsingCodec#toString(java.lang.Object)
     */
    @Override
    protected String toString(ZoneId value) {
      return (value != null) ? value.getId() : null;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see com.datastax.driver.extras.codecs.ParsingCodec#fromString(java.lang.String)
     */
    @Override
    protected ZoneId fromString(String value) {
      try {
        return (value != null) ? ZoneId.of(value) : null;
      } catch (DateTimeException e) {
        throw new InvalidTypeException(
          "cannot parse zone id from \"" + value + '"', e
        );
      }
    }
  }
}

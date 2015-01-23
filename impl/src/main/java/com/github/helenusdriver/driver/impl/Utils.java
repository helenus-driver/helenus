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
package com.github.helenusdriver.driver.impl;

import java.net.InetAddress;

import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZoneId;

import javax.json.JsonObject;

import org.apache.commons.collections4.iterators.ObjectArrayIterator;

import com.datastax.driver.core.utils.Bytes;

/**
 * The <code>Utils</code> class is a copy of the
 * {@link com.datastax.driver.core.querybuilder.Utils} which is package private
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 19, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
@SuppressWarnings("javadoc")
public abstract class Utils {
  private static final Pattern cnamePattern = Pattern
    .compile("\\w+(?:\\[.+\\])?");

  public static StringBuilder joinAndAppendNamesAndValues(
    StringBuilder sb,
    String separator,
    String keySeparator,
    Map<?, ?> mappings
  ) {
    boolean first = true;

    for (final Map.Entry<?, ?> e: mappings.entrySet()) {
      if (!first) {
        sb.append(separator);
      } else {
        first = false;
      }
      Utils.appendName(e.getKey(), sb).append(keySeparator);
      Utils.appendValue(e.getValue(), sb);
    }
    return sb;
  }
  public static StringBuilder joinAndAppend(
    TableInfoImpl<?> tinfo,
    StringBuilder sb,
    String separator,
    Collection<? extends Appendeable> values) {
    boolean first = true;

    for (final Appendeable value: values) {
      if (!first) {
        sb.append(separator);
      } else {
        first = false;
      }
      value.appendTo(tinfo, sb);
    }
    return sb;
  }

  public static StringBuilder joinAndAppendNames(
    StringBuilder sb,
    String separator,
    Iterable<?> values) {
    boolean first = true;

    for (final Object value: values) {
      if (!first) {
        sb.append(separator);
      } else {
        first = false;
      }
      appendName(value, sb);
    }
    return sb;
  }

  public static StringBuilder joinAndAppendValues(
    StringBuilder sb,
    String separator,
    Iterable<?> values) {
    boolean first = true;

    for (final Object value: values) {
      if (!first) {
        sb.append(separator);
      } else {
        first = false;
      }
      appendValue(value, sb);
    }
    return sb;
  }

  public static StringBuilder appendValue(Object value, StringBuilder sb) {
    return appendValue(value, sb, false);
  }

  public static StringBuilder appendFlatValue(Object value, StringBuilder sb) {
    appendFlatValue(value, sb, false);
    return sb;
  }

  private static StringBuilder appendValue(
    Object value,
    StringBuilder sb,
    boolean rawValue
  ) {
    if (value instanceof PersistedValue) {
      value = ((PersistedValue<?,?>)value).getEncodedValue();
    }
    // That is kind of lame but lacking a better solution
    if (appendValueIfLiteral(value, sb)) {
      return sb;
    }
    if (appendValueIfCollection(value, sb, rawValue)) {
      return sb;
    }
    appendStringIfValid(value, sb, rawValue);
    return sb;
  }

  private static void appendFlatValue(
    Object value,
    StringBuilder sb,
    boolean rawValue) {
    if (value instanceof PersistedValue) {
      value = ((PersistedValue<?,?>)value).getEncodedValue();
    }
    if (appendValueIfLiteral(value, sb)) {
      return;
    }

    appendStringIfValid(value, sb, rawValue);
  }

  private static void appendStringIfValid(
    Object value,
    StringBuilder sb,
    boolean rawValue) {
    if (value instanceof Enum) {
      value = ((Enum<?>)value).name();
    } else if (value instanceof Locale) {
      value = ((Locale)value).toString();
    } else if (value instanceof ZoneId) {
      value = ((ZoneId)value).getId();
    } else if (value instanceof Instant) {
      value = String.valueOf(((Instant)value).toEpochMilli());
    } else if (value instanceof Class) {
      value = ((Class<?>)value).getName();
    }
    if (value instanceof RawString) {
      sb.append(value.toString());
    } else {
      if (!(value instanceof String)) {
        String msg =
          String.format(
            "Invalid value %s of type unknown to the query builder",
            value);
        if (value instanceof byte[]) {
          msg += " (for blob values, make sure to use a ByteBuffer)";
        }
        throw new IllegalArgumentException(msg);
      }
      if (rawValue) {
        sb.append((String)value);
      } else {
        appendValueString((String)value, sb);
      }
    }
  }

  @SuppressWarnings("synthetic-access")
  private static boolean appendValueIfLiteral(Object value, StringBuilder sb) {
    if ((value instanceof Number)
        || (value instanceof UUID)
        || (value instanceof Boolean)) {
      sb.append(value);
      return true;
    } else if (value instanceof InetAddress) {
      sb.append("'").append(((InetAddress)value).getHostAddress()).append("'");
      return true;
    } else if (value instanceof Date) {
      sb.append(((Date)value).getTime());
      return true;
    } else if (value instanceof ByteBuffer) {
      sb.append(Bytes.toHexString((ByteBuffer)value));
      return true;
    } else if (value instanceof byte[]) {
      sb.append(Bytes.toHexString((byte[])value));
      return true;
    } else if (value == StatementManagerImpl.BIND_MARKER) {
      sb.append("?");
      return true;
    } else if (value instanceof FCall) {
      FCall fcall = (FCall)value;
      sb.append(fcall.getName()).append("(");
      for (int i = 0; i < fcall.parameters.length; i++ ) {
        if (i > 0) {
          sb.append(",");
        }
        appendValue(fcall.parameters[i], sb);
      }
      sb.append(")");
      return true;
    } else if (value instanceof CName) {
      appendName(((CName)value).getName(), sb);
      return true;
    } else if (value instanceof JsonObject) {
      // convert all ' in " and vice versa as Cassandra doesn't like ""
      sb.append(
        value.toString()
          .replaceAll("'", "'QUOTE'")
          .replaceAll("\"", "'")
          .replaceAll("\\\\'", "\"")
          .replaceAll("'QUOTE'", "\\\\'")
      );
      return true;
    } else if (value == null) {
      sb.append("null");
      return true;
    } else {
      return false;
    }
  }

  @SuppressWarnings("rawtypes")
  private static boolean appendValueIfCollection(
    Object value,
    StringBuilder sb,
    boolean rawValue) {
    if (value instanceof List) {
      appendList((List)value, sb, rawValue);
      return true;
    } else if (value instanceof Set) {
      appendSet((Set)value, sb, rawValue);
      return true;
    } else if (value instanceof Map) {
      appendMap((Map)value, sb, rawValue);
      return true;
    } else {
      return false;
    }
  }

  static StringBuilder appendCollection(Object value, StringBuilder sb) {
    boolean wasCollection = appendValueIfCollection(value, sb, false);
    assert wasCollection;
    return sb;
  }

  static StringBuilder appendList(List<?> l, StringBuilder sb) {
    return appendList(l, sb, false);
  }

  private static StringBuilder appendList(
    List<?> l,
    StringBuilder sb,
    boolean rawValue) {
    sb.append("[");
    if (l instanceof PersistedList) {
      l = ((PersistedList<?,?>)l).getPersistedList();
    }
    for (int i = 0; i < l.size(); i++ ) {
      final Object elt = l.get(i);

      org.apache.commons.lang3.Validate.isTrue(
        elt != null, "null are not supported in lists"
      );
      if (i > 0) {
        sb.append(",");
      }
      appendFlatValue(elt, sb, rawValue);
    }
    sb.append("]");
    return sb;
  }

  static StringBuilder appendSet(Set<?> s, StringBuilder sb) {
    return appendSet(s, sb, false);
  }

  private static StringBuilder appendSet(
    Set<?> s,
    StringBuilder sb,
    boolean rawValue) {
    sb.append("{");
    boolean first = true;
    if (s instanceof PersistedSet) {
      s = ((PersistedSet<?,?>)s).getPersistedSet();
    }
    for (Object elt : s) {
      org.apache.commons.lang3.Validate.isTrue(
        elt != null, "null are not supported in sets"
      );
      if (first) {
        first = false;
      } else {
        sb.append(",");
      }
      appendFlatValue(elt, sb, rawValue);
    }
    sb.append("}");
    return sb;
  }

  static StringBuilder appendMap(Map<?, ?> m, StringBuilder sb) {
    return appendMap(m, sb, false);
  }

  private static StringBuilder appendMap(
    Map<?, ?> m,
    StringBuilder sb,
    boolean rawValue) {
    sb.append("{");
    boolean first = true;
    if (m instanceof PersistedMap) {
      m = ((PersistedMap<?,?,?>)m).getPersistedMap();
    }
    for (Map.Entry<?, ?> entry : m.entrySet()) {
      final Object eval = entry.getValue();

      org.apache.commons.lang3.Validate.isTrue(
        eval != null, "null are not supported in maps"
      );
      if (first) {
        first = false;
      } else {
        sb.append(",");
      }
      appendFlatValue(entry.getKey(), sb, rawValue);
      sb.append(":");
      appendFlatValue(eval, sb, rawValue);
    }
    sb.append("}");
    return sb;
  }

  private static StringBuilder appendValueString(String value, StringBuilder sb) {
    return sb.append("'").append(replace(value, '\'', "''")).append("'");
  }

  static boolean isRawValue(Object value) {
    return value != null
           && !(value instanceof FCall)
           && !(value instanceof CName)
           && value != StatementManagerImpl.BIND_MARKER;
  }

  static String toRawString(Object value) {
    return appendValue(value, new StringBuilder(), true).toString();
  }

  public static StringBuilder appendName(String name, StringBuilder sb) {
    name = name.trim();
    // FIXME: checking for token( specifically is uber ugly, we'll need some
    // better solution.
    if (cnamePattern.matcher(name).matches()
        || name.startsWith("\"") || name.startsWith("token(")) {
      sb.append(name);
    } else {
      sb.append("\"").append(name).append("\"");
    }
    return sb;
  }

  @SuppressWarnings("synthetic-access")
  static StringBuilder appendName(Object name, StringBuilder sb) {
    if (name instanceof String) {
      appendName((String)name, sb);
    } else if (name instanceof CName) {
      appendName(((CName)name).getName(), sb);
    } else if (name instanceof FCall) {
      FCall fcall = (FCall)name;
      sb.append(fcall.getName()).append("(");
      for (int i = 0; i < fcall.parameters.length; i++ ) {
        if (i > 0) {
          sb.append(",");
        }
        appendValue(fcall.parameters[i], sb);
      }
      sb.append(")");
    } else {
      appendName((String)name, sb);
    }
    return sb;
  }

  static abstract class Appendeable {
    abstract void appendTo(TableInfoImpl<?> tinfo, StringBuilder sb);
  }

  // Simple method to replace a single character. String.replace is a bit too
  // inefficient (see JAVA-67)
  static String replace(String text, char search, String replacement) {
    if (text == null || text.isEmpty()) {
      return text;
    }

    int nbMatch = 0;
    int start = -1;
    do {
      start = text.indexOf(search, start + 1);
      if (start != -1) {
        ++nbMatch;
      }
    } while (start != -1);

    if (nbMatch == 0) {
      return text;
    }

    int newLength = text.length() + nbMatch * (replacement.length() - 1);
    char[] result = new char[newLength];
    int newIdx = 0;
    for (int i = 0; i < text.length(); i++ ) {
      char c = text.charAt(i);
      if (c == search) {
        for (int r = 0; r < replacement.length(); r++ ) {
          result[newIdx++ ] = replacement.charAt(r);
        }
      } else {
        result[newIdx++ ] = c;
      }
    }
    return new String(result);
  }

  public static class RawString {
    private final String str;

    public RawString(String str) {
      this.str = str;
    }

    @Override
    public String toString() {
      return str;
    }
  }

  static abstract class FCName {
    private final String name;

    FCName(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }
  }

  public static class FCall extends FCName implements Iterable<Object> {
    private final Object[] parameters;

    public FCall(String name, Object... parameters) {
      super(name);
      org.apache.commons.lang3.Validate.notNull(name, "invalid null operation name");
      this.parameters = parameters;
    }

    @Override
    @SuppressWarnings({"cast", "unchecked"})
    public Iterator<Object> iterator() {
      return new ObjectArrayIterator<>(parameters);
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(getName()).append("(");
      for (int i = 0; i < parameters.length; i++ ) {
        if (i > 0) {
          sb.append(",");
        }
        sb.append(parameters[i]);
      }
      sb.append(")");
      return sb.toString();
    }
  }

  public static class CName extends FCName {
    public CName(String name) {
      super(name);
      org.apache.commons.lang3.Validate.notNull(name, "invalid null column name");
    }

    public String getColumnName() {
      return getName();
    }

    @Override
    public String toString() {
      return getName();
    }
  }

  public static class CNameIndex extends CName {
    private final int idx;

    public CNameIndex(String name, int idx) {
      super(name);
      if (idx < 0) {
        throw new ArrayIndexOutOfBoundsException(
          "invalid index '" + idx + "' for column '" + name + "'"
        );
      }
      this.idx = idx;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder();

      Utils.appendName(getColumnName(), sb);
      return sb.append("[").append(idx).append("]").toString();
    }
  }

  public static class CNameKey extends CName {
    private final Object key;

    public CNameKey(String name, Object key) {
      super(name);
      this.key = key;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder();

      Utils.appendName(getColumnName(), sb);
      sb.append("[");
      Utils.appendFlatValue(key, sb);
      return sb.append("]").toString();
    }
  }

  public static class CNameSequence implements CharSequence {
    private final String[] names;
    private final String seq;

    CNameSequence(String seq, String... names) {
      this.names = names;
      this.seq = seq;
    }

    public String getName() {
      return names[0];
    }

    public String[] getNames() {
      return names;
    }

    @Override
    public int length() {
      return seq.length();
    }

    @Override
    public char charAt(int index) {
      return seq.charAt(index);
    }

    @Override
    public CharSequence subSequence(int start, int end) {
      return seq.subSequence(start, end);
    }

    @Override
    public String toString() {
      return seq;
    }
  }
}
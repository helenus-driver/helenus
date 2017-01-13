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

import java.math.BigDecimal;
import java.math.BigInteger;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.collections4.iterators.ObjectArrayIterator;
import org.apache.commons.lang3.tuple.Triple;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.TypeCodec;

import org.helenus.driver.BindMarker;
import org.helenus.driver.Keywords;
import org.helenus.driver.codecs.ArgumentsCodec;
import org.helenus.driver.persistence.CQLDataType;
import org.helenus.driver.persistence.DataType;

/**
 * The <code>Utils</code> class is a copy of the
 * <i>com.datastax.driver.core.querybuilder.Utils</i> which is package private
 *
 * @copyright 2015-2017 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 19, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
@SuppressWarnings("javadoc")
public abstract class Utils {
  private static final Pattern cnamePattern = Pattern.compile(
    "\\w+(?:\\[.+\\])?"
  );

  public static StringBuilder joinAndAppendNamesAndValues(
    TypeCodec<?> codec,
    CodecRegistry codecRegistry,
    StringBuilder sb,
    String separator,
    String keySeparator,
    Map<String, Triple<Object, CQLDataType, TypeCodec<?>>> mappings,
    List<Object> variables
  ) {
    boolean first = true;

    for (final Map.Entry<String, Triple<Object, CQLDataType, TypeCodec<?>>> e: mappings.entrySet()) {
      if (!first) {
        sb.append(separator);
      } else {
        first = false;
      }
      Utils.appendName(sb, e.getKey()).append(keySeparator);
      Utils.appendValue(
        e.getValue().getMiddle(),
        ((codec != null) ? codec : e.getValue().getRight()),
        codecRegistry,
        sb,
        e.getValue().getLeft(),
        variables
      );
    }
    return sb;
  }

  public static StringBuilder joinAndAppendWithNoDuplicates(
    TableInfoImpl<?> tinfo,
    TypeCodec<?> codec,
    CodecRegistry codecRegistry,
    StringBuilder sb,
    String separator,
    Collection<? extends Appendeable> values,
    List<Object> variables
  ) {
    final Set<String> done = new HashSet<>(values.size() * 3 / 2);
    boolean first = true;

    for (final Appendeable value: values) {
      final StringBuilder vsb = new StringBuilder(10);

      value.appendTo(tinfo, codec, codecRegistry, vsb, variables);
      final String vs = vsb.toString();

      if (done.add(vs)) {
        if (!first) {
          sb.append(separator);
        } else {
          first = false;
        }
        sb.append(vs);
      }
    }
    return sb;
  }

  public static StringBuilder joinAndAppend(
    TableInfoImpl<?> tinfo,
    TypeCodec<?> codec,
    CodecRegistry codecRegistry,
    StringBuilder sb,
    String separator,
    Collection<? extends Appendeable> values,
    List<Object> variables
  ) {
    boolean first = true;

    for (final Appendeable value: values) {
      if (!first) {
        sb.append(separator);
      } else {
        first = false;
      }
      value.appendTo(tinfo, codec, codecRegistry, sb, variables);
    }
    return sb;
  }

  public static StringBuilder joinAndAppend(
    TableInfoImpl<?> tinfo,
    TypeCodec<?> codec,
    CodecRegistry codecRegistry,
    StringBuilder sb,
    String separator,
    Collection<? extends Appendeable> values,
    Collection<? extends Appendeable> moreValues,
    List<Object> variables
  ) {
    boolean first = true;

    for (final Appendeable value: values) {
      if (!first) {
        sb.append(separator);
      } else {
        first = false;
      }
      value.appendTo(tinfo, codec, codecRegistry, sb, variables);
    }
    for (final Appendeable value: moreValues) {
      if (!first) {
        sb.append(separator);
      } else {
        first = false;
      }
      value.appendTo(tinfo, codec, codecRegistry, sb, variables);
    }
    return sb;
  }

  public static StringBuilder joinAndAppendNames(
    TableInfoImpl<?> tinfo,
    TypeCodec<?> codec,
    CodecRegistry codecRegistry,
    StringBuilder sb,
    String separator,
    Iterable<?> values
  ) {
    boolean first = true;

    for (final Object value: values) {
      if (!first) {
        sb.append(separator);
      } else {
        first = false;
      }
      Utils.appendName(tinfo, codec, codecRegistry, sb, value);
    }
    return sb;
  }

  public static StringBuilder joinAndAppendValues(
    TypeCodec<?> codec,
    CodecRegistry codecRegistry,
    StringBuilder sb,
    String separator,
    Iterable<Triple<Object, CQLDataType, TypeCodec<?>>> values,
    List<Object> variables
  ) {
    boolean first = true;

    for (final Triple<Object, CQLDataType, TypeCodec<?>> value: values) {
      if (!first) {
        sb.append(separator);
      } else {
        first = false;
      }
      Utils.appendValue(
        value.getMiddle(),
        (codec != null) ? codec : value.getRight(),
        codecRegistry,
        sb,
        value.getLeft(),
        variables
      );
    }
    return sb;
  }

  public static StringBuilder joinAndAppendValues(
    TypeCodec<?> codec,
    CodecRegistry codecRegistry,
    StringBuilder sb,
    String separator,
    Iterable<?> values,
    CQLDataType definition,
    List<Object> variables
  ) {
    boolean first = true;

    for (final Object value: values) {
      if (!first) {
        sb.append(separator);
      } else {
        first = false;
      }
      Utils.appendValue(definition, codec, codecRegistry, sb, value, variables);
    }
    return sb;
  }

  public static boolean isBindMarker(Object value) {
    return (
      (value instanceof BindMarker)
      || (value instanceof com.datastax.driver.core.querybuilder.BindMarker)
    );
  }

  @SuppressWarnings("synthetic-access")
  public static boolean containsBindMarker(Object value) {
      if (Utils.isBindMarker(value)) {
        return true;
      } else if (value instanceof FCall) {
        for (final Object p : ((FCall)value).parameters) {
          if (Utils.containsBindMarker(p)) {
            return true;
          }
        }
      } else if (value instanceof Collection) {
        return (((Collection<?>)value).stream())
          .anyMatch(e -> Utils.containsBindMarker(value));
      } else if (value instanceof Map) {
        return ((Map<?, ?>)value).entrySet().stream()
          .anyMatch(e -> Utils.containsBindMarker(e.getKey())
                         || Utils.containsBindMarker(e.getValue()));
      }
      return false;
  }

  public static boolean containsSpecialValue(Object value) {
    if (Utils.isBindMarker(value)
        || (value instanceof FCName)
        || (value instanceof RawString)) {
        return true;
    }
    if (value instanceof Collection) {
      return ((Collection<?>)value).stream()
        .anyMatch(e -> Utils.containsSpecialValue(e));
    }
    if (value instanceof Map) {
      return ((Map<?, ?>)value).entrySet().stream()
        .anyMatch(e -> Utils.containsSpecialValue(e.getKey())
                       || Utils.containsSpecialValue(e.getValue()));
    }
    return false;
}

  /**
   * Return <code>true</code> if the given value is likely to find a suitable
   * codec to be serialized as a query parameter. If the value is not serializable,
   * it must be included in the query string. Non serializable values include
   * special values such as function calls, column names and bind markers, and
   * collections thereof. We also don't serialize fixed size number types. The
   * reason is that if we do it, we will force a particular size (4 bytes for
   * ints, ...) and for the query builder, we don't want users to have to bother
   * with that.
   *
   * @author paouelle
   *
   * @param  value the value to inspect
   * @return <code>true</code> if the value is serializable, <code>false</code>
   *         otherwise
   */
  public static boolean isSerializable(Object value) {
    if (Utils.containsSpecialValue(value)
        || ((value instanceof Number)
            && !((value instanceof BigInteger) || (value instanceof BigDecimal)))) {
      return false;
    } else if (value instanceof Collection) {
      return ((Collection<?>)value).stream()
        .allMatch(e -> Utils.isSerializable(e));
    } else if (value instanceof Map) {
      return ((Map<?, ?>)value).entrySet().stream()
        .allMatch(e -> Utils.isSerializable(e.getKey())
                       && Utils.isSerializable(e.getValue()));
    }
    return true;
  }

  public static boolean isIdempotent(Object value) {
    if (value == null) {
      return true;
    } else if (value instanceof AssignmentImpl) {
      return ((AssignmentImpl)value).isIdempotent();
    } else if (value instanceof FCall) {
      return false;
    } else if (value instanceof RawString) {
      return false;
    } else if (value instanceof Collection) {
      return ((Collection<?>)value).stream()
        .allMatch(v -> Utils.isIdempotent(v));
    } else if (value instanceof Map) {
      return ((Map<?, ?>)value).entrySet().stream()
        .allMatch(e -> Utils.isIdempotent(e.getKey())
                       && Utils.isIdempotent(e.getValue()));
    } else if (value instanceof ClauseImpl) {
      return ((ClauseImpl)value).values().stream()
        .allMatch(v -> Utils.isIdempotent(v));
    }
    return true;
  }

  @SuppressWarnings({
    "synthetic-access", "unchecked"
  })
  static StringBuilder appendValue(
    CQLDataType definition,
    TypeCodec<?> codec,
    CodecRegistry codecRegistry,
    StringBuilder sb,
    Object value,
    List<Object> variables
  ) {
    if (value == null) {
      sb.append("null");
    } else if (Utils.isBindMarker(value)) {
      sb.append(value);
    } else if (value instanceof FCall) {
      final FCall fcall = (FCall)value;

      sb.append(fcall.getName()).append("(");
      for (int i = 0; i < fcall.parameters.length; i++ ) {
        if (i > 0) {
          sb.append(",");
        }
        Utils.appendValue(
          null, codec, codecRegistry, sb, fcall.parameters[i], variables
        );
      }
      sb.append(")");
    } else if (value instanceof FCName) {
      Utils.appendName(null, codec, codecRegistry, sb, ((FCName)value).getName());
    } else if (value instanceof Cast) {
      final Cast c = (Cast)value;

      sb.append("CAST(");
      Utils.appendName(null, codec, codecRegistry, sb, c.column);
      sb.append(" AS ").append(c.targetType).append(')');
    } else if (value instanceof RawString) {
      sb.append(value.toString());
    } else {
      final boolean serializable = isSerializable(value);

      if (!serializable) {
        if ((value instanceof List)) {
          Utils.appendList(
            definition, codec, codecRegistry, sb, (List<?>)value, null
          );
        } else if ((definition != null)
                   && ((definition.getMainType() == DataType.ORDERED_SET)
                       || (definition.getMainType() == DataType.FROZEN_ORDERED_SET))
                   && (value instanceof Collection)) {
          Utils.appendList(
            definition, codec, codecRegistry, sb, (Collection<?>)value, null
          );
        } else if (value instanceof Set) {
          Utils.appendSet(
            definition, codec, codecRegistry, sb, (Set<?>)value, null
          );
        } else if (value instanceof Map) {
          Utils.appendMap(
            definition, codec, codecRegistry, sb, (Map<?, ?>)value, null
          );
        } else {
          // the value is meant to be forcefully appended to the query string:
          // format it with the appropriate codec and append it now
          if (codec != null) {
            sb.append(((TypeCodec<Object>)codec).format(value));
          } else {
            sb.append(codecRegistry.codecFor(value).format(value));
          }
        }
      } else if (variables == null) {
        // we are not collecting statement values (variables == null):
        // format it with the appropriate codec and append it now
        if (codec != null) {
          sb.append(((TypeCodec<Object>)codec).format(value));
        } else {
          sb.append(codecRegistry.codecFor(value).format(value));
        }
      } else {
        // do not format the value nor append it to the query string:
        // use a bind marker instead,
        // but add the value the the statement's variables list
        sb.append('?');
        variables.add(value);
      }
    }
    return sb;
  }

  static StringBuilder appendList(
    CQLDataType definition,
    TypeCodec<?> codec,
    CodecRegistry codecRegistry,
    StringBuilder sb,
    Collection<?> l,
    List<Object> variables
  ) {
    final CQLDataType vdef = definition.getElementType();
    final TypeCodec<?> ecodec = (codec instanceof ArgumentsCodec) ? ((ArgumentsCodec<?>)codec).codec(0) : codec;

    sb.append('[');
    boolean first = true;

    for (final Object elt: l) {
      if (elt == null) {
        throw new IllegalArgumentException(
          "null are not supported in "
          + ((l instanceof List)
             ? "lists" : ((l instanceof Set) ? "sets" : "collections"))
        );
      }
      if (first) {
        first = false;
      } else {
        sb.append(',');
      }
      Utils.appendValue(vdef, ecodec, codecRegistry, sb, elt, variables);
    }
    sb.append(']');
    return sb;
  }

  static StringBuilder appendSet(
    CQLDataType definition,
    TypeCodec<?> codec,
    CodecRegistry codecRegistry,
    StringBuilder sb,
    Set<?> s,
    List<Object> variables
  ) {
    final CQLDataType vdef = definition.getElementType();
    final TypeCodec<?> ecodec = (codec instanceof ArgumentsCodec) ? ((ArgumentsCodec<?>)codec).codec(0) : codec;

    sb.append('{');
    boolean first = true;

    for (final Object elt: s) {
      org.apache.commons.lang3.Validate.isTrue(
        elt != null, "null are not supported in sets"
      );
      if (first) {
        first = false;
      } else {
        sb.append(',');
      }
      Utils.appendValue(vdef, ecodec, codecRegistry, sb, elt, variables);
    }
    sb.append('}');
    return sb;
  }

  static StringBuilder appendMap(
    CQLDataType definition,
    TypeCodec<?> codec,
    CodecRegistry codecRegistry,
    StringBuilder sb,
    Map<?, ?> m,
    List<Object> variables
  ) {
    final CQLDataType kdef = definition.getFirstArgumentType();
    final TypeCodec<?> kcodec;
    final CQLDataType vdef = definition.getElementType();
    final TypeCodec<?> vcodec;

    if (codec instanceof ArgumentsCodec) {
      final ArgumentsCodec<?> acodec = (ArgumentsCodec<?>)codec;

      kcodec = acodec.codec(0);
      vcodec = acodec.codec(1);
    } else {
      kcodec = null;
      vcodec = codec;
    }
    sb.append('{');
    boolean first = true;

    for (final Map.Entry<?, ?> entry: m.entrySet()) {
      final Object eval = entry.getValue();

      org.apache.commons.lang3.Validate.isTrue(
        eval != null, "null are not supported in maps"
      );
      if (first) {
        first = false;
      } else {
        sb.append(',');
      }
      Utils.appendValue(
        kdef, kcodec, codecRegistry, sb, entry.getKey(), variables
      );
      sb.append(':');
      Utils.appendValue(vdef, vcodec, codecRegistry, sb, eval, variables);
    }
    sb.append('}');
    return sb;
  }

  public static StringBuilder appendName(StringBuilder sb, String name) {
    name = name.trim();
    // FIXME: checking for token( specifically is uber ugly, we'll need some
    // better solution.
    if ((cnamePattern.matcher(name).matches() && !Keywords.isReserved(name))
        || name.startsWith("\"")
        || name.startsWith("token(")
        // | is used when a select statement requires searching multiple keyspaces
        // as a result of using an IN with keyspace keys
        || name.contains("|")) {
      sb.append(name);
    } else {
      sb.append('"').append(name).append('"');
    }
    return sb;
  }

  @SuppressWarnings("synthetic-access")
  static StringBuilder appendName(
    TableInfoImpl<?> tinfo,
    TypeCodec<?> codec,
    CodecRegistry codecRegistry,
    StringBuilder sb,
    Object name
  ) {
    if (name instanceof String) {
      Utils.appendName(sb, (String)name);
    } else if (name instanceof CNameKey) {
      ((CNameKey)name).appendTo(tinfo, sb, codec, codecRegistry);
    } else if (name instanceof FCName) {
      Utils.appendName(sb, ((FCName)name).getName());
    } else if (name instanceof FCall) {
      final FCall fcall = (FCall)name;

      sb.append(fcall.getName()).append('(');
      for (int i = 0; i < fcall.parameters.length; i++ ) {
        if (i > 0) {
          sb.append(',');
        }
        Utils.appendValue(
          null, codec, codecRegistry, sb, fcall.parameters[i], null
        );
      }
      sb.append(")");
    } else if (name instanceof Alias) {
      final Alias a = (Alias)name;

      sb.append("AS(");
      Utils.appendName(tinfo, codec, codecRegistry, sb, a.column);
      sb.append(" AS ").append(a.alias);
    } else if (name instanceof Cast) {
      final Cast c = (Cast)name;

      sb.append("CAST(");
      Utils.appendName(tinfo, codec, codecRegistry, sb, c.column);
      sb.append(" AS ").append(c.targetType).append(')');
    } else if (name instanceof RawString) {
      sb.append(((RawString)name).str);
    } else if (name instanceof CharSequence) {
      Utils.appendName(sb, ((CharSequence)name).toString());
    } else {
      throw new IllegalArgumentException(
        "invalid column '"
        + name
        + "' of unknown type: "
        + ((name != null) ? name.getClass().getName() : "null")
      );
    }
    return sb;
  }

  static abstract class Appendeable {
    abstract void appendTo(
      TableInfoImpl<?> tinfo,
      TypeCodec<?> codec,
      CodecRegistry codecRegistry,
      StringBuilder sb,
      List<Object> variables
    );
    abstract boolean containsBindMarker();
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

  static abstract class FCName  {
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
      sb.append(getName()).append('(');
      for (int i = 0; i < parameters.length; i++ ) {
        if (i > 0) {
          sb.append('.');
        }
        sb.append(parameters[i]);
      }
      sb.append(')');
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

      Utils.appendName(sb, getColumnName());
      return sb.append('[').append(idx).append(']').toString();
    }
  }

  public static class CNameKey extends CName {
    private final Object key;

    public CNameKey(String name, Object key) {
      super(name);
      this.key = key;
    }

    void appendTo(
      TableInfoImpl<?> tinfo,
      StringBuilder sb,
      TypeCodec<?> codec,
      CodecRegistry codecRegistry
    ) {
      TypeCodec<?> kcodec;
      TypeCodec<?> vcodec;

      if (codec instanceof ArgumentsCodec) {
        final ArgumentsCodec<?> acodec = (ArgumentsCodec<?>)codec;

        kcodec = acodec.codec(0);
        vcodec = acodec.codec(1);
      } else {
        kcodec = null;
        vcodec = codec;
      }
      Utils.appendName(sb, getColumnName());
      sb.append('[');
      if (tinfo != null) {
        final FieldInfoImpl<?> finfo = tinfo.getColumnImpl(getColumnName());

        if (finfo != null) {
          codec = finfo.getCodec();
          if (codec instanceof ArgumentsCodec) {
            final ArgumentsCodec<?> acodec = (ArgumentsCodec<?>)codec;

            if (kcodec == null) {
              kcodec = acodec.codec(0);
            }
            if (vcodec == null) {
              vcodec = acodec.codec(1);
            }
          } else if (vcodec == null) {
            vcodec = codec;
          }
          Utils.appendValue(
            finfo.getDataType(), kcodec, codecRegistry, sb, key, null
          );
          sb.append(']');
          return;
        }
      }
      Utils.appendValue(null, vcodec, codecRegistry, sb, key, null);
      sb.append(']');
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder();

      Utils.appendName(sb, getColumnName());
      sb.append('[');
      Utils.appendValue(null, null, null, sb, key, null);
      return sb.append(']').toString();
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

  public static class Alias {
    private final Object column;
    private final String alias;

    Alias(Object column, String alias) {
      this.column = column;
      this.alias = alias;
    }

    @Override
    public String toString() {
      return String.format("%s AS %s", column, alias);
    }
  }

  static class Cast {
    private final Object column;
    private final DataType targetType;

    Cast(Object column, DataType targetType) {
      this.column = column;
      this.targetType = targetType;
    }

    @Override
    public String toString() {
      return String.format("CAST(%s AS %s)", column, targetType);
    }
  }
}


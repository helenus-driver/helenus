/*
 * Copyright (C) 2015-2016 The Helenus Driver Project Authors.
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
package org.helenus.jackson.jsonSchema.factories;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import java.time.ZoneId;

import org.apache.commons.collections4.iterators.EnumerationIterator;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonView;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonAnyFormatVisitor;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonArrayFormatVisitor;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonBooleanFormatVisitor;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonFormatTypes;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonFormatVisitable;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonIntegerFormatVisitor;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonMapFormatVisitor;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonNullFormatVisitor;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonNumberFormatVisitor;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonObjectFormatVisitor;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonStringFormatVisitor;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonValueFormat;
import com.fasterxml.jackson.databind.ser.std.JsonValueSerializer;
import com.fasterxml.jackson.databind.ser.std.StdKeySerializers;
import com.fasterxml.jackson.databind.ser.std.StringSerializer;
import com.fasterxml.jackson.module.jsonSchema.JsonSchema;
import com.fasterxml.jackson.module.jsonSchema.factories.ArrayVisitor;
import com.fasterxml.jackson.module.jsonSchema.factories.JsonSchemaFactory;
import com.fasterxml.jackson.module.jsonSchema.factories.MapVisitor;
import com.fasterxml.jackson.module.jsonSchema.factories.ObjectVisitor;
import com.fasterxml.jackson.module.jsonSchema.factories.ObjectVisitorDecorator;
import com.fasterxml.jackson.module.jsonSchema.factories.SchemaFactoryWrapper;
import com.fasterxml.jackson.module.jsonSchema.factories.StringVisitor;
import com.fasterxml.jackson.module.jsonSchema.factories.Visitor;
import com.fasterxml.jackson.module.jsonSchema.factories.VisitorContext;
import com.fasterxml.jackson.module.jsonSchema.factories.WrapperFactory;
import com.fasterxml.jackson.module.jsonSchema.types.ArraySchema;
import com.fasterxml.jackson.module.jsonSchema.types.ContainerTypeSchema;
import com.fasterxml.jackson.module.jsonSchema.types.IntegerSchema;
import com.fasterxml.jackson.module.jsonSchema.types.NumberSchema;
import com.fasterxml.jackson.module.jsonSchema.types.ObjectSchema;
import com.fasterxml.jackson.module.jsonSchema.types.ReferenceSchema;
import com.fasterxml.jackson.module.jsonSchema.types.SimpleTypeSchema;
import com.fasterxml.jackson.module.jsonSchema.types.StringSchema;
import com.fasterxml.jackson.module.jsonSchema.types.ValueTypeSchema;

import org.helenus.commons.lang3.reflect.ReflectionUtils;
import org.helenus.jackson.annotation.JsonPropertyDefaultValues;
import org.helenus.jackson.annotation.JsonPropertyDefaultValuesProvider;
import org.helenus.jackson.annotation.JsonPropertyDivisibleBy;
import org.helenus.jackson.annotation.JsonPropertyEnumValues;
import org.helenus.jackson.annotation.JsonPropertyEnumValuesProvider;
import org.helenus.jackson.annotation.JsonPropertyKeyDescription;
import org.helenus.jackson.annotation.JsonPropertyKeyReference;
import org.helenus.jackson.annotation.JsonPropertyLength;
import org.helenus.jackson.annotation.JsonPropertyMaximumValue;
import org.helenus.jackson.annotation.JsonPropertyMinimumValue;
import org.helenus.jackson.annotation.JsonPropertyOneOfDoubleValues;
import org.helenus.jackson.annotation.JsonPropertyOneOfFloatValues;
import org.helenus.jackson.annotation.JsonPropertyOneOfIntegerValues;
import org.helenus.jackson.annotation.JsonPropertyOneOfLongValues;
import org.helenus.jackson.annotation.JsonPropertyPattern;
import org.helenus.jackson.annotation.JsonPropertyReadOnly;
import org.helenus.jackson.annotation.JsonPropertyReference;
import org.helenus.jackson.annotation.JsonPropertyTitle;
import org.helenus.jackson.annotation.JsonPropertyUniqueItems;
import org.helenus.jackson.annotation.JsonPropertyValueDescription;
import org.helenus.jackson.annotation.JsonPropertyValueFormat;
import org.helenus.jackson.jsonSchema.types.MapTypesSchema;
import org.helenus.jackson.jsonSchema.types.ObjectTypesSchema;
import org.helenus.jackson.jsonSchema.types.ReferenceTypesSchema;
import org.helenus.jackson.jsonSchema.types.StringTypesSchema;
import org.helenus.util.function.EBiConsumer;

/**
 * The <code>JsonAnnotationSchemaFactoryWrapper</code> class extends on
 * {@link SchemaFactoryWrapper} to provide support for Json views, titles,
 * and validations through Json annotations. It also properly handles
 * schemas for <code>byte[]</code>.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Oct 28, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
public class JsonAnnotationSchemaFactoryWrapper extends SchemaFactoryWrapper {
  /**
   * Constants used to enable support for special property references
   * annotation to short-circuit the expansion based on annotations.
   *
   * @author paouelle
   */
  static final boolean ENABLE_ADHOC_REFS = false;

  /**
   * Gets the Json view name from the specified class.
   *
   * @author paouelle
   *
   * @param  clazz the class for which to get a Json view name
   * @return the corresponding Json view name
   */
  private static String getViewName(Class<?> clazz) {
    final String cname = clazz.getSimpleName();
    final Class<?> pclazz = clazz.getDeclaringClass();

     if (pclazz != null) {
       return pclazz.getSimpleName() + "." + cname;
     }
     return cname;
  }

  /**
   * Gets the enum values based on the specified annotation.
   *
   * @author paouelle
   *
   * @param  ae the Json enum values annotation
   * @param  filteredEnums the set of enum values to use when filtering default
   *         and enum values for the various properties of the schema
   * @param  keys <code>true</code> if we are updating the schema for keys;
   *         <code>false</code> for values
   * @return a set of all provided enum values
   */
  private static Set<String> getEnumValues(
    JsonPropertyEnumValues ae, Set<Enum<?>> filteredEnums, boolean keys
  ) {
    final Class<?>[] sclasses = keys ? ae.keySubTypesOf() : ae.valueSubTypesOf();
    Stream<String> s = Stream.empty();

    if (sclasses.length > 0) {
      final JsonSubTypes jst = ReflectionUtils.findFirstAnnotation(sclasses[0], JsonSubTypes.class);

      if (jst != null) {
        s = Stream.of(jst.value())
          .filter(t -> sclasses[0].isAssignableFrom(t.value()))
          .map(t -> {
            if (!t.name().isEmpty()) {
              return t.name();
            }
            final JsonTypeName jtn = t.value().getAnnotation(JsonTypeName.class);

            if (jtn != null) {
              return jtn.value();
            }
            return t.value().getName();
          });
      }
    }
    final Class<?>[] iclasses = keys ? ae.keyAvailablesOf() : ae.valueAvailablesOf();
    Stream<String> i = Stream.empty();

    if (iclasses.length > 0) {
      // check if the class provides a method annotated with JsonPropertyEnumValuesProvider
      final Set<Method> providers = ReflectionUtils.getAllAnnotationsForMethodsAnnotatedWith(
        iclasses[0], JsonPropertyEnumValuesProvider.class, true
      ).keySet();

      if (!providers.isEmpty()) {
        i = providers.stream()
          .flatMap(m -> {
           // validate the method is static
           if (!Modifier.isStatic(m.getModifiers())) {
             throw new IllegalArgumentException(
               "json property enum values provider method '"
               + m.getName()
               + "' is not static in class: "
               + iclasses[0].getSimpleName()
             );
           }
           try {
             final Object ret;

             if (m.getParameterCount() == 1) {
               ret = m.invoke(null, filteredEnums);
             } else {
               ret = m.invoke(null);
             }
             if (ret == null) {
               return Stream.empty();
             }
             final Class<?> type = m.getReturnType();

             if (type.isArray()) {
               return IntStream.range(0, Array.getLength(ret)).mapToObj(j -> Array.get(ret, j));
             } else if (ret instanceof Collection) {
               return ((Collection<?>)ret).stream();
             } else if (ret instanceof Stream) {
               return ((Stream<?>)ret);
             } else if (ret instanceof Iterator) {
               return StreamSupport.stream(
                 Spliterators.spliteratorUnknownSize(
                   (Iterator<?>)ret, Spliterator.ORDERED
                 ), false
               );
             } else if (ret instanceof Enumeration<?>) {
               return StreamSupport.stream(
                 Spliterators.spliteratorUnknownSize(
                   new EnumerationIterator<>((Enumeration<?>)ret), Spliterator.ORDERED
                 ), false
               );
             } else if (ret instanceof Iterable) {
               return StreamSupport.stream(((Iterable<?>)ret).spliterator(), false);
             }
             return Stream.of(ret);
           } catch (IllegalAccessException e) { // should not happen
             throw new IllegalStateException(e);
           } catch (InvocationTargetException e) {
             final Throwable t = e.getTargetException();

             if (t instanceof Error) {
               throw (Error)t;
             } else if (t instanceof RuntimeException) {
               throw (RuntimeException)t;
             } else { // we don't expect any of those
               throw new IllegalStateException(t);
             }
           }
         })
         .filter(o -> o != null)
         .map(o -> o.toString());
      } else if (iclasses[0] == Locale.class) {
        i = Stream.of(Locale.getAvailableLocales()).map(Locale::toString);
      } else if (iclasses[0] == ZoneId.class) {
        i = ZoneId.getAvailableZoneIds().stream();
      } else if (Enum.class.isAssignableFrom(iclasses[0])) {
        i = Stream.of(iclasses[0].getEnumConstants()).map(e -> ((Enum<?>)e).name());
      }
    }
    final String[] exclude = keys ? ae.keyExclude() : ae.valueExclude();

    return Stream.of(
      Stream.of(keys ? ae.key() : ae.value()), s, i
    ).flatMap(e -> e)
     .filter(e -> !ArrayUtils.contains(exclude, e))
     .collect(Collectors.toCollection(LinkedHashSet::new));
  }

  /**
   * Holds the type for the schema.
   *
   * @author paouelle
   */
  private JavaType type;

  /**
   * Instantiates a new <code>JsonViewSchemaFactoryWrapper</code> object.
   *
   * @author paouelle
   */
  public JsonAnnotationSchemaFactoryWrapper() {
    super(new JsonAnnotationSchemaFactoryWrapperFactory());
    super.schemaProvider = new JsonAnnotationSchemaFactoryProvider();
  }

  /**
   * Updates the specified schema for the specified property or its contained values.
   *
   * @author paouelle
   *
   * @param cschema the container schema for the property or <code>null</code>
   *        if not an item
   * @param schema the schema to update
   * @param prop the property for the schema
   * @param filteredEnumValues the set of enum values to use when filtering default
   *        and enum values for the various properties of the schema
   * @param filteredEnumKeyValues the set of enum key values to use when filtering default
   *        and enum values for the various map keys of the schema
   * @param which <code>true</code> if we are updating the schema for keys;
   *        <code>false</code> for map or array values and <code>null</code> for standard
   *        property
   */
  void updateSchema(
    JsonSchema cschema,
    JsonSchema schema,
    BeanProperty prop,
    Set<Enum<?>> filteredEnumValues,
    Set<Enum<?>> filteredEnumKeyValues,
    Boolean which
  ) {
    final boolean keys = (which != null) && which;
    JavaType jtype = prop.getType();

    if (Optional.class.isAssignableFrom(jtype.getRawClass())) {
      jtype = jtype.containedType(0); // jtype.getReferencedType() always return null :-(
    }
    if (cschema == null) { // not on keys/values; only on container
      final JsonPropertyReadOnly aro = prop.getAnnotation(JsonPropertyReadOnly.class);

      if (aro != null) {
        schema.setReadonly(true);
      }
    }
    if (which != null) {
      if (which) {
        final JsonPropertyKeyDescription ak = prop.getAnnotation(JsonPropertyKeyDescription.class);

        if ((ak != null) && !ak.value().isEmpty()) {
          schema.setDescription(ak.value());
        }
      } else {
        final JsonPropertyValueDescription av = prop.getAnnotation(JsonPropertyValueDescription.class);

        if ((av != null) && !av.value().isEmpty()) {
          schema.setDescription(av.value());
        } else { // pass description to value if none set specifically
          if (StringUtils.isEmpty(schema.getDescription())) {
            final JsonPropertyDescription apd = prop.getAnnotation(JsonPropertyDescription.class);

            if (apd != null) {
              schema.setDescription(apd.value());
            }
          }
        }
      }
    }
    if (schema instanceof SimpleTypeSchema) {
      final SimpleTypeSchema sschema = (SimpleTypeSchema)schema;
      final JsonPropertyTitle at = prop.getAnnotation(JsonPropertyTitle.class);

      if (at != null) {
        final String t = keys ? at.key() : at.value();

        if (!t.isEmpty()) {
          sschema.setTitle(t);
        }
      }
      final JsonPropertyDefaultValues adv = prop.getAnnotation(JsonPropertyDefaultValues.class);

      if (adv != null) {
        sschema.setDefault(
          Stream.concat(
            Stream.of(adv.value()),
            Stream.of(adv.valueFromClass())
              .flatMap(c -> ReflectionUtils.getAllAnnotationsForMethodsAnnotatedWith(
                c, JsonPropertyDefaultValuesProvider.class, true
              ).keySet().stream())
              .flatMap(m -> {
                // validate the method is static
                if (!Modifier.isStatic(m.getModifiers())) {
                  throw new IllegalArgumentException(
                    "json property default values provider method '"
                    + m.getName()
                    + "' is not static in class: "
                    + m.getDeclaringClass().getSimpleName()
                  );
                }
                try {
                  final Object ret;

                  if (m.getParameterCount() == 1) {
                    ret = m.invoke(null, keys ? filteredEnumKeyValues : filteredEnumValues);
                  } else {
                    ret = m.invoke(null);
                  }
                  if (ret == null) {
                    return Stream.empty();
                  }
                  final Class<?> type = m.getReturnType();

                  if (type.isArray()) {
                    return IntStream.range(0, Array.getLength(ret)).mapToObj(j -> Array.get(ret, j));
                  } else if (ret instanceof Collection) {
                    return ((Collection<?>)ret).stream();
                  } else if (ret instanceof Stream) {
                    return ((Stream<?>)ret);
                  } else if (ret instanceof Iterator) {
                    return StreamSupport.stream(
                      Spliterators.spliteratorUnknownSize(
                        (Iterator<?>)ret, Spliterator.ORDERED
                      ), false
                    );
                  } else if (ret instanceof Enumeration<?>) {
                    return StreamSupport.stream(
                      Spliterators.spliteratorUnknownSize(
                        new EnumerationIterator<>((Enumeration<?>)ret), Spliterator.ORDERED
                      ), false
                    );
                  } else if (ret instanceof Iterable) {
                    return StreamSupport.stream(((Iterable<?>)ret).spliterator(), false);
                  }
                  return Stream.of(ret);
                } catch (IllegalAccessException e) { // should not happen
                  throw new IllegalStateException(e);
                } catch (InvocationTargetException e) {
                  final Throwable t = e.getTargetException();

                  if (t instanceof Error) {
                    throw (Error)t;
                  } else if (t instanceof RuntimeException) {
                    throw (RuntimeException)t;
                  } else { // we don't expect any of those
                    throw new IllegalStateException(t);
                  }
                }
              })
              .filter(o -> o != null)
              .map(o -> o.toString())
          ).collect(Collectors.joining(", "))
        );
      }
    }
    if (schema instanceof ValueTypeSchema) {
      final ValueTypeSchema vtschema = (ValueTypeSchema)schema;
      final JsonPropertyEnumValues ae = prop.getAnnotation(JsonPropertyEnumValues.class);

      if (ae != null) {
        vtschema.setEnums(JsonAnnotationSchemaFactoryWrapper.getEnumValues(
          ae, keys ? filteredEnumKeyValues : filteredEnumValues, keys
        ));
      }
      final JsonPropertyValueFormat avf = prop.getAnnotation(JsonPropertyValueFormat.class);

      if (avf != null) {
        final JsonValueFormat[] jvf = keys ? avf.key() : avf.value();

        if (jvf.length > 0) {
          vtschema.setFormat(jvf[0]);
        }
      }
    }
    if (schema instanceof ContainerTypeSchema) {
      final ContainerTypeSchema ctschema = (ContainerTypeSchema)schema;

      if (!(schema instanceof MapTypesSchema)) {
        final JsonPropertyEnumValues ae = prop.getAnnotation(JsonPropertyEnumValues.class);

        if (ae != null) {
          ctschema.setEnums(JsonAnnotationSchemaFactoryWrapper.getEnumValues(
            ae, keys ? filteredEnumKeyValues : filteredEnumValues, keys
          ));
        }
      }
      final JsonPropertyOneOfIntegerValues ai = prop.getAnnotation(JsonPropertyOneOfIntegerValues.class);
      final JsonPropertyOneOfLongValues al = prop.getAnnotation(JsonPropertyOneOfLongValues.class);
      final JsonPropertyOneOfFloatValues af = prop.getAnnotation(JsonPropertyOneOfFloatValues.class);
      final JsonPropertyOneOfDoubleValues ad = prop.getAnnotation(JsonPropertyOneOfDoubleValues.class);

      ctschema.setOneOf(
        Stream.of(
          (ai != null) ? Stream.of(keys ? ai.key() : ai.value()) : Stream.empty(),
          (al != null) ? Stream.of(keys ? al.key() : al.value()) : Stream.empty(),
          (af != null) ? Stream.of(keys ? af.key() : af.value()) : Stream.empty(),
          (ad != null) ? Stream.of(keys ? ad.key() : ad.value()) : Stream.empty()
        ).flatMap(n -> n)
         .collect(Collectors.toCollection(LinkedHashSet::new))
      );
    }
    if (schema instanceof MapTypesSchema) {
      final MapTypesSchema mschema = (MapTypesSchema)schema;

      updateSchema(schema, mschema.getKeysSchema(), prop, filteredEnumValues, filteredEnumKeyValues, true);
      updateSchema(schema, mschema.getValuesSchema(), prop, filteredEnumValues, filteredEnumKeyValues, false);
    } else if (schema.isObjectSchema()) {
      final ObjectSchema oschema = (ObjectSchema)schema;
      final ObjectSchema.AdditionalProperties aprops = oschema.getAdditionalProperties();

      if (aprops instanceof ObjectSchema.SchemaAdditionalProperties) {
        final ObjectSchema.SchemaAdditionalProperties saprops = (ObjectSchema.SchemaAdditionalProperties)aprops;
        final JsonSchema sschema = saprops.getJsonSchema();

        updateSchema(schema, sschema, prop, filteredEnumValues, filteredEnumKeyValues, null);
      }
    } else if (schema.isArraySchema()) {
      final ArraySchema aschema = schema.asArraySchema();
      final JsonPropertyLength al = prop.getAnnotation(JsonPropertyLength.class);

      if (al != null) {
        final int l = keys ? al.key() : al.value();

        if (l >= 0) {
          aschema.setMinItems(l);
          aschema.setMaxItems(l);
        } else {
          final int min = keys ? al.keyMin() : al.valueMin();
          final int max = keys ? al.keyMax() : al.valueMax();

          if (min >= 0) {
            aschema.setMinItems(min);
          }
          if (max >= 0) {
            aschema.setMaxItems(max);
          }
        }
      }
      if (Set.class.isAssignableFrom(jtype.getRawClass())) {
        aschema.setUniqueItems(true);
      } else {
        final JsonPropertyUniqueItems au = prop.getAnnotation(JsonPropertyUniqueItems.class);

        if (au != null) {
          aschema.setUniqueItems(true);
        }
      }
      final ArraySchema.Items items = aschema.getItems();

      if (items.isArrayItems()) {
        final ArraySchema.ArrayItems aitems = (ArraySchema.ArrayItems)items;

        for (final JsonSchema ischema: aitems.getJsonSchemas()) {
          updateSchema(schema, ischema, prop, filteredEnumValues, filteredEnumKeyValues, false);
        }
      } else if (items.isSingleItems()) {
        final ArraySchema.SingleItems sitems = (ArraySchema.SingleItems)items;
        final JsonSchema ischema = sitems.getSchema();

        updateSchema(schema, ischema, prop, filteredEnumValues, filteredEnumKeyValues, false);
      }
    } else if (schema.isNumberSchema()) {
      final NumberSchema nschema = schema.asNumberSchema();
      final JsonPropertyMaximumValue ama = prop.getAnnotation(JsonPropertyMaximumValue.class);
      final JsonPropertyMinimumValue ami = prop.getAnnotation(JsonPropertyMinimumValue.class);

      if (ama != null) {
        final double[] max = keys ? ama.key() : ama.value();

        if (max.length > 0) {
          nschema.setMaximum(max[0]);
          if (keys ? ama.keyExclusive() : ama.valueExclusive()) {
            nschema.setExclusiveMaximum(true);
          }
        }
      }
      if (ami != null) {
        final double[] min = keys ? ami.key() : ami.value();

        if (min.length > 0) {
          nschema.setMinimum(min[0]);
          if (keys ? ami.keyExclusive() : ami.valueExclusive()) {
            nschema.setExclusiveMinimum(true);
          }
        }
      }
      if (schema.isIntegerSchema()) {
        final IntegerSchema ischema = (IntegerSchema)schema;
        final JsonPropertyDivisibleBy ad = prop.getAnnotation(JsonPropertyDivisibleBy.class);

        if (ad != null) {
          final int div = keys ? ad.key() : ad.value();

          if (div > 0) {
            ischema.setDivisibleBy(div);
          }
        }
      }
    } else if (schema.isStringSchema()) {
      final StringSchema sschema = schema.asStringSchema();
      final JsonPropertyLength al = prop.getAnnotation(JsonPropertyLength.class);

      if (al != null) {
        final int l = keys ? al.key() : al.value();

        if (l >= 0) {
          sschema.setMinLength(l);
          sschema.setMaxLength(l);
        } else {
          final int min = keys ? al.keyMin() : al.valueMin();
          final int max = keys ? al.keyMax() : al.valueMax();

          if (min >= 0) {
            sschema.setMinLength(min);
          }
          if (max >= 0) {
            sschema.setMaxLength(max);
          }
        }
      }
      final JsonPropertyPattern ap = prop.getAnnotation(JsonPropertyPattern.class);

      if (ap != null) {
        sschema.setPattern(keys ? ap.key() : ap.value());
      } else {
        Class<?> clazz = jtype.getRawClass();

        if (cschema != null) { // doing keys/values of a container
          if (Map.class.isAssignableFrom(clazz)) {
            clazz = (keys ? jtype.getKeyType() : jtype.getContentType()).getRawClass();
          } else if (Collection.class.isAssignableFrom(clazz)) {
            clazz = jtype.getContentType().getRawClass();
          }
        }
        if (Locale.class.isAssignableFrom(clazz)) {
          sschema.setPattern("^([a-zA-Z]{2,8}(_[a-zA-Z]{2}|[0-9]{3})?([-_]([0-9][0-9a-zA-Z]{3}|[0-9a-zA-Z]{5,8}))?)?$   # Java Locale");
        } else if (ZoneId.class.isAssignableFrom(clazz)) {
          sschema.setEnums(
            Stream.of(TimeZone.getAvailableIDs())
              .collect(Collectors.toCollection(TreeSet::new))
          );
        } else if (UUID.class.isAssignableFrom(clazz)) {
          sschema.setPattern("^(?i)[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$   # UUID");
        } else if (Class.class.isAssignableFrom(clazz)) {
          sschema.setPattern("\\p{ASCII}+   # ascii");
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.fasterxml.jackson.module.jsonSchema.factories.SchemaFactoryWrapper#expectArrayFormat(com.fasterxml.jackson.databind.JavaType)
   */
  @Override
  public JsonArrayFormatVisitor expectArrayFormat(JavaType type) {
    this.type = type;
    if (type.getContentType().getRawClass().equals(byte.class)) {
      // special case to handle byte[] serialized as string and not string[]
      final StringVisitor visitor = (StringVisitor)super.expectStringFormat(type);

      visitor.getSchema().setPattern("^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$   # Base64");
      // :-( we still need to return a fake array visitor
      return new ArrayVisitor(null, null) {
        @Override
        public JsonSchema getSchema() {
          return visitor.getSchema();
        }
        @Override
        public void itemsFormat(JsonFormatVisitable handler, JavaType contentType)
          throws JsonMappingException {}
        @Override
        public void itemsFormat(JsonFormatTypes format)
          throws JsonMappingException {}
      };
    }
    // if we don't already have a recursive visitor context, create one
    if (visitorContext == null) {
      super.visitorContext = new JsonAnnotationVisitorContext();
    }
    final ArrayVisitor visitor = (ArrayVisitor)super.expectArrayFormat(type);
    final ArraySchema aschema = (ArraySchema)visitor.getSchema();

    return new ArrayVisitor(visitor.getProvider(), aschema, visitor.getWrapperFactory()) {
      @Override
      public SerializerProvider getProvider() {
        return visitor.getProvider();
      }
      @Override
      public void setProvider(SerializerProvider provider) {
        visitor.setProvider(provider);
      }
      @SuppressWarnings("synthetic-access")
      @Override
      public void itemsFormat(JsonFormatVisitable handler, JavaType elementType)
        throws JsonMappingException {
        visitor.itemsFormat(handler, elementType);
        final ArraySchema.Items items = aschema.getItems();

        if (items instanceof ArraySchema.SingleItems) {
          final ArraySchema.SingleItems sitems = (ArraySchema.SingleItems)items;
          final JsonSchema ischema = sitems.getSchema();

          if (ischema instanceof ReferenceSchema) {
            // make sure we are always returning ref types schema
            if (!(ischema instanceof ReferenceTypesSchema)) {
              sitems.setSchema(
                new ReferenceTypesSchema((ReferenceSchema)ischema, elementType)
              );
            }
          }
        } else if (items instanceof ArraySchema.ArrayItems) {
          final ArraySchema.ArrayItems aitems = (ArraySchema.ArrayItems)aschema.getItems();
          final JsonSchema[] ischemas = aitems.getJsonSchemas();

          for (int i = 0; i < ischemas.length; i++) {
            // make sure we are always returning ref types schema
            if (!(ischemas[i] instanceof ReferenceTypesSchema)) {
              ischemas[i] = new ReferenceTypesSchema(
                (ReferenceSchema)ischemas[i], elementType
              );
            }
          }
        } else { // (items == null)
          // for some stupid reason, the schema factory we are extending decided
          // it would be a good idea to null instead of an any schema!!!!!
          // so normally we should return an any schema
          aschema.setItemsSchema(schemaProvider.anySchema());
        }
      }
      @Override
      public void itemsFormat(JsonFormatTypes format)
        throws JsonMappingException {
        visitor.itemsFormat(format);
      }
      @Override
      public JsonSchema getSchema() {
        return visitor.getSchema();
      }
      @Override
      public WrapperFactory getWrapperFactory() {
        return visitor.getWrapperFactory();
      }
      @Override
      public void setWrapperFactory(WrapperFactory wrapperFactory) {
        visitor.setWrapperFactory(wrapperFactory);
      }
      @Override
      public Visitor setVisitorContext(VisitorContext rvc) {
        return visitor.setVisitorContext(rvc);
      }
    };
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.fasterxml.jackson.module.jsonSchema.factories.SchemaFactoryWrapper#expectObjectFormat(com.fasterxml.jackson.databind.JavaType)
   */
  @Override
  public JsonObjectFormatVisitor expectObjectFormat(JavaType type) {
    this.type = type;
    // if we don't already have a recursive visitor context, create one
    if (visitorContext == null) {
      super.visitorContext = new JsonAnnotationVisitorContext();
    }
    final ObjectVisitor visitor = (ObjectVisitor)super.expectObjectFormat(type);

    if (visitor.getSchema() instanceof ObjectTypesSchema) { // should always be true
      ((ObjectTypesSchema)visitor.getSchema()).setJavaTypesFor(type);
    }
    final SerializerProvider p = getProvider();
    final Class<?> view = (p != null) ? p.getActiveView() : null;
    final JsonTypeInfo jti = ReflectionUtils.findFirstAnnotation(type.getRawClass(), JsonTypeInfo.class);
    final String typeIdName;
    final String typeName;

    if ((jti != null) && (jti.use() == JsonTypeInfo.Id.NAME)) {
      typeIdName = jti.property();
      final JsonTypeName jtn = type.getRawClass().getAnnotation(JsonTypeName.class);

      if (jtn != null) {
        typeName = jtn.value();
      } else {
        typeName = null;
      }
    } else {
      typeIdName = null;
      typeName = null;
    }
//    if (type.getRawClass().getSimpleName().equals("RoleMap")) {
//      System.out.println("**************** HERE HERE HERE OBJ: RoleMap - " + ReflectionToStringBuilder.toString(schema));
//    }
    return new ObjectVisitorDecorator(visitor) {
      @SuppressWarnings("synthetic-access")
      private JsonSchema getPropertySchema(BeanProperty writer) {
        final Map<String, JsonSchema> properties = ((ObjectSchema)getSchema()).getProperties();
        JsonSchema schema = properties.get(writer.getName());

        if (JsonAnnotationSchemaFactoryWrapper.ENABLE_ADHOC_REFS) {
          final JsonPropertyReference apr = writer.getAnnotation(JsonPropertyReference.class);

          if (apr != null) {
            if (schema instanceof ArraySchema) {
              final ArraySchema aschema = (ArraySchema)schema;
              final ArraySchema.Items items = aschema.getItems();

              if (items.isArrayItems()) {
                final ArraySchema.ArrayItems aitems = (ArraySchema.ArrayItems)items;
                final JsonSchema[] aschemas = aitems.getJsonSchemas();

                for (int i = 0; i < aitems.getJsonSchemas().length; i++) {
                  if (aschemas[i] instanceof ObjectTypesSchema) {
                    aschemas[i] = new ReferenceTypesSchema(
                      (ObjectTypesSchema)aschemas[i]
                    );
                  }
                }
              } else if (items.isSingleItems()) {
                final ArraySchema.SingleItems sitems = (ArraySchema.SingleItems)items;
                final JsonSchema ischema = sitems.getSchema();

                if (ischema instanceof ObjectTypesSchema) {
                  sitems.setSchema(
                    new ReferenceTypesSchema((ObjectTypesSchema)ischema)
                  );
                }
              }
            } else if (schema instanceof MapTypesSchema) {
              final MapTypesSchema mschema = (MapTypesSchema)schema;
              final JsonSchema vschema = mschema.getValuesSchema();

              if (vschema instanceof ObjectTypesSchema) {
                mschema.setValuesSchema(
                  new ReferenceTypesSchema((ObjectTypesSchema)vschema)
                );
              } else if (vschema instanceof ArraySchema) {
                final ArraySchema aschema = (ArraySchema)vschema;
                final ArraySchema.Items items = aschema.getItems();

                if (items.isArrayItems()) {
                  final ArraySchema.ArrayItems aitems = (ArraySchema.ArrayItems)items;
                  final JsonSchema[] aschemas = aitems.getJsonSchemas();

                  for (int i = 0; i < aitems.getJsonSchemas().length; i++) {
                    if (aschemas[i] instanceof ObjectSchema) {
                      aschemas[i] = new ReferenceTypesSchema(
                        (ObjectTypesSchema)aschemas[i]
                      );
                    }
                  }
                } else if (items.isSingleItems()) {
                  final ArraySchema.SingleItems sitems = (ArraySchema.SingleItems)items;
                  final JsonSchema ischema = sitems.getSchema();

                  if (ischema instanceof ObjectSchema) {
                    sitems.setSchema(
                      new ReferenceTypesSchema((ObjectTypesSchema)ischema)
                    );
                  }
                }
              }
            } else if (schema instanceof ObjectTypesSchema) {
              schema = new ReferenceTypesSchema((ObjectTypesSchema)schema);
            }
            properties.put(writer.getName(), schema);
          }
          if (schema instanceof MapTypesSchema) {
            final JsonPropertyKeyReference apkr = writer.getAnnotation(JsonPropertyKeyReference.class);

            if (apkr != null) {
              final MapTypesSchema mschema = (MapTypesSchema)schema;
              final JsonSchema kschema = mschema.getKeysSchema();

              if (kschema instanceof ObjectTypesSchema) {
                mschema.setKeysSchema(
                  new ReferenceTypesSchema((ObjectTypesSchema)kschema)
                );
              }
            }
          }
        }
        if (schema instanceof ReferenceSchema) {
          // make sure we are always returning ref types schema
          if (!(schema instanceof ReferenceTypesSchema)) {
            schema = new ReferenceTypesSchema((ReferenceSchema)schema, writer.getType());
            properties.put(writer.getName(), schema);
          }
        }
        return schema;
      }
      private boolean isIncluded(BeanProperty writer) {
        if (view == null) {
          return true;
        }
        final JsonView a = writer.getAnnotation(JsonView.class);

        if ((a == null) || ArrayUtils.isEmpty(a.value())) {
          return true;
        }
        for (final Class<?> clazz: a.value()) {
          if (clazz.isAssignableFrom(view)) {
            return true;
          }
        }
        return false;
      }
      private void updateForTypeId(JsonSchema schema, BeanProperty writer) {
        if ((schema instanceof SimpleTypeSchema)
            && writer.getName().equals(typeIdName)) {
          final SimpleTypeSchema sschema = (SimpleTypeSchema)schema;

          if (sschema.getDefault() == null) {
            sschema.setDefault(typeName);
          }
        }
      }
      private Set<Enum<?>> getPropertyEnumValues(
        JsonSchema schema, BeanProperty writer, boolean keys
      ) {
        if ((schema instanceof ObjectSchema) && writer.getType().isEnumType()) {
          // check if the property was annotated with the @JsonPropertyEnumValues
          // with valueAvailablesOf from a class that has an annotated method
          // with @JsonPropertyEnumProvider that returns enum of that type
          final JsonPropertyEnumValues ae = writer.getAnnotation(JsonPropertyEnumValues.class);

          if (ae != null) {
            final Class<?>[] aclasses = keys ? ae.keyAvailablesOf() : ae.valueAvailablesOf();

            if (aclasses.length > 0) {
              final Set<Enum<?>> set = ReflectionUtils.getAllAnnotationsForMethodsAnnotatedWith(
                aclasses[0], JsonPropertyEnumValuesProvider.class, true
              ).keySet().stream()
               .flatMap(m -> {
                 // validate the method is static
                 if (!Modifier.isStatic(m.getModifiers())) {
                   throw new IllegalArgumentException(
                     "json property enum values provider method '"
                     + m.getName()
                     + "' is not static in class: "
                     + aclasses[0].getSimpleName()
                   );
                 }
                 try {
                   final Object ret = m.invoke(null);

                   if (ret == null) {
                     return Stream.empty();
                   }
                   final Class<?> type = m.getReturnType();

                   if (type.isArray()) {
                     return IntStream.range(0, Array.getLength(ret)).mapToObj(j -> Array.get(ret, j));
                   } else if (ret instanceof Collection) {
                     return ((Collection<?>)ret).stream();
                   } else if (ret instanceof Stream) {
                     return (Stream<?>)ret;
                   } else if (ret instanceof Iterator) {
                     return StreamSupport.stream(
                       Spliterators.spliteratorUnknownSize(
                         (Iterator<?>)ret, Spliterator.ORDERED
                       ), false
                     );
                   } else if (ret instanceof Enumeration<?>) {
                     return StreamSupport.stream(
                       Spliterators.spliteratorUnknownSize(
                         new EnumerationIterator<>((Enumeration<?>)ret), Spliterator.ORDERED
                       ), false
                     );
                   } else if (ret instanceof Iterable) {
                     return StreamSupport.stream(((Iterable<?>)ret).spliterator(), false);
                   }
                   return Stream.of(ret);
                 } catch (IllegalAccessException e) { // should not happen
                   throw new IllegalStateException(e);
                 } catch (InvocationTargetException e) {
                   final Throwable t = e.getTargetException();

                   if (t instanceof Error) {
                     throw (Error)t;
                   } else if (t instanceof RuntimeException) {
                     throw (RuntimeException)t;
                   } else { // we don't expect any of those
                     throw new IllegalStateException(t);
                   }
                 }
               })
               .filter(o -> writer.getType().getRawClass().isInstance(o))
               .map(o -> (Enum<?>)o)
               .collect(Collectors.toCollection(LinkedHashSet::new));

              return Collections.unmodifiableSet(set);
            }
          }
        }
        return Collections.emptySet();
      }
      @SuppressWarnings("synthetic-access")
      @Override
      public void optionalProperty(BeanProperty writer) throws JsonMappingException {
        // this is called when @JsonInclude(JsonInclude.Include.NON_EMPTY) is set on the property
//        if (type.getRawClass().getSimpleName().equals("RoleMap")) {
//          System.out.println("**************** HERE HERE HERE OPROPS: " + writer.getName() + " RoleMap - " + schema);
//        }
//          if (writer.getName().equals("groups")) {
//            System.out.println("*** GROUPS");
//          }
        if (isIncluded(writer)) {
          ((JsonAnnotationVisitorContext)visitorContext).withEnumValuesDo(
            getPropertyEnumValues(schema, writer, false),
            getPropertyEnumValues(schema, writer, true),
            (evs, eks) -> {
              super.optionalProperty(writer);
              final JsonSchema schema = getPropertySchema(writer);

              updateSchema(null, schema, writer, evs, eks, null);
              updateForTypeId(schema, writer);
            }
          );
        }
      }
      @SuppressWarnings("synthetic-access")
      @Override
      public void property(BeanProperty writer) throws JsonMappingException {
//        if (type.getRawClass().getSimpleName().equals("RoleMap")) {
//          System.out.println("**************** HERE HERE HERE PROPS 1: " + writer.getName() + " RoleMap - This: " + schema);
//        } else if (type.getRawClass().getSimpleName().equals("Restrictions")) {
//          System.out.println("**************** HERE HERE HERE PROPS 1: " + writer.getName() + " Restrictions - This: " + schema);
//        }
//        if (writer.getName().equals("groups")) {
//          System.out.println("*** GROUPS");
//        }
        if (isIncluded(writer)) {
          ((JsonAnnotationVisitorContext)visitorContext).withEnumValuesDo(
            getPropertyEnumValues(schema, writer, false),
            getPropertyEnumValues(schema, writer, true),
            (evs, eks) -> {
              super.property(writer);
//              if (type.getRawClass().getSimpleName().equals("RoleMap")) {
//                System.out.println("**************** HERE HERE HERE PROPS 2: " + writer.getName() + " RoleMap - This: " + schema);
//              } else if (type.getRawClass().getSimpleName().equals("Restrictions")) {
//                System.out.println("**************** HERE HERE HERE PROPS 2: " + writer.getName() + " Restrictions - This: " + schema);
//              }
              final JsonSchema schema = getPropertySchema(writer);

              updateSchema(null, schema, writer, evs, eks, null);
              updateForTypeId(schema, writer);
//              if (type.getRawClass().getSimpleName().equals("RoleMap")) {
//                System.out.println("**************** HERE HERE HERE PROPS 3: " + writer.getName() + " RoleMap - Property: " + schema);
//              } else if (type.getRawClass().getSimpleName().equals("Restrictions")) {
//                System.out.println("**************** HERE HERE HERE PROPS 3: " + writer.getName() + " Restrictions - Property: " + schema);
//              }
            }
          );
        }
      }
    };
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.fasterxml.jackson.module.jsonSchema.factories.SchemaFactoryWrapper#expectMapFormat(com.fasterxml.jackson.databind.JavaType)
   */
  @Override
  public JsonMapFormatVisitor expectMapFormat(JavaType type)
    throws JsonMappingException {
    this.type = type;
    // if we don't already have a recursive visitor context, create one
    if (visitorContext == null) {
      super.visitorContext = new JsonAnnotationVisitorContext();
    }
    // replace schema created by super with our own more specific one
    final MapVisitor visitor = (MapVisitor)super.expectMapFormat(type);
    final MapTypesSchema mschema = new MapTypesSchema();

    super.schema = mschema;
    final MapVisitor mvisitor = new MapVisitor(visitor.getProvider(), mschema, new JsonAnnotationSchemaFactoryWrapperFactory()) {
      final MapTypesSchema schema = mschema;

      protected JsonSchema propertySchema(
        JsonFormatVisitable handler,
        JavaType propertyTypeHint,
        boolean key
      ) throws JsonMappingException {
        if (key && (handler instanceof JsonValueSerializer)) {
          final JsonValueSerializer jvhandler = (JsonValueSerializer)handler;

          if (jvhandler.handledType().equals(String.class)) {
            // special handling for keys that are not string but are of a class that
            // is annotated with @JsonValue which returns a string such that they
            // don't get their schema generated as an ANY schema
            return new StringTypesSchema(propertyTypeHint);
          }
        }
        final JsonSchema schema = super.propertySchema(handler, propertyTypeHint);

        if (schema instanceof ReferenceSchema) {
          if (key) {
            // oh well! we saw that type before but this is not good as we need a
            // special string schema that references this type
            // such type typically have a serializer defined so they can be serialized to strings
            return new StringTypesSchema((ReferenceSchema)schema, propertyTypeHint);
          }
          // make sure we have a reference types schema
          if (!(schema instanceof ReferenceTypesSchema)) {
            return new ReferenceTypesSchema((ReferenceSchema)schema, propertyTypeHint);
          }
        }
        return schema;
      }
      @Override
      public void keyFormat(JsonFormatVisitable handler, JavaType keyType)
        throws JsonMappingException {
//        if (keyType.getRawClass().getSimpleName().equals("EMAC")) {
//          System.out.println("HERE HERE HER HERE HERE HERE - handler: " + ReflectionToStringBuilder.toString(handler));
//        }
        if (handler instanceof StdKeySerializers.StringKeySerializer) {
          // special handling for keys such that they don't get their schema
          // generated as an ANY schema
          handler = new StringSerializer();
        }
        final JsonSchema kschema = propertySchema(handler, keyType, true);

        schema.setKeysSchema(kschema);
        schema.setKeysType(keyType);
      }
      @Override
      public void valueFormat(JsonFormatVisitable handler, JavaType valueType)
        throws JsonMappingException {
//        if (valueType.getRawClass().getSimpleName().equals("Restrictions")) {
//          System.out.println("**************** HERE HERE HERE 1: Restrictions - Container: " + schema);
//        } else if (valueType.getRawClass().getSimpleName().equals("RoleMap")) {
//          System.out.println("**************** HERE HERE HERE 1: Restrictions - Container: " + schema);
//        }
        final JsonSchema vschema = propertySchema(handler, valueType, false);

        schema.setValuesSchema(vschema);
        schema.setValuesType(valueType);
//        if (valueType.getRawClass().getSimpleName().equals("Restrictions")) {
//          System.out.println("**************** HERE HERE HERE 2: Restrictions - This: " + schema);
//        } else if (valueType.getRawClass().getSimpleName().equals("RoleMap")) {
//          System.out.println("**************** HERE HERE HERE REF: RoleMap - This: " + schema);
//        }
      }
    };

    mvisitor.setVisitorContext(visitorContext);
    return mvisitor;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.fasterxml.jackson.module.jsonSchema.factories.SchemaFactoryWrapper#expectAnyFormat(com.fasterxml.jackson.databind.JavaType)
   */
  @Override
  public JsonAnyFormatVisitor expectAnyFormat(JavaType type) {
    this.type = type;
    return super.expectAnyFormat(type);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.fasterxml.jackson.module.jsonSchema.factories.SchemaFactoryWrapper#expectBooleanFormat(com.fasterxml.jackson.databind.JavaType)
   */
  @Override
  public JsonBooleanFormatVisitor expectBooleanFormat(JavaType type) {
    this.type = type;
    return super.expectBooleanFormat(type);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.fasterxml.jackson.module.jsonSchema.factories.SchemaFactoryWrapper#expectIntegerFormat(com.fasterxml.jackson.databind.JavaType)
   */
  @Override
  public JsonIntegerFormatVisitor expectIntegerFormat(JavaType type) {
    this.type = type;
    return super.expectIntegerFormat(type);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.fasterxml.jackson.module.jsonSchema.factories.SchemaFactoryWrapper#expectNullFormat(com.fasterxml.jackson.databind.JavaType)
   */
  @Override
  public JsonNullFormatVisitor expectNullFormat(JavaType type) {
    this.type = type;
    return super.expectNullFormat(type);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.fasterxml.jackson.module.jsonSchema.factories.SchemaFactoryWrapper#expectNumberFormat(com.fasterxml.jackson.databind.JavaType)
   */
  @Override
  public JsonNumberFormatVisitor expectNumberFormat(JavaType type) {
    this.type = type;
    return super.expectNumberFormat(type);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.fasterxml.jackson.module.jsonSchema.factories.SchemaFactoryWrapper#expectStringFormat(com.fasterxml.jackson.databind.JavaType)
   */
  @Override
  public JsonStringFormatVisitor expectStringFormat(JavaType type) {
    this.type = type;
    return super.expectStringFormat(type);
  }

  /**
   * Finalize the schema and add a suitable title to it if none defined.
   *
   * @author paouelle
   *
   * @return the corresponding schema
   */
  public JsonSchema finalSchemaWithTitle() {
    final JsonSchema schema = super.finalSchema();
    final SimpleTypeSchema sschema = schema.asSimpleTypeSchema();

    if (sschema != null) {
      if ((type != null) && (sschema.getTitle() == null)) {
        sschema.setTitle(type.getRawClass().getSimpleName());
      }
      final String title = sschema.getTitle();

      if (title != null) {
        final SerializerProvider p = getProvider();
        final Class<?> view = (p != null) ? p.getActiveView() : null;

        if (view != null) {
          sschema.setTitle(
            title
            + " with view '"
            + JsonAnnotationSchemaFactoryWrapper.getViewName(view)
            + "'"
          );
        }
      }
    }
    return schema;
  }
}

/**
 * The <code>JsonAnnotationSchemaFactoryProvider</code> provides a schema
 * factory provider suitable to this extension to the wrapper factory wrapper.
 *
 * @copyright 2015-2016 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jun 24, 2016 - paouelle - Creation
 *
 * @since 1.0
 */
class JsonAnnotationSchemaFactoryProvider extends JsonSchemaFactory {
  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.fasterxml.jackson.module.jsonSchema.factories.JsonSchemaFactory#objectSchema()
   */
  @Override
  public ObjectSchema objectSchema() {
    return new ObjectTypesSchema();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.fasterxml.jackson.module.jsonSchema.factories.JsonSchemaFactory#arraySchema()
   */
  @Override
  public ArraySchema arraySchema() {
    return new org.helenus.jackson.jsonSchema.types.ArraySchema();
  }
}

/**
 * The <code>JsonAnnotationSchemaFactoryWrapperFactory</code> class defines a factory
 * for the {@link JsonAnnotationSchemaFactoryWrapper} class.
 *
 * @copyright 2015-2016 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Oct 29, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
class JsonAnnotationSchemaFactoryWrapperFactory extends WrapperFactory {
  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.fasterxml.jackson.module.jsonSchema.factories.WrapperFactory#getWrapper(com.fasterxml.jackson.databind.SerializerProvider)
   */
  @Override
  public SchemaFactoryWrapper getWrapper(SerializerProvider p) {
    final SchemaFactoryWrapper wrapper = new JsonAnnotationSchemaFactoryWrapper();

    wrapper.setProvider(p);
    return wrapper;
  };

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.fasterxml.jackson.module.jsonSchema.factories.WrapperFactory#getWrapper(com.fasterxml.jackson.databind.SerializerProvider, com.fasterxml.jackson.module.jsonSchema.factories.VisitorContext)
   */
  @Override
  public SchemaFactoryWrapper getWrapper(SerializerProvider p, VisitorContext rvc) {
    final SchemaFactoryWrapper wrapper = new JsonAnnotationSchemaFactoryWrapper();

    wrapper.setProvider(p);
    wrapper.setVisitorContext(rvc);
    return wrapper;
  }
};

/**
 * The <code>JsonAnnotationVisitorContext</code> class extends the standard
 * {@link VisitorContext} class in order to keep track of a set of filtered
 * enum values to be used when visiting enum classes that are serialized as
 * Json objects instead of the usual string schema.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Nov 22, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
class JsonAnnotationVisitorContext extends VisitorContext {
  /**
   * Holds the current set of filtered enum values.
   *
   * @author paouelle
   */
  private Set<Enum<?>> enumValues = Collections.emptySet();

  /**
   * Holds the current set of filtered enum key values.
   *
   * @author paouelle
   */
  private Set<Enum<?>> enumKeyValues = Collections.emptySet();

  /**
   * Executes the specified piece of code after having pushed onto the current
   * stack the provided sets of filtered enum values and filtered enum key values.
   * The specified code will be provided with the previous sets of enum values and
   * enum key values while any recursion from this point on will be given the
   * specified sets of filtered enum values and filtered enum key values.
   *
   * @author paouelle
   *
   * @param  enumValues the non-<code>null</code> new set of filtered enum
   *         values to provide to any recursion coming back into this method
   * @param  enumKeyValues the non-<code>null</code> new set of filtered enum
   *         values to provide to any recursion coming back into this method
   *         defined for map keys
   * @param  code the code to be executed with the current set of filtered enum
   *         values and enum filtered key values
   * @throws JsonMappingException if a mapping exception occurs
   */
  public void withEnumValuesDo(
    Set<Enum<?>> enumValues,
    Set<Enum<?>> enumKeyValues,
    EBiConsumer<Set<Enum<?>>, Set<Enum<?>>, JsonMappingException> code
  ) throws JsonMappingException {
    final Set<Enum<?>> oldvs = this.enumValues;
    final Set<Enum<?>> oldks = this.enumKeyValues;

    try {
      this.enumValues = enumValues;
      code.accept(oldvs, oldks);
    } finally {
      this.enumValues = oldvs;
      this.enumKeyValues = oldks;
    }
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.fasterxml.jackson.module.jsonSchema.factories.VisitorContext#addSeenSchemaUri(com.fasterxml.jackson.databind.JavaType)
   */
  @Override
  public String addSeenSchemaUri(JavaType aSeenSchema) {
    // if the javaType is for the same type as the enum values currently being kept
    // around then don't bother keeping track of it as we want to make sure
    // it is not re-used afterward
    if (aSeenSchema.isEnumType()
        && !enumValues.isEmpty()
        && aSeenSchema.getRawClass().isInstance(enumValues.iterator().next())) {
      return null;
    }
    return super.addSeenSchemaUri(aSeenSchema);
  }
};

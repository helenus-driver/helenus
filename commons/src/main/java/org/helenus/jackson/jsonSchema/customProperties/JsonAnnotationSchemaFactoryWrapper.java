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
package org.helenus.jackson.jsonSchema.customProperties;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import java.time.ZoneId;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
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
import com.fasterxml.jackson.module.jsonSchema.factories.MapVisitor;
import com.fasterxml.jackson.module.jsonSchema.factories.ObjectVisitor;
import com.fasterxml.jackson.module.jsonSchema.factories.ObjectVisitorDecorator;
import com.fasterxml.jackson.module.jsonSchema.factories.SchemaFactoryWrapper;
import com.fasterxml.jackson.module.jsonSchema.factories.StringVisitor;
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
import org.helenus.jackson.annotation.JsonPropertyDefaultValue;
import org.helenus.jackson.annotation.JsonPropertyDivisibleBy;
import org.helenus.jackson.annotation.JsonPropertyEnumValues;
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
  }

  /**
   * Gets the enum values based on the specified annotation.
   *
   * @author paouelle
   *
   * @param  ae the Json enum values annotation
   * @param  keys <code>true</code> if we are updating the schema for keys;
   *         <code>false</code> for values
   * @return a set of all provided enum values
   */
  private Set<String> getEnumValues(JsonPropertyEnumValues ae, boolean keys) {
    final Class<?>[] classes = keys ? ae.keySubTypesOf() : ae.valueSubTypesOf();
    Stream<String> s = Stream.empty();

    if (classes.length > 0) {
      final JsonSubTypes jst = ReflectionUtils.findFirstAnnotation(classes[0], JsonSubTypes.class);

      if (jst != null) {
        s = Stream.of(jst.value())
          .filter(t -> classes[0].isAssignableFrom(t.value()))
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
    return Stream.concat(
      Stream.of(keys ? ae.key() : ae.value()), s
    ).collect(Collectors.toCollection(LinkedHashSet::new));
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
   * @param which <code>true</code> if we are updating the schema for keys;
   *        <code>false</code> for map values and <code>null</code> for standard
   *        property
   */
  void updateSchema(
    JsonSchema cschema, JsonSchema schema, BeanProperty prop, Boolean which
  ) {
    final boolean keys = (which != null) && which;
    JavaType jtype = prop.getType();

    if (Optional.class.isAssignableFrom(jtype.getRawClass())) {
      jtype = jtype.getReferencedType();
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
      final JsonPropertyDefaultValue adv = prop.getAnnotation(JsonPropertyDefaultValue.class);

      if (adv != null) {
        sschema.setDefault(adv.value());
      }
    }
    if (schema instanceof ValueTypeSchema) {
      final ValueTypeSchema vtschema = (ValueTypeSchema)schema;
      final JsonPropertyEnumValues ae = prop.getAnnotation(JsonPropertyEnumValues.class);

      if (ae != null) {
        vtschema.setEnums(getEnumValues(ae, keys));
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
        boolean skipEnum = false;

      if (schema.isObjectSchema()) {
        final ObjectSchema oschema = (ObjectSchema)schema;
        final ObjectSchema.AdditionalProperties aprops = oschema.getAdditionalProperties();

        if (aprops instanceof MapSchemaAdditionalProperties) {
          skipEnum = true;
        }
      }
      if (!skipEnum) {
        final JsonPropertyEnumValues ae = prop.getAnnotation(JsonPropertyEnumValues.class);

        if (ae != null) {
          ctschema.setEnums(getEnumValues(ae, keys));
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
    if (schema.isObjectSchema()) {
      final ObjectSchema oschema = (ObjectSchema)schema;
      final ObjectSchema.AdditionalProperties aprops = oschema.getAdditionalProperties();

      if (aprops instanceof ObjectSchema.SchemaAdditionalProperties) {
        final ObjectSchema.SchemaAdditionalProperties saprops = (ObjectSchema.SchemaAdditionalProperties)aprops;
        final JsonSchema sschema = saprops.getJsonSchema();

        updateSchema(schema, sschema, prop, null);
      } else if (aprops instanceof MapSchemaAdditionalProperties) {
        final MapSchemaAdditionalProperties maprops = (MapSchemaAdditionalProperties)aprops;
        final JsonSchema kschema = maprops.getKeysSchema();
        final JsonSchema vschema = maprops.getValuesSchema();

        updateSchema(schema, kschema, prop, true);
        updateSchema(schema, vschema, prop, false);
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
          updateSchema(schema, ischema, prop, null);
        }
      } else if (items.isSingleItems()) {
        final ArraySchema.SingleItems sitems = (ArraySchema.SingleItems)items;
        final JsonSchema ischema = sitems.getSchema();

        updateSchema(schema, ischema, prop, null);
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
    return super.expectArrayFormat(type);
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
    final ObjectVisitor visitor = (ObjectVisitor)super.expectObjectFormat(type);
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
    return new ObjectVisitorDecorator(visitor) {
      @SuppressWarnings("synthetic-access")
      private JsonSchema getPropertySchema(BeanProperty writer) {
        final Map<String, JsonSchema> properties = ((ObjectSchema)getSchema()).getProperties();
        JsonSchema schema = properties.get(writer.getName());
        final JsonPropertyReference apr = writer.getAnnotation(JsonPropertyReference.class);

        if (apr != null) {
          JavaType jtype = writer.getType();

          if (Optional.class.isAssignableFrom(jtype.getRawClass())) {
            jtype = jtype.getReferencedType();
          }
          if (schema instanceof ArraySchema) {
            final ArraySchema aschema = (ArraySchema)schema;
            final ArraySchema.Items items = aschema.getItems();

            jtype = jtype.getContentType();
            if (items.isArrayItems()) {
              final ArraySchema.ArrayItems aitems = (ArraySchema.ArrayItems)items;
              final JsonSchema[] aschemas = aitems.getJsonSchemas();

              for (int i = 0; i < aitems.getJsonSchemas().length; i++) {
                if (aschemas[i] instanceof ObjectSchema) {
                  aschemas[i] = new ReferenceSchema(visitorContext.javaTypeToUrn(jtype));
                }
              }
            } else if (items.isSingleItems()) {
              final ArraySchema.SingleItems sitems = (ArraySchema.SingleItems)items;
              final JsonSchema ischema = sitems.getSchema();

              if (ischema instanceof ObjectSchema) {
                sitems.setSchema(new ReferenceSchema(visitorContext.javaTypeToUrn(jtype)));
              }
            }
          } else if (schema instanceof ObjectSchema) {
            final ObjectSchema oschema = (ObjectSchema)schema;
            final ObjectSchema.AdditionalProperties aprops = oschema.getAdditionalProperties();

            if (aprops instanceof MapSchemaAdditionalProperties) {
              final MapSchemaAdditionalProperties maprops = (MapSchemaAdditionalProperties)aprops;
              final JsonSchema kschema = maprops.getKeysSchema();

              if (kschema instanceof ObjectSchema) {
                maprops.setKeysSchema(new ReferenceSchema(visitorContext.javaTypeToUrn(maprops.getKeysType())));
              }
            } else {
              schema = new ReferenceSchema(visitorContext.javaTypeToUrn(jtype));
            }
          }
          properties.put(writer.getName(), schema);
        }
        if (schema instanceof ObjectSchema) {
          final JsonPropertyKeyReference apkr = writer.getAnnotation(JsonPropertyKeyReference.class);

          if (apkr != null) {
            final ObjectSchema oschema = (ObjectSchema)schema;
            final ObjectSchema.AdditionalProperties aprops = oschema.getAdditionalProperties();

            if (aprops instanceof MapSchemaAdditionalProperties) {
              final MapSchemaAdditionalProperties maprops = (MapSchemaAdditionalProperties)aprops;
              final JsonSchema vschema = maprops.getValuesSchema();

              if (vschema instanceof ObjectSchema) {
                maprops.setValuesSchema(new ReferenceSchema(visitorContext.javaTypeToUrn(maprops.getValuesType())));
              }
            }
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
      @Override
      public void optionalProperty(BeanProperty writer) throws JsonMappingException {
        if (isIncluded(writer)) {
          super.optionalProperty(writer);
          final JsonSchema schema = getPropertySchema(writer);

          updateSchema(null, schema, writer, null);
          updateForTypeId(schema, writer);
        }
      }
      @Override
      public void property(BeanProperty writer) throws JsonMappingException {
        if (isIncluded(writer)) {
          super.property(writer);
          final JsonSchema schema = getPropertySchema(writer);

          updateSchema(null, schema, writer, null);
          updateForTypeId(schema, writer);
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
    final MapVisitor visitor = (MapVisitor)super.expectMapFormat(type);

    return new MapVisitor(visitor.getProvider(), visitor.getSchema()) {
      @Override
      public void keyFormat(JsonFormatVisitable handler, JavaType keyType)
        throws JsonMappingException {
        MapSchemaAdditionalProperties aprops = (MapSchemaAdditionalProperties)schema.getAdditionalProperties();

        if (aprops == null) {
          aprops = new MapSchemaAdditionalProperties();
          schema.setAdditionalProperties(aprops);
        }
        if (handler instanceof StdKeySerializers.StringKeySerializer) {
          // special handling for keys such that they don't get their schema
          // generated as an ANY schema
          handler = new StringSerializer();
        } else if (handler instanceof JsonValueSerializer) {
          final JsonValueSerializer jvhandler = (JsonValueSerializer)handler;

          if (jvhandler.handledType().equals(String.class)) {
            // special handling for keys that are not string but are of a class that
            // is annotated with @JsonValue which returns a string such that they
            // don't get their schema generated as an ANY schema
            handler = new StringSerializer();
          }
        }
        final JsonSchema schema = propertySchema(handler, keyType);

        aprops.setKeysSchema(schema);
        aprops.setKeysType(keyType);
      }
      @Override
      public void valueFormat(JsonFormatVisitable handler, JavaType valueType)
        throws JsonMappingException {
        MapSchemaAdditionalProperties aprops = (MapSchemaAdditionalProperties)schema.getAdditionalProperties();

        if (aprops == null) {
          aprops = new MapSchemaAdditionalProperties();
          schema.setAdditionalProperties(aprops);
        }
        final JsonSchema schema = propertySchema(handler, valueType);

        aprops.setValuesSchema(schema);
        aprops.setValuesType(valueType);
      }
    };
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
 * The <code>MapSchemaAdditionalProperties</code> class defines additional
 * properties used for a map to represent both the keys and values schemas.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Oct 29, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
class MapSchemaAdditionalProperties extends ObjectSchema.AdditionalProperties {
  /**
   * Holds the schema for the map keys.
   *
   * @author paouelle
   */
  @JsonProperty("keys")
  private JsonSchema keysSchema;

  /**
   * Holds the type for keys.
   *
   * @author paouelle
   */
  @JsonIgnore
  private JavaType keysType;

  /**
   * Holds the schema for the map values.
   *
   * @author paouelle
   */
  @JsonProperty("values")
  private JsonSchema valuesSchema;

  /**
   * Holds the type for values.
   *
   * @author paouelle
   */
  @JsonIgnore
  private JavaType valuesType;

  /**
   * Gets the schema for the map keys.
   *
   * @author paouelle
   *
   * @return the schema for the map keys
   */
  public JsonSchema getKeysSchema() {
    return keysSchema;
  }

  /**
   * Sets the schema for the map keys.
   *
   * @author paouelle
   *
   * @param keys the schema for the map keys
   */
  public void setKeysSchema(JsonSchema keys) {
    this.keysSchema = keys;
  }

  /**
   * Gets the type for the map keys.
   *
   * @author paouelle
   *
   * @return the type for the map keys
   */
  public JavaType getKeysType() {
    return keysType;
  }

  /**
   * Sets the type for the map keys.
   *
   * @author paouelle
   *
   * @param keys the type for the map keys
   */
  public void setKeysType(JavaType keys) {
    this.keysType = keys;
  }

  /**
   * Gets the schema for the map values.
   *
   * @author paouelle
   *
   * @return the schema for the map values
   */
  public JsonSchema getValuesSchema() {
    return valuesSchema;
  }

  /**
   * Sets the schema for the map values.
   *
   * @author paouelle
   *
   * @param values the schema for the map values
   */
  public void setValuesSchema(JsonSchema values) {
    this.valuesSchema = values;
  }

  /**
   * Gets the type for the map values.
   *
   * @author paouelle
   *
   * @return the type for the map values
   */
  public JavaType getValuesType() {
    return valuesType;
  }

  /**
   * Sets the type for the map values.
   *
   * @author paouelle
   *
   * @param values the type for the map values
   */
  public void setValuesType(JavaType values) {
    this.valuesType = values;
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
    return Objects.hash(keysSchema, valuesSchema);
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
    return (
      (obj instanceof MapSchemaAdditionalProperties)
      && Objects.equals(keysSchema, ((MapSchemaAdditionalProperties)obj).keysSchema)
      && Objects.equals(valuesSchema, ((MapSchemaAdditionalProperties)obj).valuesSchema)
    );
  }
}

/**
 * The <code>JsonAnnotationSchemaFactoryWrapperFactory</code> class defines a factory
 * for the {@link JsonAnnotationSchemaFactoryWrapper} class.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
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

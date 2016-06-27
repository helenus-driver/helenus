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
package org.helenus.jackson.jsonSchema.types;

import java.util.Objects;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.module.jsonSchema.JsonSchema;
import com.fasterxml.jackson.module.jsonSchema.types.ObjectSchema;

/**
 * The <code>MapTypesSchema</code> class extends the {@link ObjectSchema}
 * class to provide additional information about the Java map being defined.
 *
 * @copyright 2015-2016 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jun 27, 2016 - paouelle - Creation
 *
 * @since 1.0
 */
public class MapTypesSchema extends ObjectTypesSchema {
  /**
   * Instantiates a new <code>MapTypesSchema</code> object.
   *
   * @author paouelle
   *
   * @param  schema the object schema representing the map type to copy
   * @throws IllegalArgumentException if the schema doesn't represent a map
   */
  public MapTypesSchema(ObjectTypesSchema schema) {
    super(schema);
    org.apache.commons.lang3.Validate.isTrue(
      schema.getAdditionalProperties() instanceof AdditionalProperties,
      "schema doesn't represent a map"
    );
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.fasterxml.jackson.module.jsonSchema.types.ObjectSchema#getAdditionalProperties()
   */
  @Override
  public AdditionalProperties getAdditionalProperties() {
    return (AdditionalProperties)super.getAdditionalProperties();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @throws IllegalArgumentException if the properties doesn't represent
   *         additional map properties
   *
   * @see com.fasterxml.jackson.module.jsonSchema.types.ObjectSchema#setAdditionalProperties(com.fasterxml.jackson.module.jsonSchema.types.ObjectSchema.AdditionalProperties)
   */
  @Override
  public void setAdditionalProperties(ObjectSchema.AdditionalProperties additionalProperties) {
    org.apache.commons.lang3.Validate.isTrue(
      additionalProperties instanceof AdditionalProperties,
      "schema doesn't represent map additional properties"
    );
    super.setAdditionalProperties(additionalProperties);
  }

  /**
   * Instantiates a new <code>MapTypesSchema</code> object.
   *
   * @author paouelle
   */
  public MapTypesSchema() {
    super.setAdditionalProperties(new AdditionalProperties());
  }

  /**
   * Gets the schema for the map keys.
   *
   * @author paouelle
   *
   * @return the schema for the map keys
   */
  public JsonSchema getKeysSchema() {
    return getAdditionalProperties().getKeysSchema();
  }

  /**
   * Sets the schema for the map keys.
   *
   * @author paouelle
   *
   * @param keys the schema for the map keys
   */
  public void setKeysSchema(JsonSchema keys) {
    getAdditionalProperties().setKeysSchema(keys);
  }

  /**
   * Gets the type for the map keys.
   *
   * @author paouelle
   *
   * @return the type for the map keys
   */
  public JavaType getKeysType() {
    return getAdditionalProperties().getKeysType();
  }

  /**
   * Sets the type for the map keys.
   *
   * @author paouelle
   *
   * @param keys the type for the map keys
   */
  public void setKeysType(JavaType keys) {
    getAdditionalProperties().setKeysType(keys);
  }

  /**
   * Gets the schema for the map values.
   *
   * @author paouelle
   *
   * @return the schema for the map values
   */
  public JsonSchema getValuesSchema() {
    return getAdditionalProperties().getValuesSchema();
  }

  /**
   * Sets the schema for the map values.
   *
   * @author paouelle
   *
   * @param values the schema for the map values
   */
  public void setValuesSchema(JsonSchema values) {
    getAdditionalProperties().setValuesSchema(values);
  }

  /**
   * Gets the type for the map values.
   *
   * @author paouelle
   *
   * @return the type for the map values
   */
  public JavaType getValuesType() {
    return getAdditionalProperties().getValuesType();
  }

  /**
   * Sets the type for the map values.
   *
   * @author paouelle
   *
   * @param values the type for the map values
   */
  public void setValuesType(JavaType values) {
    getAdditionalProperties().setValuesType(values);
  }

  /**
   * The <code>AdditionalProperties</code> class defines additional
   * properties used for a map to represent both the keys and values schemas.
   *
   * @copyright 2015-2015 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Oct 29, 2015 - paouelle - Creation
   * @version 2 - Jun 24, 2016 - paouelle - Moved to its own public class
   *
   * @since 1.0
   */
  public static class AdditionalProperties extends ObjectSchema.AdditionalProperties {
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
        (obj instanceof AdditionalProperties)
        && Objects.equals(keysSchema, ((AdditionalProperties)obj).keysSchema)
        && Objects.equals(valuesSchema, ((AdditionalProperties)obj).valuesSchema)
      );
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
      return ReflectionToStringBuilder.toString(this);
    }
  }
}

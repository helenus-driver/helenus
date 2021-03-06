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
package org.helenus.driver.info;

import java.lang.annotation.Annotation;

import org.helenus.driver.persistence.ClusteringKey;
import org.helenus.driver.persistence.Index;
import org.helenus.driver.persistence.KeyspaceKey;
import org.helenus.driver.persistence.PartitionKey;
import org.helenus.driver.persistence.TypeKey;

/**
 * The <code>FieldInfo</code> interface provides information about a specific
 * field in a given table for a particular POJO class.
 *
 * @copyright 2015-2016 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 15, 2015 - paouelle - Creation
 *
 * @param <T> The type of POJO represented by this class
 *
 * @since 2.0
 */
public interface FieldInfo<T> {
  /**
   * Gets the class of POJO represented by this class info object.
   *
   * @author paouelle
   *
   * @return the non-<code>null</code> type of POJO represented by this class
   *         info
   */
  public Class<T> getObjectClass();

  /**
   * Gets the class declaring this field.
   *
   * @author paouelle
   *
   * @return the non-<code>null</code> class declaring this field
   */
  public Class<?> getDeclaringClass();

  /**
   * Gets the class info for the POJO.
   *
   * @author paouelle
   *
   * @return the non-<code>null</code> class info for the POJO
   */
  public ClassInfo<T> getClassInfo();

  /**
   * Gets the table info this field is defined in.
   *
   * @author paouelle
   *
   * @return the table info this field is defined in or <code>null</code>
   *         if it represents only a keyspace key and not a column
   */
  public TableInfo<T> getTableInfo();

  /**
   * Gets the field name.
   *
   * @author paouelle
   *
   * @return the non-<code>null</code> field name
   */
  public String getName();

  /**
   * Gets the type for this field.
   *
   * @author paouelle
   *
   * @return the non-<code>null</code> type for this field
   */
  public Class<?> getType();

  /**
   * Checks if the field is annotated as a column.
   *
   * @author paouelle
   *
   * @return <code>true</code> if the field is annotated as a column;
   *         <code>false</code> otherwise
   */
  public boolean isColumn();

  /**
   * Checks if this field is annotated as a static column.
   *
   * @author paouelle
   *
   * @return <code>true</code> if this field is annotated as a static column;
   *         <code>false</code> otherwise
   */
  public boolean isStatic();

  /**
   * Checks if this field is annotated as a keyspace key.
   *
   * @author paouelle
   *
   * @return <code>true</code> if this field is annotated as a keyspace key;
   *         <code>false</code> otherwise
   */
  public boolean isKeyspaceKey();

  /**
   * Gets the column name for this field.
   *
   * @author paouelle
   *
   * @return the column name for this field if it is annotated as a column;
   *         <code>null</code> otherwise
   */
  public String getColumnName();

  /**
   * Gets the keyspace key name for this field.
   *
   * @author paouelle
   *
   * @return the keyspace key name for this field if it is annotated as a keyspace
   *         key; <code>null</code> otherwise
   */
  public String getKeyspaceKeyName();

  /**
   * Gets the keyspace key annotation for this field.
   *
   * @author paouelle
   *
   * @return the keyspace key annotation for this field if it is annotated as a
   *         keyspace key; <code>null</code> otherwise
   */
  public KeyspaceKey getKeyspaceKey();

  /**
   * Checks if this field is mandatory.
   *
   * @author paouelle
   *
   * @return <code>true</code> if this field is mandatory and cannot be
   *         <code>null</code>; <code>false</code> otherwise
   */
  public boolean isMandatory();

  /**
   * Checks if this field is an index.
   *
   * @author paouelle
   *
   * @return <code>true</code> if this field is an index; <code>false</code>
   *         otherwise
   */
  public boolean isIndex();

  /**
   * Gets the index annotation for this field.
   *
   * @author paouelle
   *
   * @return the index annotation for this field if this field is an index;
   *         <code>null</code> otherwise
   */
  public Index getIndex();

  /**
   * Checks if this field is a counter.
   *
   * @author paouelle
   *
   * @return <code>true</code> if this field is a counter; <code>false</code>
   *         otherwise
   */
  public boolean isCounter();

  /**
   * Checks if this column is the last key in the partition or the cluster if
   * this field represents a primary key.
   *
   * @author paouelle
   *
   * @return <code>true</code> if this key is the last in the partition or the
   *         cluster; <code>false</code> otherwise
   */
  public boolean isLast();

  /**
   * Checks if this field is also a partition key.
   *
   * @author paouelle
   *
   * @return <code>true</code> if this field is a partition key; <code>false</code>
   *         otherwise
   */
  public boolean isPartitionKey();

  /**
   * Gets the partition key annotation for this field.
   *
   * @author paouelle
   *
   * @return the partition key annotation for this field if it is defined as
   *         a partition key; <code>null</code> otherwise
   */
  public PartitionKey getPartitionKey();

  /**
   * Checks if this field is also a clustering key.
   *
   * @author paouelle
   *
   * @return <code>true</code> if this field is a clustering key; <code>false</code>
   *         otherwise
   */
  public boolean isClusteringKey();

  /**
   * Gets the clustering key annotation for this field.
   *
   * @author paouelle
   *
   * @return the clustering key annotation for this field if it is defined as
   *         a clustering key; <code>null</code> otherwise
   */
  public ClusteringKey getClusteringKey();

  /**
   * Checks if this field is annotated as a case insensitive partition or
   * clustering key.
   *
   * @author paouelle
   *
   * @return <code>true</code> if this field is annotated as a case insensitive
   *         partition or clustering key; <code>false</code> otherwise
   */
  public boolean isCaseInsensitiveKey();

  /**
   * Checks if this field is also a type key.
   *
   * @author paouelle
   *
   * @return <code>true</code> if this field is a type key; <code>false</code>
   *         otherwise
   */
  public boolean isTypeKey();

  /**
   * Gets the type key annotation for this field.
   *
   * @author paouelle
   *
   * @return the type key annotation for this field if it is defined as
   *         a type key; <code>null</code> otherwise
   */
  public TypeKey getTypeKey();

  /**
   * Checks if this field is a multi-key.
   *
   * @author paouelle
   *
   * @return <code>true</code> if the field is a multi-key;
   *         <code>false</code> otherwise
   */
  public boolean isMultiKey();

  /**
   * Gets this field's annotation for the specified type if such an annotation
   * is present.
   *
   * @param <A> the type of annotation to retrieve
   *
   * @param  annotationClass the Class object corresponding to the
   *         annotation type
   * @return this element's annotation for the specified annotation type if
   *         present on this element; otherwise <code>null</code>
   * @throws NullPointerException if the given annotation class is <code>null</code>
   */
  public <A extends Annotation> A getAnnotation(Class<A> annotationClass);

  /**
   * Checks if the field is defined as final.
   *
   * @author paouelle
   *
   * @return <code>true</code> if the field is defined as final; <code>false</code>
   *         otherwise
   */
  public boolean isFinal();
}

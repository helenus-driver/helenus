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
package org.helenus.jackson.jdatabind;

import java.lang.annotation.Annotation;
import java.lang.annotation.Repeatable;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;

import com.fasterxml.jackson.annotation.JsonFormat.Value;
import com.fasterxml.jackson.databind.AnnotationIntrospector;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.PropertyMetadata;
import com.fasterxml.jackson.databind.PropertyName;
import com.fasterxml.jackson.databind.introspect.AnnotatedMember;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonObjectFormatVisitor;

/**
 * The <code>ExtendedBeanProperty</code> class extends on Jackson's
 * {@link BeanProperty} to provide support for querying repeatable annotations.
 *
 * @copyright 2015-2016 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jul 21, 2016 - paouelle - Creation
 *
 * @since 1.0
 */
public class ExtendedBeanProperty implements BeanProperty {
  /**
   * Holds the bean being wrapped by this class.
   *
   * @author paouelle
   */
  private final BeanProperty bean;

  /**
   * Instantiates a new <code>ExtendedBeanProperty</code> object.
   *
   * @author paouelle
   *
   * @param bean the bean to wrap and extend
   */
  public ExtendedBeanProperty(BeanProperty bean) {
    this.bean = bean;
  }

  /**
   * Gets the underlying bean.
   *
   * @author paouelle
   *
   * @return the non-<code>null</code> underlying bean
   */
  public BeanProperty getBean() {
    return bean;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.fasterxml.jackson.databind.BeanProperty#getName()
   */
  @Override
  public String getName() {
    return bean.getName();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.fasterxml.jackson.databind.BeanProperty#getFullName()
   */
  @Override
  public PropertyName getFullName() {
    return bean.getFullName();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.fasterxml.jackson.databind.BeanProperty#getType()
   */
  @Override
  public JavaType getType() {
    return bean.getType();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.fasterxml.jackson.databind.BeanProperty#getWrapperName()
   */
  @Override
  public PropertyName getWrapperName() {
    return bean.getWrapperName();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.fasterxml.jackson.databind.BeanProperty#getMetadata()
   */
  @Override
  public PropertyMetadata getMetadata() {
    return bean.getMetadata();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.fasterxml.jackson.databind.BeanProperty#isRequired()
   */
  @Override
  public boolean isRequired() {
    return bean.isRequired();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.fasterxml.jackson.databind.BeanProperty#getAnnotation(java.lang.Class)
   */
  @Override
  public <A extends Annotation> A getAnnotation(Class<A> acls) {
    return bean.getAnnotation(acls);
  }

  /**
   * Method for finding annotations associated with this property;
   * meaning annotations associated with one of entities used to
   * access property.
   * <p>
   * <i>Note:</i> This method should only be called for custom annotations;
   * access to standard Jackson annotations (or ones supported by
   * alternate {@link AnnotationIntrospector}s) should be accessed
   * through {@link AnnotationIntrospector}.
   * <p>
   * The difference between this method and {@link #getAnnotation(Class)}
   * is that this method detects if its argument is a <em>repeatable
   * annotation type</em> (JLS 9.6), and if so, attempts to find one or
   * more annotations of that type by "looking through" a container
   * annotation.
   * <p>
   * The caller of this method is free to modify the returned array; it will
   * have no effect on the arrays returned to other callers.
   *
   * @author paouelle
   *
   * @param  acls the class object corresponding to the annotation type
   * @return all this property's annotations for the specified annotation type
   *         if associated with this element, else an array of length zero
   * @throws NullPointerException if the given annotation class is <code>null</code>
   * @throws InternalError if the annotation is repeatable but not defined properly
   */
  @SuppressWarnings("unchecked")
  public <A extends Annotation> A[] getAnnotationByType(Class<A> acls) {
    // first check if the property is annotated once
    final A a = getAnnotation(acls);

    if (a != null) {
      final A[] as = (A[])Array.newInstance(acls, 1);

      Array.set(as, 0, a);
      return as;
    }
    // check if the annotation is repeatable so we can get to its container
    final Repeatable r = acls.getAnnotation(Repeatable.class);

    if (r != null) {
      final Annotation ca = getAnnotation(r.value());

      if (ca != null) {
        try {
          return (A[])org.apache.commons.lang3.reflect.MethodUtils.invokeExactMethod(ca, "value");
        } catch (InvocationTargetException e) {
          throw new InternalError(e.getTargetException());
        } catch (NoSuchMethodException|IllegalAccessException e) {
          throw new InternalError(e);
        }
      }
    }
    return (A[])Array.newInstance(acls, 0);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.fasterxml.jackson.databind.BeanProperty#getContextAnnotation(java.lang.Class)
   */
  @Override
  public <A extends Annotation> A getContextAnnotation(Class<A> acls) {
    return bean.getContextAnnotation(acls);
  }

  /**
   * Method for finding annotation associated with context of
   * this property; usually class in which member is declared
   * (or its subtype if processing subtype).
   * <p>
   * <i>Note:</i> This method should only be called for custom annotations;
   * access to standard Jackson annotations (or ones supported by
   * alternate {@link AnnotationIntrospector}s) should be accessed
   * through {@link AnnotationIntrospector}.
   * <p>
   * The difference between this method and {@link #getContextAnnotation(Class)}
   * is that this method detects if its argument is a <em>repeatable
   * annotation type</em> (JLS 9.6), and if so, attempts to find one or
   * more annotations of that type by "looking through" a container
   * annotation.
   * <p>
   * The caller of this method is free to modify the returned array; it will
   * have no effect on the arrays returned to other callers.
   *
   * @author paouelle
   *
   * @param  acls the class object corresponding to the annotation type
   * @return all this property's context annotations for the specified annotation type
   *         if associated with this element, else an array of length zero
   * @throws NullPointerException if the given annotation class is <code>null</code>
   * @throws InternalError if the annotation is repeatable but not defined properly
   */
  @SuppressWarnings("unchecked")
  public <A extends Annotation> A[] getContextAnnotationByType(Class<A> acls) {
    // first check if the property is annotated once
    final A a = getContextAnnotation(acls);

    if (a != null) {
      final A[] as = (A[])Array.newInstance(acls, 1);

      Array.set(as, 0, a);
      return as;
    }
    // check if the annotation is repeatable so we can get to its container
    final Repeatable r = acls.getAnnotation(Repeatable.class);

    if (r != null) {
      final Annotation ca = getContextAnnotation(r.value());

      if (ca != null) {
        try {
          return (A[])org.apache.commons.lang3.reflect.MethodUtils.invokeExactMethod(ca, "value");
        } catch (InvocationTargetException e) {
          throw new InternalError(e.getTargetException());
        } catch (NoSuchMethodException|IllegalAccessException e) {
          throw new InternalError(e);
        }
      }
    }
    return (A[])Array.newInstance(acls, 0);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.fasterxml.jackson.databind.BeanProperty#getMember()
   */
  @Override
  public AnnotatedMember getMember() {
    return bean.getMember();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.fasterxml.jackson.databind.BeanProperty#findFormatOverrides(com.fasterxml.jackson.databind.AnnotationIntrospector)
   */
  @Override
  public Value findFormatOverrides(AnnotationIntrospector intr) {
    return bean.findFormatOverrides(intr);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.fasterxml.jackson.databind.BeanProperty#depositSchemaProperty(com.fasterxml.jackson.databind.jsonFormatVisitors.JsonObjectFormatVisitor)
   */
  @Override
  public void depositSchemaProperty(JsonObjectFormatVisitor objectVisitor)
    throws JsonMappingException {
    bean.depositSchemaProperty(objectVisitor);
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
    return bean.getMember().toString() + " (" + bean.getFullName() + ")";
  }
}

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
package org.helenus.commons.lang3.reflect;

import java.lang.annotation.Annotation;
import java.lang.annotation.Repeatable;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import java.io.File;
import java.io.IOException;

import java.net.JarURLConnection;
import java.net.URL;

import java.security.AccessController;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.stream.Stream;

import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.AnnotationValue;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.PackageElement;
import javax.lang.model.type.ArrayType;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;

import org.helenus.annotation.Keyable;

/**
 * The <code>ReflectionUtils</code> class provides reflection utilities.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 15, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
public class ReflectionUtils {
  /**
   * Reflection factory for obtaining constructors used to instantiate blank
   * object whether or not a default constructor was defined by the user.
   *
   * @author paouelle
   */
  @SuppressWarnings("all")
  private static final sun.reflect.ReflectionFactory reflFactory
    = (sun.reflect.ReflectionFactory)AccessController.doPrivileged(
        new sun.reflect.ReflectionFactory.GetReflectionFactoryAction()
      );

  /**
   * Finds the first annotation from a given class' and interface's hierarchy of
   * the specified annotation.
   *
   * @author paouelle
   *
   * @param <A> the type of annotation to search for
   *
   * @param  clazz the class from which to search
   * @param  annotationClass the annotation to search for
   * @return the first annotation in <code>clazz</code>'s hierarchy or
   *         <code>null</code> if none is found
   * @throws NullPointerException if <code>clazz</code> or
   *         <code>annotationClass</code> is <code>null</code>
   */
  public static <A extends Annotation> A findFirstAnnotation(
    Class<?> clazz, Class<A> annotationClass
  ) {
    org.apache.commons.lang3.Validate.notNull(clazz, "invalid null class");
    org.apache.commons.lang3.Validate.notNull(
      annotationClass, "invalid null annotation class"
    );
    Class<?> c = clazz;

    do {
      final A[] as = c.getDeclaredAnnotationsByType(annotationClass);

      if (as.length > 0) {
        return as[0];
      }
      for (final Class<?> i: c.getInterfaces()) {
        final A a = ReflectionUtils.findFirstAnnotation(i, annotationClass);

        if (a != null) {
          return a;
        }
      }
      c = c.getSuperclass();
    } while (c != null);
    return null;
  }

  /**
   * Finds the first class from a given class' hierarchy that is annotated
   * with the specified annotation.
   *
   * @author paouelle
   *
   * @param <T> the type of the class to start searching from
   *
   * @param  clazz the class from which to search
   * @param  annotationClass the annotation to search for
   * @return the first class in <code>clazz</code>'s hierarchy annotated with
   *         the specified annotation or <code>null</code> if none is found
   * @throws NullPointerException if <code>clazz</code> or
   *         <code>annotationClass</code> is <code>null</code>
   */
  public static <T> Class<? super T> findFirstClassAnnotatedWith(
    Class<T> clazz, Class<? extends Annotation> annotationClass
  ) {
    org.apache.commons.lang3.Validate.notNull(clazz, "invalid null class");
    org.apache.commons.lang3.Validate.notNull(
      annotationClass, "invalid null annotation class"
    );
    Class<? super T> c = clazz;

    do {
      if (c.getDeclaredAnnotationsByType(annotationClass).length > 0) {
        return c;
      }
      c = c.getSuperclass();
    } while (c != null);
    return null;
  }

  /**
   * Finds the first class from a given class' hierarchy that is not annotated
   * with the specified annotation.
   *
   * @author paouelle
   *
   * @param <T> the type of the class to start searching from
   *
   * @param  clazz the class from which to search
   * @param  annotationClass the annotation to search for
   * @return the first class in <code>clazz</code>'s hierarchy not annotated
   *         with the specified annotation or <code>null</code> if none found
   * @throws NullPointerException if <code>clazz</code> or
   *         <code>annotationClass</code> is <code>null</code>
   */
  public static <T> Class<? super T> findFirstClassNotAnnotatedWith(
    Class<T> clazz, Class<? extends Annotation> annotationClass
  ) {
    org.apache.commons.lang3.Validate.notNull(clazz, "invalid null class");
    org.apache.commons.lang3.Validate.notNull(
      annotationClass, "invalid null annotation class"
    );
    Class<? super T> c = clazz;

    while ((c != null)
           && (c.getDeclaredAnnotationsByType(annotationClass).length > 0)) {
      c = c.getSuperclass();
    }
    return c;
  }

  /**
   * Finds the last class from a given class' hierarchy that is annotated
   * with the specified annotation.
   *
   * @author paouelle
   *
   * @param <T> the type of the class to start searching from
   *
   * @param  clazz the class from which to search
   * @param  annotationClass the annotation to search for
   * @return the last class in <code>clazz</code>'s hierarchy annotated with
   *         the specified annotation or <code>null</code> if none found
   * @throws NullPointerException if <code>clazz</code> or
   *         <code>annotationClass</code> is <code>null</code>
   */
  public static <T> Class<? super T> findLastClassAnnotatedWith(
    Class<T> clazz, Class<? extends Annotation> annotationClass
  ) {
    org.apache.commons.lang3.Validate.notNull(clazz, "invalid null class");
    org.apache.commons.lang3.Validate.notNull(
      annotationClass, "invalid null annotation class"
    );
    Class<? super T> p = null;
    Class<? super T> c = clazz;

    while ((c != null)
        && (c.getDeclaredAnnotationsByType(annotationClass).length > 0)) {
      p = c;
      c = c.getSuperclass();
    }
    return p;
  }

  /**
   * Gets the serialization constructor for the given class based on the given
   * base class. The specified base class is expected to have a default constructor
   * to use for initializing the hierarchy from that point on. The constructor
   * can have any visibility (i.e. public, protected, package private, or private).
   * <p>
   * <i>Note:</i> The returned constructor results in having a proper object of
   * the given class instantiated without calling any of the constructors from
   * its class or super classes. The fields of all classes in the hierarchy will
   * not be initialized; thus set using default Java initialization (i.e.
   * <code>null</code>, 0, 0L, false, ...). This is how Java creates new
   * serialized objects before calling the <code>readObject()</code> method of
   * each of the subclasses.
   *
   * @author paouelle
   *
   * @param <T> the class for which to get the serialization constructor
   *
   * @param  clazz the class for which to get the serialization constructor
   * @param  bclass the base class to start the initialization from in the
   *         returned serialization constructor
   * @return the non-<code>null</code> serialization constructor for the given
   *         class
   * @throws NullPointerException if <code>clazz</code> or <code>bclass</code>
   *         is <code>null</code>
   * @throws IllegalArgumentException if <code>bclass</code> is not a base class
   *         for <code>clazz</code>
   * @throws SecurityException if the request is denied
   */
  public static <T> Constructor<T> getSerializationConstructorFromBaseClass(
    Class<T> clazz, Class<?> bclass
  ) {
    org.apache.commons.lang3.Validate.notNull(clazz, "invalid null class");
    org.apache.commons.lang3.Validate.notNull(bclass, "invalid null base class");
    org.apache.commons.lang3.Validate.isTrue(
      bclass.isAssignableFrom(clazz),
      bclass.getName() + " is not a superclass of " + clazz.getName()
    );
    final Constructor<?> ctor;

    try { // find the default ctor for the base class
      ctor = bclass.getDeclaredConstructor();
      ctor.setAccessible(true);
    } catch (NoSuchMethodException e) {
      throw new IllegalStateException(
        "missing default constructor for base class: " + bclass.getName(), e
      );
    }
    @SuppressWarnings({"restriction", "unchecked"})
    final Constructor<T> nc
      = (Constructor<T>)ReflectionUtils.reflFactory.newConstructorForSerialization(
          clazz, ctor
        );

    nc.setAccessible(true);
    return nc;
  }

  /**
   * Gets the serialization constructor for the given class based on the given
   * annotation. The annotation is used to find the first class in its hierarchy
   * that is not annotated with the annotation and expect a default constructor
   * to use for initializing the hierarchy from that point on. The constructor
   * can have any visibility (i.e. public, protected, package private, or private).
   * <p>
   * <i>Note:</i> The returned constructor results in having a proper object of
   * the given class instantiated without calling any of the constructors from
   * its class or super classes. The fields of all classes in the hierarchy will
   * not be initialized; thus set using default Java initialization (i.e.
   * <code>null</code>, 0, 0L, false, ...). This is how Java creates new
   * serialized objects before calling the <code>readObject()</code> method of
   * each of the subclasses.
   *
   * @author paouelle
   *
   * @param <T> the class for which to get the serialization constructor
   *
   * @param  clazz the class for which to get the serialization constructor
   * @param  annotationClass the annotation to search with
   * @return the non-<code>null</code> serialization constructor for the given
   *         class
   * @throws NullPointerException if <code>clazz</code> or
   *         <code>annotationClass</code> is <code>null</code>
   * @throws IllegalStateException if unable to find a default constructor for
   *         the first base class not annotated with <code>annotationClass</code>
   * @throws SecurityException if the request is denied
   */
  public static <T> Constructor<T> getSerializationConstructorFromAnnotation(
    Class<T> clazz, Class<? extends Annotation> annotationClass
  ) {
    org.apache.commons.lang3.Validate.notNull(clazz, "invalid null class");
    org.apache.commons.lang3.Validate.notNull(
      annotationClass, "invalid null annotation class"
    );
    final Class<?> bclass = ReflectionUtils.findFirstClassNotAnnotatedWith(
      clazz, annotationClass
    );
    return ReflectionUtils.getSerializationConstructorFromBaseClass(clazz, bclass);
  }

  /**
   * Gets the declared members of the given type from the specified class.
   *
   * @author paouelle
   *
   * @param <T> the type of member
   *
   * @param  type the type of members to retrieve
   * @param  clazz the class from which to retrieve the members
   * @return the non-<code>null</code> members of the given type from the
   *         specified class
   * @throws NullPointerException if <code>type</code> or
   *         <code>clazz</code> is <code>null</code>
   * @throws IllegalArgumentException if <code>type</code> is not
   *         {@link Field}, {@link Method}, or {@link Constructor}
   */
  @SuppressWarnings("unchecked")
  public static <T extends Member> T[] getDeclaredMembers(
    Class<T> type, Class<?> clazz
  ) {
    org.apache.commons.lang3.Validate.notNull(type, "invalid null member type");
    org.apache.commons.lang3.Validate.notNull(clazz, "invalid null class");
    if (type == Field.class) {
      return (T[])clazz.getDeclaredFields();
    } else if (type == Method.class) {
      return (T[])clazz.getDeclaredMethods();
    } else if (type == Constructor.class) {
      return (T[])clazz.getDeclaredConstructors();
    } else {
      throw new IllegalArgumentException(
        "invalid member class: " + type.getName()
      );
    }
  }

  /**
   * Gets all members of a given type (up the super class hierarchy) annotated
   * with the specified annotation.
   *
   * @author paouelle
   *
   * @param <T> the type of members to retrieve (either {@link Field},
   *            {@link Method}, or {@link Constructor})
   *
   * @param  type the type of members to retrieve
   * @param  clazz the class from which to find all annotated members
   * @param  annotation the annotation for which to find all members
   * @param  up <code>true</code> to look up the class hierarchy;
   *         <code>false</code> to only look at the specified class level
   * @return a list in the provided order for all annotated members
   * @throws NullPointerException if <code>type</code>,
   *         <code>clazz</code> or <code>annotation</code> is <code>null</code>
   * @throws IllegalArgumentException if <code>type</code> is not
   *         {@link Field}, {@link Method}, or {@link Constructor}
   */
  public static <T extends Member> List<T> getAllMembersAnnotatedWith(
    Class<T> type,
    Class<?> clazz,
    Class<? extends Annotation> annotation,
    boolean up
  ) {
    org.apache.commons.lang3.Validate.notNull(type, "invalid null member type");
    org.apache.commons.lang3.Validate.notNull(clazz, "invalid null class");
    org.apache.commons.lang3.Validate.notNull(annotation, "invalid null annotation class");
    final LinkedList<Class<?>> classes = new LinkedList<>();

    if (up) {
      while (clazz != null) {
        classes.push(clazz);
        clazz = clazz.getSuperclass();
      }
    } else {
      classes.push(clazz);
    }
    final List<T> members = new ArrayList<>(12);

    while (!classes.isEmpty()) {
      clazz = classes.pop();
      for (final T m: ReflectionUtils.getDeclaredMembers(type, clazz)) {
        if ((m instanceof AnnotatedElement)
            && ((AnnotatedElement)m).getAnnotationsByType(annotation).length > 0) {
          members.add(m);
        }
      }
    }
    return members;
  }

  /**
   * Gets all fields (up the super class hierarchy) annotated with the specified
   * annotation.
   *
   * @author paouelle
   *
   * @param  clazz the class from which to find all annotated fields
   * @param  annotation the annotation for which to find all fields
   * @param  up <code>true</code> to look up the class hierarchy;
   *         <code>false</code> to only look at the specified class level
   * @return a list in the provided order for all annotated fields
   * @throws NullPointerException if <code>clazz</code> or
   *         <code>annotation</code> is <code>null</code>
   */
  public static List<Field> getAllFieldsAnnotatedWith(
    Class<?> clazz, Class<? extends Annotation> annotation, boolean up
  ) {
    return ReflectionUtils.getAllMembersAnnotatedWith(
      Field.class, clazz, annotation, up
    );
  }

  /**
   * Gets all methods (up the super class hierarchy) annotated with the specified
   * annotation.
   *
   * @author paouelle
   *
   * @param  clazz the class from which to find all annotated methods
   * @param  annotation the annotation for which to find all methods
   * @param  up <code>true</code> to look up the class hierarchy;
   *         <code>false</code> to only look at the specified class level
   * @return a list in the provided order for all annotated methods
   * @throws NullPointerException if <code>clazz</code> or
   *         <code>annotation</code> is <code>null</code>
   */
  public static List<Method> getAllMethodsAnnotatedWith(
    Class<?> clazz, Class<? extends Annotation> annotation, boolean up
  ) {
    return ReflectionUtils.getAllMembersAnnotatedWith(
      Method.class, clazz, annotation, up
    );
  }

  /**
   * Gets all members of a given type (up the super class hierarchy) annotated
   * with the specified annotation.
   *
   * @author paouelle
   *
   * @param <T> the type of members to retrieve (either {@link Field},
   *            {@link Method}, or {@link Constructor})
   * @param <A> the type of annotation to search for
   *
   * @param  type the type of members to retrieve
   * @param  clazz the class from which to find all annotated members
   * @param  annotation the annotation for which to find all members
   * @param  up <code>true</code> to look up the class hierarchy;
   *         <code>false</code> to only look at the specified class level
   * @return a non-<code>null</code> map in the provided order for all annotated
   *         members with their found annotations
   * @throws NullPointerException if <code>type</code>,
   *         <code>clazz</code> or <code>annotation</code> is <code>null</code>
   * @throws IllegalArgumentException if <code>type</code> is not
   *         {@link Field}, {@link Method}, or {@link Constructor}
   */
  public static <T extends Member, A extends Annotation> Map<T, A[]> getAllAnnotationsForMembersAnnotatedWith(
    Class<T> type,
    Class<?> clazz,
    Class<A> annotation,
    boolean up
  ) {
    org.apache.commons.lang3.Validate.notNull(type, "invalid null member type");
    org.apache.commons.lang3.Validate.notNull(clazz, "invalid null class");
    org.apache.commons.lang3.Validate.notNull(annotation, "invalid null annotation class");
    final LinkedList<Class<?>> classes = new LinkedList<>();

    if (up) {
      while (clazz != null) {
        classes.push(clazz);
        clazz = clazz.getSuperclass();
      }
    } else {
      classes.push(clazz);
    }
    final Map<T, A[]> members = new LinkedHashMap<>(12);

    while (!classes.isEmpty()) {
      clazz = classes.pop();
      for (final T m: ReflectionUtils.getDeclaredMembers(type, clazz)) {
        if (m instanceof AnnotatedElement) {
          final A[] as = ((AnnotatedElement)m).getAnnotationsByType(annotation);

          if (as.length > 0) {
            members.put(m, as);
          }
        }
      }
    }
    return members;
  }

  /**
   * Gets all fields of a given type (up the super class hierarchy) annotated
   * with the specified annotation.
   *
   * @author paouelle
   *
   * @param <A> the type of annotation to search for
   *
   * @param  clazz the class from which to find all annotated fields
   * @param  annotation the annotation for which to find all fields
   * @param  up <code>true</code> to look up the class hierarchy;
   *         <code>false</code> to only look at the specified class level
   * @return a non-<code>null</code> map in the provided order for all annotated
   *         fields with their found annotations
   * @throws NullPointerException if <code>clazz</code> or
   *         <code>annotation</code> is <code>null</code>
   */
  public static <A extends Annotation> Map<Field, A[]> getAllAnnotationsForFieldsAnnotatedWith(
    Class<?> clazz, Class<A> annotation, boolean up
  ) {
    return ReflectionUtils.getAllAnnotationsForMembersAnnotatedWith(
      Field.class, clazz, annotation, up
    );
  }

  /**
   * Gets all methods of a given type (up the super class hierarchy) annotated
   * with the specified annotation.
   *
   * @author paouelle
   *
   * @param <A> the type of annotation to search for
   *
   * @param  clazz the class from which to find all annotated methods
   * @param  annotation the annotation for which to find all methods
   * @param  up <code>true</code> to look up the class hierarchy;
   *         <code>false</code> to only look at the specified class level
   * @return a non-<code>null</code> map in the provided order for all annotated
   *         methods with their found annotations
   * @throws NullPointerException if <code>clazz</code> or
   *         <code>annotation</code> is <code>null</code>
   */
  public static <A extends Annotation> Map<Method, A[]> getAllAnnotationsForMethodsAnnotatedWith(
    Class<?> clazz, Class<A> annotation, boolean up
  ) {
    return ReflectionUtils.getAllAnnotationsForMembersAnnotatedWith(
      Method.class, clazz, annotation, up
    );
  }

  /**
   * Gets annotations that are associated with the specified element. If there
   * are no annotations associated with the element, the return value is an
   * empty map. The difference between this method and
   * {@link AnnotatedElement#getAnnotationsByType(Class)} is that this method
   * works on annotation classes that are annotated with the {@link Keyable}
   * annotation in order to determine the key of each annotation. For this to
   * work, the annotated class must defined a key element as defined in the
   * {@link Keyable} annotation of the same return type as specified in
   * <code>keyClass</code>. If its argument is a repeatable annotation type
   * (JLS 9.6), this method, attempts to find one or more annotations of that
   * type by "looking through" a container annotation. The caller of this method
   * is free to modify the returned map; it will have no effect on the maps
   * returned to other callers.
   * <p>
   * This method will look for both the specified annotation and if the
   * annotation is annotated with {@link Repeatable}, it will also look for this
   * containing annotation which must have a <code>value()</code>
   * element with an array type of the specified annotation. The resulting
   * array will always start with the annotation itself if found followed by
   * all of those provided by the containing annotation.
   *
   * @author paouelle
   *
   * @param <K> the type of the keys to find
   * @param <T> the type of the annotation to query for and return if present
   *
   * @param  keyClass the class of the keys to find and return
   * @param  annotationClass the type of annotations to retrieve
   * @param  annotatedElement the element from which to retrieve the annotations
   * @return a non-<code>null</code> ordered map of annotations associated with the
   *         given element properly keyed as defined in each annotation (may be
   *         empty if none found)
   * @throws NullPointerException if <code>annotatedElement</code>,
   *         <code>annotationClass</code> or <code>keyClass</code> is
   *         <code>null</code>
   * @throws IllegalArgumentException if <code>annotationClass</code> is not
   *         annotated with {@link Keyable} or if the containing annotation
   *         doesn't define a <code>value()</code> element returning an array
   *         of type <code>annotationClass</code> or if <code>annotationClass</code>
   *         doesn't define an element named as specified in its {@link Keyable}
   *         annotation that returns a value of the same class as <code>keyClass</code>
   *         or again if a duplicated keyed annotation is found
   */
  @SuppressWarnings("unchecked")
  public static <K, T extends Annotation> Map<K, T> getAnnotationsByType(
    Class<K> keyClass,
    Class<T> annotationClass,
    AnnotatedElement annotatedElement
  ) {
    org.apache.commons.lang3.Validate.notNull(
      annotationClass, "invalid null annotation class"
    );
    org.apache.commons.lang3.Validate.notNull(
      annotatedElement, "invalid null annotation element"
    );
    org.apache.commons.lang3.Validate.notNull(
      keyClass, "invalid null key class"
    );
    final Keyable k = annotationClass.getAnnotation(Keyable.class);

    org.apache.commons.lang3.Validate.isTrue(
      k != null,
      "annotation @%s not annotated with @Keyable",
      annotationClass.getName()
    );
    final Method km;

    try {
      km = annotationClass.getMethod(k.value());
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
        "annotation key element @"
        + annotationClass.getName()
        + "."
        + k.value()
        + "() not found",
        e
      );
    }
    org.apache.commons.lang3.Validate.isTrue(
      keyClass.isAssignableFrom(km.getReturnType()),
      "annotation key element @%s.%s() doesn't return class: %s",
      annotationClass.getName(),
      k.value(),
      keyClass.getName()
    );
    final T[] as = annotatedElement.getAnnotationsByType(annotationClass);

    if (as.length == 0) {
      return Collections.emptyMap();
    }
    final Map<K, T> map = new LinkedHashMap<>(as.length);

    for (T a: as) {
      final K ak;

      try {
        ak = (K)km.invoke(a);
      } catch (IllegalAccessException e) { // not expected
        throw new IllegalStateException(e);
      } catch (InvocationTargetException e) {
        final Throwable t = e.getTargetException();

        if (t instanceof Error) {
          throw (Error)t;
        } else if (t instanceof RuntimeException) {
          throw (RuntimeException)t;
        } else { // not expected
          throw new IllegalStateException(e);
        }
      }
      org.apache.commons.lang3.Validate.isTrue(
        map.put(ak, a) == null,
        "duplicate key '%s' found in annotation @%s",
        ak,
        annotationClass.getName()
      );
    }
    return map;
  }

  /**
   * Gets all declared members of a given type (up the super class hierarchy).
   *
   * @author paouelle
   *
   * @param <T> the type of members to retrieve (either {@link Field},
   *            {@link Method}, or {@link Constructor})
   *
   * @param  type the type of members to retrieve
   * @param  clazz the class from which to find all declared members
   * @param  up <code>true</code> to look up the class hierarchy;
   *         <code>false</code> to only look at the specified class level
   * @return a list in the provided order for all declared members
   * @throws NullPointerException if <code>type</code> or
   *         <code>clazz</code> is <code>null</code>
   * @throws IllegalArgumentException if <code>type</code> is not
   *         {@link Field}, {@link Method}, or {@link Constructor}
   */
  public static <T extends Member> List<T> getAllDeclaredMembers(
    Class<T> type, Class<?> clazz, boolean up
  ) {
    org.apache.commons.lang3.Validate.notNull(type, "invalid null member type");
    org.apache.commons.lang3.Validate.notNull(clazz, "invalid null class");
    final LinkedList<Class<?>> classes = new LinkedList<>();

    if (up) {
      while (clazz != null) {
        classes.push(clazz);
        clazz = clazz.getSuperclass();
      }
    } else {
      classes.push(clazz);
    }
    final List<T> members = new ArrayList<>(12);

    while (!classes.isEmpty()) {
      clazz = classes.pop();
      for (final T m: ReflectionUtils.getDeclaredMembers(type, clazz)) {
        members.add(m);
      }
    }
    return members;
  }

  /**
   * Gets all declared fields (up the super class hierarchy).
   *
   * @author paouelle
   *
   * @param  clazz the class from which to find all declared fields
   * @param  up <code>true</code> to look up the class hierarchy;
   *         <code>false</code> to only look at the specified class level
   * @return a list in the provided order for all declared fields
   * @throws NullPointerException if <code>clazz</code> is <code>null</code>
   */
  public static List<Field> getAllDeclaredFields(
    Class<?> clazz, boolean up
  ) {
    return ReflectionUtils.getAllDeclaredMembers(Field.class, clazz, up);
  }

  /**
   * Gets all declared methods (up the super class hierarchy).
   *
   * @author paouelle
   *
   * @param  clazz the class from which to find all declared methods
   * @param  up <code>true</code> to look up the class hierarchy;
   *         <code>false</code> to only look at the specified class level
   * @return a list in the provided order for all declared methods
   * @throws NullPointerException if <code>clazz</code> is <code>null</code>
   */
  public static List<Method> getAllDeclaredMethods(
    Class<?> clazz, boolean up
  ) {
    return ReflectionUtils.getAllDeclaredMembers(Method.class, clazz, up);
  }

  /**
   * Gets the raw class from the provided type.
   *
   * @author paouelle
   *
   * @param  t the type for which to get the raw class
   * @return the raw class for the given type
   */
  public static Class<?> getRawClass(Type t) {
    Type tt = t;

    while (!(tt instanceof Class<?>)) {
      if (tt instanceof ParameterizedType) {
        tt = ((ParameterizedType)t).getRawType();
      } else {
        throw new IllegalStateException("there should be a raw class for: " + t);
      }
    }
    return (Class<?>)tt;
  }

  /**
   * Gets the hierarchy of a given class.
   *
   * @author paouelle
   *
   * @param <T> the type of the class to get the hierarchy for
   *
   * @param  clazz the class to get the hierarchy for
   * @return a list of all classes in the given class's hierarchy starting with
   *         the specified class
   */
  public static <T> List<Class<?>> getClassHierarchy(Class<?> clazz) {
    org.apache.commons.lang3.Validate.notNull(clazz, "invalid null class");
    final LinkedList<Class<?>> classes = new LinkedList<>();

    while (clazz != null) {
      classes.push(clazz);
      clazz = clazz.getSuperclass();
    }
    return classes;
  }

  /**
   * Finds all classes defined in a given package using the current classloader.
   *
   * @author paouelle
   *
   * @param  pkg the package from which to find all defined classes
   * @return the non-<code>null</code> collection of all classes defined in the
   *         given package
   * @throws NullPointerException if <code>pkg</code> is <code>null</code>
   */
  public static Collection<Class<?>> findClasses(String pkg) {
    return ReflectionUtils.findClasses(
      pkg, Thread.currentThread().getContextClassLoader()
    );
  }

  /**
   * Finds all classes defined in a given package.
   *
   * @author paouelle
   *
   * @param  pkg the package from which to find all defined classes
   * @param  cl the classloader to find the classes with
   * @return the non-<code>null</code> collection of all classes defined in the
   *         given package
   * @throws NullPointerException if <code>pkg</code> or <code>cl</code> is
   *         <code>null</code>
   */
  public static Collection<Class<?>> findClasses(String pkg, ClassLoader cl) {
    org.apache.commons.lang3.Validate.notNull(pkg, "invalid null pkg");
    final String scannedPath = pkg.replace('.', File.separatorChar);
    final Enumeration<URL> resources;

    try {
      resources = cl.getResources(scannedPath);
    } catch (IOException e) {
      throw new IllegalArgumentException(
        "Unable to get resources from path '"
        + scannedPath
        + "'. Are you sure the given '"
        + pkg
        + "' package exists?",
        e
      );
    }
    final List<Class<?>> classes = new LinkedList<>();

    while (resources.hasMoreElements()) {
      final URL url = resources.nextElement();

      if ("jar".equals(url.getProtocol())) {
        ReflectionUtils.findClassesFromJar(classes, url, scannedPath, cl);
      } else if ("file".equals(url.getProtocol())) {
        final File file = new File(url.getFile());

        ReflectionUtils.findClassesFromFile(classes, file, pkg, cl);
      } else {
        throw new IllegalArgumentException("package is provided by an unknown url: " + url);
      }
    }
    return classes;
  }

  /**
   * Find all classes from a given jar file and add them to the provided
   * list.
   *
   * @author paouelle
   *
   * @param classes the non-<code>null</code> collection of classes where to add new
   *        found classes
   * @param url the non-<code>null</code> url for the jar where to find classes
   * @param resource the non-<code>null</code> resource being scanned that
   *        corresponds to given package
   * @param cl the classloader to find the classes with
   */
  private static void findClassesFromJar(
    Collection<Class<?>> classes, URL url, String resource, ClassLoader cl
  ) {
    try {
      final JarURLConnection conn = (JarURLConnection)url.openConnection();
      final URL jurl = conn.getJarFileURL();

      try (
        final JarInputStream jar = new JarInputStream(jurl.openStream());
      ) {
        while (true) {
          final JarEntry entry = jar.getNextJarEntry();

          if (entry == null) {
            break;
          }
          final String name = entry.getName();

          if (name.endsWith(".class") && name.startsWith(resource)) {
            final String cname = name.substring(
              0, name.length() - 6 // 6 for .class
            ).replace(File.separatorChar, '.');

            try {
              classes.add(cl.loadClass(cname));
            } catch (ClassNotFoundException e) { // ignore it
            }
          }
        }
      }
    } catch (IOException e) {
      throw new IllegalArgumentException("unable to find classes in package", e);
    }
  }

  /**
   * Find all classes from a given file or directory and add them to the provided
   * list.
   *
   * @author paouelle
   *
   * @param classes the non-<code>null</code> collection of classes where to add new
   *        found classes
   * @param file the non-<code>null</code> file or directory from which to find
   *        classes
   * @param resource the non-<code>null</code> resource being scanned that
   *        corresponds to given file or directory
   * @param cl the classloader to find the classes with
   */
  private static void findClassesFromFile(
    Collection<Class<?>> classes, File file, String resource, ClassLoader cl
  ) {
    if (file.isDirectory()) {
      for (File f: file.listFiles()) {
        ReflectionUtils.findClassesFromFile(
          classes, f, resource + "." + f.getName(), cl
        );
      }
    } else if (resource.endsWith(".class")) {
      final String cname = resource.substring(0, resource.length() - 6); // 6 for .class

      try {
        classes.add(cl.loadClass(cname));
      } catch (ClassNotFoundException e) { // ignore it
      }
    }
  }

  /**
   * Finds all resources defined in a given package using the current classloader.
   *
   * @author paouelle
   *
   * @param  pkg the package from which to find all defined resources
   * @return the non-<code>null</code> collection of all resources defined in the
   *         given package
   * @throws NullPointerException if <code>pkg</code> is <code>null</code>
   */
  public static Collection<URL> findResources(String pkg) {
    return ReflectionUtils.findResources(
      pkg, Thread.currentThread().getContextClassLoader()
    );
  }

  /**
   * Finds all resources defined in a given package.
   *
   * @author paouelle
   *
   * @param  pkg the package from which to find all defined resources
   * @param  cl the classloader to find the resources with
   * @return the non-<code>null</code> collection of all resources defined in the
   *         given package
   * @throws NullPointerException if <code>pkg</code> or <code>cl</code> is
   *         <code>null</code>
   */
  public static Collection<URL> findResources(String pkg, ClassLoader cl) {
    org.apache.commons.lang3.Validate.notNull(pkg, "invalid null pkg");
    final String scannedPath = pkg.replace('.', File.separatorChar);
    final Enumeration<URL> resources;

    try {
      resources = cl.getResources(scannedPath);
    } catch (IOException e) {
      throw new IllegalArgumentException(
        "Unable to get resources from path '"
        + scannedPath
        + "'. Are you sure the given '"
        + pkg
        + "' package exists?",
        e
      );
    }
    final List<URL> urls = new LinkedList<>();

    while (resources.hasMoreElements()) {
      final URL url = resources.nextElement();

      if ("jar".equals(url.getProtocol())) {
        ReflectionUtils.findResourcesFromJar(urls, url, scannedPath, cl);
      } else if ("file".equals(url.getProtocol())) {
        final File file = new File(url.getFile());

        ReflectionUtils.findResourcesFromFile(urls, file, scannedPath, cl);
      } else {
        throw new IllegalArgumentException("package is provided by an unknown url: " + url);
      }
    }
    return urls;
  }

  /**
   * Find all resources from a given jar file and add them to the provided
   * list.
   *
   * @author paouelle
   *
   * @param urls the non-<code>null</code> collection of resources where to add new
   *        found resources
   * @param url the non-<code>null</code> url for the jar where to find resources
   * @param resource the non-<code>null</code> resource being scanned that
   *        corresponds to given package
   * @param cl the classloader to find the resources with
   */
  private static void findResourcesFromJar(
    Collection<URL> urls, URL url, String resource, ClassLoader cl
  ) {
    try {
      final JarURLConnection conn = (JarURLConnection)url.openConnection();
      final URL jurl = conn.getJarFileURL();

      try (
        final JarInputStream jar = new JarInputStream(jurl.openStream());
      ) {
        while (true) {
          final JarEntry entry = jar.getNextJarEntry();

          if (entry == null) {
            break;
          }
          final String name = entry.getName();

          if (name.startsWith(resource)) {
            final URL u = cl.getResource(name);

            if (u != null) {
              urls.add(u);
            }
          }
        }
      }
    } catch (IOException e) {
      throw new IllegalArgumentException("unable to find resources in package", e);
    }
  }

  /**
   * Find all resources from a given file or directory and add them to the provided
   * list.
   *
   * @author paouelle
   *
   * @param urls the non-<code>null</code> collection of resources where to add new
   *        found resources
   * @param file the non-<code>null</code> file or directory from which to find
   *        resources
   * @param resource the non-<code>null</code> resource being scanned that
   *        corresponds to given file or directory
   * @param cl the classloader to find the resources with
   */
  private static void findResourcesFromFile(
    Collection<URL> urls, File file, String resource, ClassLoader cl
  ) {
    if (file.isDirectory()) {
      for (File f: file.listFiles()) {
        ReflectionUtils.findResourcesFromFile(
          urls, f, resource + File.separatorChar + f.getName(), cl
        );
      }
    } else {
      final URL u = cl.getResource(resource);

      if (u != null) {
        urls.add(u);
      }
    }
  }

  /**
   * Checks if the specified class is assignment compatible (i.e. is a subclass of)
   * any of the classes provided.
   *
   * @author paouelle
   *
   * @param  classes the set of classes to check if <code>clazz</code> is
   *         assignment compatible with
   * @param  clazz the class to be checking
   * @return <code>true</code> if <code>clazz</code> is a subclass of at least
   *         one of the specified classes
   */
  public final static boolean isAssignableFrom(Class<?>[] classes, Class<?> clazz) {
    for (final Class<?> c: classes) {
      if (c.isAssignableFrom(clazz)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Gets a class object corresponding to a given declared type.
   *
   * @author paouelle
   *
   * @param  type the declared type for which to get a class object
   * @return the corresponding non-<code>null</code> class object
   * @throws ClassNotFoundException if the class is not found
   */
  public static Class<?> classFor(DeclaredType type)
    throws ClassNotFoundException {
    // recurse the class hierarchy to build it manually
    final StringBuilder sb = new StringBuilder(40);
    final Deque<Element> es = new LinkedList<>();

    for (Element e = type.asElement(); e != null; e = e.getEnclosingElement()) {
      es.push(e);
    }
    Element previous = null;

    for (final Element e: es) {
      if (previous != null) {
        if (previous.getKind().isClass() || previous.getKind().isInterface()) {
          sb.append('$');
        } else { // should be a package
          sb.append('.');
        }
      }
      if (e instanceof PackageElement) { // easy as well!
        sb.append(((PackageElement)e).getQualifiedName());
      } else if (e.getKind() == ElementKind.PACKAGE) { // oh well! rely on toString() :-(
        sb.append(e);
      } else { // anything else, deal with its simple name only
        sb.append(e.getSimpleName());
      }
      previous = e;
    }
    return Class.forName(sb.toString());
  }

  /**
   * Gets a primitive class for the specified primitive type kind.
   *
   * @author paouelle
   *
   * @param  kind the primitive type kind for which to get a class
   * @return the corresponding non-<code>null</code> class
   * @throws ClassNotFoundException if an invalid primitive kind is specified
   */
  private static Class<?> primitiveClassFor(TypeKind kind)
    throws ClassNotFoundException {
    switch (kind) {
      case BYTE:
        return Byte.TYPE;
      case CHAR:
        return Character.TYPE;
      case SHORT:
        return Short.TYPE;
      case INT:
        return Integer.TYPE;
      case LONG:
        return Long.TYPE;
      case FLOAT:
        return Float.TYPE;
      case DOUBLE:
        return Double.TYPE;
      case BOOLEAN:
        return Boolean.TYPE;
      default:
        throw new ClassNotFoundException("unknown primitive kind: " + kind);
    }
  }

  /**
   * Gets a primitive array class for the specified primitive component type kind.
   *
   * @author paouelle
   *
   * @param  kind the primitive component type kind for which to get a class
   * @return the corresponding non-<code>null</code> class
   * @throws ClassNotFoundException if an invalid primitive component kind is specified
   */
  private static Class<?> primitiveArrayClassFor(TypeKind kind)
    throws ClassNotFoundException {
    switch (kind) {
      case BYTE:
        return byte[].class;
      case CHAR:
        return char[].class;
      case SHORT:
        return short[].class;
      case INT:
        return int[].class;
      case LONG:
        return long[].class;
      case FLOAT:
        return float[].class;
      case DOUBLE:
        return double[].class;
      case BOOLEAN:
        return boolean[].class;
      default:
        throw new ClassNotFoundException("unknown primitive array element kind: " + kind);
    }
  }

  /**
   * Gets a class object corresponding to a given type mirror.
   *
   * @author paouelle
   *
   * @param  tm the type mirror for which to get a class object
   * @return the corresponding non-<code>null</code> class object
   * @throws ClassNotFoundException if the class is not found
   */
  public static Class<?> classFor(TypeMirror tm)
    throws ClassNotFoundException {
    if (tm instanceof ArrayType) {
      final ArrayType array = (ArrayType)tm;
      final TypeMirror compType = array.getComponentType();

      if (compType.getKind().isPrimitive()) {
        return ReflectionUtils.primitiveArrayClassFor(compType.getKind());
      } else if (compType instanceof DeclaredType) {
        return Array.newInstance(
          ReflectionUtils.classFor((DeclaredType)compType), 0
        ).getClass();
      } // else - oh! well, rely on toString()
      return Array.newInstance(Class.forName(compType.toString()), 0).getClass();
    } else if (tm.getKind().isPrimitive()) {
      return ReflectionUtils.primitiveClassFor(tm.getKind());
    } else if (tm.getKind() == TypeKind.VOID) {
      return Void.TYPE;
    } else if (tm instanceof DeclaredType) {
      return ReflectionUtils.classFor((DeclaredType)tm);
    } // else - oh! well, rely on toString()
    return Class.forName(tm.toString());
  }

  /**
   * Gets annotations of a specific type that are <em>associated</em> with the
   * element.
   * <p>
   * If there are no annotations <em>associated</em> with this element, the return
   * value is an empty stream.
   *
   * @param <T> the type of the annotation to query for and return if present
   *
   * @param  annotatedElement the element to retrieve the annotations from
   * @param  annotationClass the type of annotations to retrieve
   * @return a stream of the element's annotations for the specified annotation
   *         type if present on this element, else an empty stream
   * @throws NullPointerException if the given element or annotation class are
   *         <code>null</code>
   *
   * @see <a href="https://blog.retep.org/2009/02/13/getting-class-values-from-annotations-in-an-annotationprocessor/">https://blog.retep.org/2009/02/13/getting-class-values-from-annotations-in-an-annotationprocessor/</a>
   */
  @SuppressWarnings("unchecked")
  public static <T extends Annotation> Stream<T> annotationsByType(
    Element annotatedElement, Class<T> annotationClass
  ) {
    final String aname = annotationClass.getName();

    return annotatedElement.getAnnotationMirrors().stream()
      .filter(am -> aname.equals(am.getAnnotationType().toString()))
      .map(am -> (T)java.lang.reflect.Proxy.newProxyInstance(
        annotationClass.getClassLoader(),
        new Class<?>[] { annotationClass },
        new AnnotationProxy(am)
      ));
  }

  /**
   * Gets annotations which are themselves annotated with a given type of
   * annotation that are <em>present</em> on the element.
   * <p>
   * If there are no annotations <em>present</em> on the element, the return
   * value is an empty stream.
   * <p>
   * For example, one could find all annotations annotated with the &#64;Tag
   * annotation, which in the following examples would return &#64;ThisOne and
   * &#64;ThisOneToo annotations but not &#64;NotThisOne:
   * <p><pre>
   *   &#64;Tag
   *   public &#64;interface ThisOne {
   *      String value();
   *   }
   *
   *   &#64;Tag
   *   public &#64;interface ThisOneToo {
   *      String value();
   *   }
   *
   *   public &#64;interface NotThisOne {
   *      String value();
   *   }
   * </pre>
   *
   * @param  annotatedElement the element to retrieve the annotations from
   * @param  annotationClass the type of annotations to retrieve annotations
   *         annotated with
   * @return a stream of the element's annotations annotated with the specified
   *         annotation type if present on this element, else an empty stream
   * @throws NullPointerException if the given element or annotation class are
   *         <code>null</code>
   *
   * @see <a href="https://blog.retep.org/2009/02/13/getting-class-values-from-annotations-in-an-annotationprocessor/">https://blog.retep.org/2009/02/13/getting-class-values-from-annotations-in-an-annotationprocessor/</a>
   */
  public static Stream<Annotation> annotationsAnnotatedWith(
    Element annotatedElement, Class<? extends Annotation> annotationClass
  ) {
    return annotatedElement.getAnnotationMirrors().stream()
      .map(am -> {
        try {
          final Class<?> aclass = ReflectionUtils.classFor(am.getAnnotationType());

          if (!aclass.isAnnotationPresent(annotationClass)) {
            return null;
          }
          return (Annotation)java.lang.reflect.Proxy.newProxyInstance(
            aclass.getClassLoader(),
            new Class<?>[] { aclass },
            new AnnotationProxy(am)
          );
        } catch (ClassNotFoundException e) {
          // ignore it as it will most likely be an annotation defined within
          // the current compilation unit which means we do not have a class for them anyway
          return null;
        }
      })
      .filter(a -> a != null);
  }

  /**
   * Gets annotations that are <em>present</em> on the element.
   * <p>
   * If there are no annotations <em>present</em> on the element, the return
   * value is an empty stream.
   *
   * @param  annotatedElement the element to retrieve the annotations from
   * @return a stream of annotations present on the element
   * @throws NullPointerException if the given element is <code>null</code>
   *
   * @see <a href="https://blog.retep.org/2009/02/13/getting-class-values-from-annotations-in-an-annotationprocessor/">https://blog.retep.org/2009/02/13/getting-class-values-from-annotations-in-an-annotationprocessor/</a>
   */
  public static Stream<Annotation> annotations(Element annotatedElement) {
    return annotatedElement.getAnnotationMirrors().stream()
      .map(am -> {
        try {
          final Class<?> aclass = ReflectionUtils.classFor(am.getAnnotationType());

          return (Annotation)java.lang.reflect.Proxy.newProxyInstance(
            aclass.getClassLoader(),
            new Class<?>[] { aclass },
            new AnnotationProxy(am)
          );
        } catch (ClassNotFoundException e) {
          // ignore it as it will most likely be an annotation defined within
          // the current compilation unit which means we do not have a class for them anyway
          return null;
        }
      })
      .filter(a -> a != null);
  }

  /**
   * Gets annotations which are themselves annotated with a given type of
   * annotation that are <em>present</em> on the element.
   * <p>
   * If there are no annotations <em>present</em> on the element, the return
   * value is an array of length 0.
   * <p>
   * For example, one could find all annotations annotated with the &#64;Tag
   * annotation, which in the following examples would return &#64;ThisOne and
   * &#64;ThisOneToo annotations but not &#64;NotThisOne:
   * <p><pre>
   *   &#64;Tag
   *   public &#64;interface ThisOne {
   *      String value();
   *   }
   *
   *   &#64;Tag
   *   public &#64;interface ThisOneToo {
   *      String value();
   *   }
   *
   *   public &#64;interface NotThisOne {
   *      String value();
   *   }
   * </pre>
   *
   * @param  annotatedElement the element to retrieve the annotations from
   * @param  annotationClass the type of annotations to retrieve annotations
   *         annotated with
   * @return an array of the element's annotations annotated with the specified
   *         annotation type if present on this element, else an array of length
   *         0
   * @throws NullPointerException if the given element or annotation class are
   *         <code>null</code>
   *
   * @see <a href="https://blog.retep.org/2009/02/13/getting-class-values-from-annotations-in-an-annotationprocessor/">https://blog.retep.org/2009/02/13/getting-class-values-from-annotations-in-an-annotationprocessor/</a>
   */
  public static Annotation[] getAnnotationsAnnotatedWith(
    Element annotatedElement, Class<? extends Annotation> annotationClass
  ) {
    return ReflectionUtils.annotationsAnnotatedWith(annotatedElement, annotationClass)
      .toArray(Annotation[]::new);
  }

  /**
   * Gets annotations that are <em>present</em> on the element.
   * <p>
   * If there are no annotations <em>present</em> on the element, the return
   * value is an array of length 0.
   * <p>
   * The caller of this method is free to modify the returned array; it will
   * have no effect on the arrays returned to other callers.
   *
   * @param  annotatedElement the element to retrieve the annotations from
   * @return an array of annotations present on the element, else an array of
   *         length 0
   * @throws NullPointerException if the given element is <code>null</code>
   *
   * @see <a href="https://blog.retep.org/2009/02/13/getting-class-values-from-annotations-in-an-annotationprocessor/">https://blog.retep.org/2009/02/13/getting-class-values-from-annotations-in-an-annotationprocessor/</a>
   */
  public static Annotation[] getAnnotations(Element annotatedElement) {
    return ReflectionUtils.annotations(annotatedElement)
        .toArray(Annotation[]::new);
  }

  /**
   * Gets annotations of a specific that are <em>associated</em> with the element.
   * <p>
   * If there are no annotations <em>associated</em> with this element, the return
   * value is an array of length 0.
   * <p>
   * The difference between this method and
   * {@link ReflectionUtils#getAnnotation(Element, Class)} is that this method
   * detects if its argument is a <em>repeatable annotation type</em> (JLS 9.6),
   * and if so, attempts to find one or more annotations of that type by
   * "looking through" a container annotation.
   * <p>
   * The caller of this method is free to modify the returned array; it will
   * have no effect on the arrays returned to other callers.
   * <p>
   *
   * @param <T> the type of the annotation to query for and return if present
   *
   * @param  annotatedElement the element to retrieve the annotations from
   * @param  annotationClass the type of annotations to retrieve
   * @return an array of the element's annotations for the specified annotation
   *         type if present on this element, else an empty array
   * @throws NullPointerException if the given element or annotation class are
   *         <code>null</code>
   *
   * @see <a href="https://blog.retep.org/2009/02/13/getting-class-values-from-annotations-in-an-annotationprocessor/">https://blog.retep.org/2009/02/13/getting-class-values-from-annotations-in-an-annotationprocessor/</a>
   */
  @SuppressWarnings("unchecked")
  public static <T extends Annotation> T[] getAnnotationsByType(
    Element annotatedElement, Class<T> annotationClass
  ) {
    return ReflectionUtils.annotationsByType(annotatedElement, annotationClass)
      .toArray(size -> (T[])Array.newInstance(annotationClass, size));
  }

  /**
   * Gets the specified element's annotation for the specified type if such an
   * annotation is <em>present</em>, else <code>null</code> where classes
   * referenced from the annotation are properly resolved and will not generate
   * a compile-time mirror exception.
   * <p>
   * This method is useful when dealing with annotations in a processing
   * environment where Javac does not load classes in the normal manner. In fact
   * it doesn’t at all for classes that are in the source – it’s all contained
   * within a model.
   *
   * @author paouelle
   *
   * @param <T> the type of the annotation to query for and return if present
   *
   * @param  annotatedElement the element to retrieve the annotation from
   * @param  annotationClass the type of annotation to retrieve
   * @return the element's annotation for the specified annotation type if
   *         present on this element, else <code>null</code>
   * @throws NullPointerException if the given element or annotation class are
   *         <code>null</code>
   *
   * @see <a href="https://blog.retep.org/2009/02/13/getting-class-values-from-annotations-in-an-annotationprocessor/">https://blog.retep.org/2009/02/13/getting-class-values-from-annotations-in-an-annotationprocessor/</a>
   */
  public static <T extends Annotation> T getAnnotation(
    Element annotatedElement, Class<T> annotationClass
  ) {
    return ReflectionUtils.annotationsByType(annotatedElement, annotationClass)
      .findFirst()
      .orElse(null);
  }

  /**
   * Prevents instantiation of a new <code>class</code> object.
   *
   * @author paouelle
   *
   * @throws IllegalStateException always thrown
   */
  private ReflectionUtils() {
    throw new IllegalStateException("invalid constructor called");
  }
}

/**
 * The <code>AnnotationProxy</code> class provides a definition for a dynamic
 * proxy used when dynamically creating annotation instances to go around the
 * annotation processor issues with annotations that contains classes as returned
 * types for its elements.
 *
 * @copyright 2015-2016 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jul 7, 2016 - paouelle - Creation
 *
 * @since 1.0
 */
class AnnotationProxy implements InvocationHandler {
  /**
   * Holds the annotation mirror this object is a proxy for.
   *
   * @author paouelle
   */
  private final AnnotationMirror am;

  /**
   * Instantiates a new <code>AnnotationProxy</code> object.
   *
   * @author paouelle
   *
   * @param am the annotation mirror for which we are creating a proxy
   */
  AnnotationProxy(AnnotationMirror am) {
    this.am = am;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see java.lang.reflect.InvocationHandler#invoke(java.lang.Object, java.lang.reflect.Method, java.lang.Object[])
   */
  @Override
  public java.lang.Object invoke(
    java.lang.Object proxy,
    Method method,
    java.lang.Object[] args
  ) throws Throwable {
    final Class<?> rclass = method.getReturnType();
    final String name = method.getName();

    //System.err.print(">>> M: " + method.getDeclaringClass().getSimpleName() + "." + name);
    for (final Map.Entry<? extends ExecutableElement, ? extends AnnotationValue> e: am.getElementValues().entrySet()) {
      if (name.equals(e.getKey().getSimpleName().toString())) {
        final AnnotationValue eav = e.getValue();

        if (Class.class.isAssignableFrom(rclass)) {
          //System.err.println(" -> class, eav: " + eav.getValue().getClass() + ", eav str: " + eav.getValue().toString());
          if (eav.getValue() instanceof DeclaredType) {
            return ReflectionUtils.classFor((DeclaredType)eav.getValue());
          } // oh well! really on toString and cross your fingers :-(
          return Class.forName(eav.getValue().toString());
        } else if (rclass.isArray() && (eav.getValue() instanceof List)) {
          // why couldn't the damn compiler just return arrays!!!!!
          final List<AnnotationValue> list = List.class.cast(eav.getValue());
          final Object array = Array.newInstance(rclass.getComponentType(), list.size());
          int i = 0;

          for (final Object o: list) {
            final Object av = ((AnnotationValue)o).getValue();

            if (Class.class.isAssignableFrom(rclass.getComponentType())) {
              // special case since classes are not returned here!!!!
              if (av instanceof DeclaredType) {
                Array.set(array, i++, ReflectionUtils.classFor((DeclaredType)av));
              } else { // oh well! really on toString and cross your fingers :-(
                Array.set(array, i++, Class.forName(av.toString()));
              }
            } else {
              Array.set(array, i++, av);
            }
          }
          return array;
        }
        //System.err.println(" -> ?, eav: " + eav.getValue().getClass() + ", eav str: " + eav.getValue());
        return eav.getValue();
      }
    }
    // fallback to default defined in annotation class
    //System.err.println(" -> default: " + method.getDefaultValue());
    return method.getDefaultValue();
  }
}


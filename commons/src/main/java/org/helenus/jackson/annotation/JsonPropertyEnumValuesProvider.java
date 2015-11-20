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
package org.helenus.jackson.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import java.util.Collection;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.stream.Stream;

import com.fasterxml.jackson.annotation.JacksonAnnotation;

/**
 * The <code>JsonPropertyEnumValuesProvider</code> annotation is used to annotated
 * a static method in a class which is able to provide the set of valid enum values
 * for a Json property annotated with {@link JsonPropertyEnumValues} and defining
 * a class using the {@link JsonPropertyEnumValues#valueAvailablesOf} or
 * {@link JsonPropertyEnumValues#keyAvailablesOf}.
 * <p>
 * The referenced method must be public and static. It can return an array, a
 * {@link Collection}, an {@link Iterable}, an {@link Iterator}, an
 * {@link Enumeration}, or a {@link Stream} of objects or a single object.
 * The string representation of each returned object will be used as a possible
 * enum value for the Json property.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Nov 20, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@JacksonAnnotation
public @interface JsonPropertyEnumValuesProvider {}

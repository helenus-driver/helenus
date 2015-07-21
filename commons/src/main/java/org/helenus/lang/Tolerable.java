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
package org.helenus.lang;

/**
 * The <code>Tolerable</code> interface can be implemented by classes that
 * supports testing for equality with a tolerable error when comparing floating
 * point values.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jul 20, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
public interface Tolerable {
  /**
   * Indicates whether some other object is "equal to" this one with a
   * tolerable error.
   *
   * @author paouelle
   *
   * @param  obj the reference object with which to compare
   * @param  epsilon the tolerable error
   * @return <code>true</code> if this object is the same as a <code>obj</code>
   *         within the specified tolerable error
   */
  public boolean equals(Object obj, double epsilon);
}

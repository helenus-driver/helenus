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
package com.github.helenusdriver.driver;

/**
 * The <code>Recorder</code> interface can be used along with a {@link Batch}
 * or a {@link Sequence} in order to record inserted, updated, or removed POJO
 * objects as statements are added to a batch or a sequence using one of the
 * following methods:
 * <p>
 * <ul>
 *   <li>{@link Batch#add(BatchableStatement)}</li>
 *   <li>{@link Sequence#add(BatchableStatement)}</li>
 *   <li>{@link Sequence#add(SequenceableStatement)}</li>
 * </ul>
 * <p>
 * It can also be used to validate the statement before it is allowed to be
 * added to the batch.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - May 20, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
public interface Recorder {
  /**
   * Called when an object statement is being recorded with a
   * {@link Group}.
   * <p>
   * <i>Note:</i> All runtime exceptions thrown out of this method will bubble
   * out of any of the <code>add()</code> methods listed above.
   *
   * @author paouelle
   *
   * @param  statement the non-<code>null</code> object statement recorded
   * @throws ObjectValidationException if the statement's POJO is validated and
   *         that validation fails
   */
  public void recorded(ObjectStatement<?> statement);
}

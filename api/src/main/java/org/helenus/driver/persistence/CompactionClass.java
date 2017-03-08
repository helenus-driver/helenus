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
package org.helenus.driver.persistence;

/**
 * The <code>CompactionClass</code> enumeration provides option that determines
 * how Cassandra compacts tables.
 *
 * @copyright 2015-2017 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Mar 7, 2017 - paouelle - Creation
 *
 * @since 1.0
 */
@SuppressWarnings("javadoc")
public enum CompactionClass {
  SIZED_TIERED("SizeTieredCompactionStrategy"),
  LEVELED("LeveledCompactionStrategy"),
  DATE_TIERED("DateTieredCompactionStrategy");

  /**
   * Holds the compaction class name.
   *
   * @author paouelle
   */
  public final String NAME;

  /**
   * Instantiates a new <code>CompactionClass</code> object.
   *
   * @author paouelle
   *
   * @param name the compaction class name
   */
  CompactionClass(String name) {
    this.NAME = name;
  }
}

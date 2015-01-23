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
package com.github.helenusdriver.examples.music;

import com.github.helenusdriver.persistence.Entity;
import com.github.helenusdriver.persistence.Keyspace;
import com.github.helenusdriver.persistence.Table;

/**
 * The <code>Song</code> class provides the definition for an actual song
 * provided by a social music service.
 * <p>
 * <ul>
 *   <li>Keyspace: "global"</li>
 *   <li>Table: "songs"</li>
 *   <li>Partition Key: [id]</li>
 *   <li>Data:
 *     <ul>
 *       <li>[title]</li>
 *       <li>[album]</li>
 *       <li>[artist]</li>
 *       <li>[data]</li>
 *     </ul>
 *   </li>
 * </ul>
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 15, 2015 - paouelle - Creation
 *
 * @since 1.0
 *
 * @see <a href="http://www.datastax.com/documentation/cql/3.1/cql/ddl/ddl_music_service_c.html">Datastax Music Service Example</a>
 */
@lombok.NoArgsConstructor
@lombok.Getter
@lombok.Setter
@lombok.experimental.Accessors(chain=true)
@lombok.ToString
@lombok.EqualsAndHashCode(of={"projectId", "recordType", "id"})
@Keyspace(suffixes=Song.SUFFIX)
@Table(name=Song.TABLE)
@Entity
public class Song {
  public static final String SUFFIX = "service";
  public static final String TABLE = "songs";
  public static final String ID = "id";
  public static final String TITLE = "title";
  public static final String ALBUM = "album";
  public static final String ARTIST = "artist";
  public static final String DATA = "data";
}

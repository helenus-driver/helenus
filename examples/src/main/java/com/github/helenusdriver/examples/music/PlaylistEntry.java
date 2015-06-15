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

import java.util.UUID;
import java.util.stream.Stream;

import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.datastax.driver.core.exceptions.QueryValidationException;
import com.github.helenusdriver.driver.ObjectConversionException;
import com.github.helenusdriver.driver.StatementBuilder;
import com.github.helenusdriver.persistence.ClusteringKey;
import com.github.helenusdriver.persistence.Column;
import com.github.helenusdriver.persistence.Entity;
import com.github.helenusdriver.persistence.Index;
import com.github.helenusdriver.persistence.Keyspace;
import com.github.helenusdriver.persistence.Mandatory;
import com.github.helenusdriver.persistence.PartitionKey;
import com.github.helenusdriver.persistence.Table;

/**
 * The <code>PlaylistEntry</code> class provides the definition for an entry in
 * a playlist provided by a social music service.
 * <p>
 * <ul>
 *   <li>Keyspace: "music"</li>
 *   <li>Table: "playlists"</li>
 *   <li>Partition Key: [id]</li>
 *   <li>Clustering Key: [song-order]↑</li>
 *   <li>Data:
 *     <ul>
 *       <li>[song-id]</li>
 *       <li>[title]</li>
 *       <li>[album]</li>
 *       <li>[artist]</li>
 *     </ul>
 *   </li>
 * </ul>
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 23, 2015 - paouelle - Creation
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
@lombok.EqualsAndHashCode(of={"id", "songOrder"})
@Keyspace(suffixes=Constants.MUSIC)
@Table(name=Constants.PLAYLISTS)
@Entity
public class PlaylistEntry {
  /**
   * Loads a specific playlist entry from the database.
   *
   * @author paouelle
   *
   * @param  id the playlist identifier
   * @param  songOrder the song/entry number in that playlist
   * @return the playlist entry loaded from the db
   * @throws NullPointerException if <code>id</code> is <code>null</code>
   * @throws NoHostAvailableException if no host in the cluster can be contacted
   *         successfully to execute this statement.
   * @throws QueryExecutionException if the statement triggered an execution
   *         exception, i.e. an exception thrown by Cassandra when it cannot
   *         execute the statement with the requested consistency level
   *         successfully.
   * @throws QueryValidationException if the statement is invalid (syntax error,
   *         unauthorized or any other validation problem).
   * @throws ObjectConversionException if unable to convert to a POJO
   */
  public static PlaylistEntry load(UUID id, int songOrder) {
    org.apache.commons.lang3.Validate.notNull(id, "invalid null id");
    return StatementBuilder.select(PlaylistEntry.class)
      .all()
      .from(Constants.PLAYLISTS)
      .where(StatementBuilder.eq(Constants.ID, id))
      .and(StatementBuilder.eq(Constants.SONG_ORDER, songOrder))
      .limit(1)
      .execute()
      .one();
  }

  /**
   * Loads a specific playlist from the database.
   *
   * @author paouelle
   *
   * @param  id the playlist identifier
   * @return a stream of all playlist entries for the given id loaded from the db
   * @throws NullPointerException if <code>id</code> is <code>null</code>
   * @throws NoHostAvailableException if no host in the cluster can be contacted
   *         successfully to execute this statement.
   * @throws QueryExecutionException if the statement triggered an execution
   *         exception, i.e. an exception thrown by Cassandra when it cannot
   *         execute the statement with the requested consistency level
   *         successfully.
   * @throws QueryValidationException if the statement is invalid (syntax error,
   *         unauthorized or any other validation problem).
   * @throws ObjectConversionException if unable to convert to a POJO
   */
  public static Stream<PlaylistEntry> load(UUID id) {
    org.apache.commons.lang3.Validate.notNull(id, "invalid null id");
    return StatementBuilder.select(PlaylistEntry.class)
      .all()
      .from(Constants.PLAYLISTS)
      .where(StatementBuilder.eq(Constants.ID, id))
      .execute()
      .stream();
  }

  /**
   * Loads all playlist entries from the database.
   *
   * @author paouelle
   *
   * @return a stream of all playlist entries loaded from the db
   * @throws NoHostAvailableException if no host in the cluster can be contacted
   *         successfully to execute this statement.
   * @throws QueryExecutionException if the statement triggered an execution
   *         exception, i.e. an exception thrown by Cassandra when it cannot
   *         execute the statement with the requested consistency level
   *         successfully.
   * @throws QueryValidationException if the statement is invalid (syntax error,
   *         unauthorized or any other validation problem).
   * @throws ObjectConversionException if unable to convert a POJO
   */
  public static Stream<PlaylistEntry> all() {
    return StatementBuilder.select(PlaylistEntry.class)
      .all()
      .from(Constants.PLAYLISTS)
      .execute()
      .stream();
  }

  /**
   * Holds the unique playlist id.
   *
   * @author paouelle
   */
  @lombok.NonNull
  @Column(name=Constants.ID)
  @PartitionKey
  @Mandatory
  private UUID id;

  /**
   * Holds the song order in the playlist for this entry.
   *
   * @author paouelle
   */
  @lombok.NonNull
  @Column(name=Constants.SONG_ORDER)
  @ClusteringKey
  @Mandatory
  private Integer songOrder;

  /**
   * Holds the song's id.
   *
   * @author paouelle
   */
  @lombok.NonNull
  @Column(name=Constants.SONG_ID)
  @Mandatory
  private UUID songId;

  /**
   * Holds the song's title.
   *
   * @author paouelle
   */
  @lombok.NonNull
  @Column(name=Constants.TITLE)
  @Mandatory
  private String title;

  /**
   * Holds the song's album.
   *
   * @author paouelle
   */
  @lombok.NonNull
  @Column(name=Constants.ALBUM)
  @Mandatory
  private String album;

  /**
   * Holds the song's artist.
   *
   * @author paouelle
   */
  @lombok.NonNull
  @Column(name=Constants.ARTIST)
  @Index
  @Mandatory
  private String artist;

  /**
   * Saves this playlist entry directly in the database.
   *
   * @author paouelle
   *
   * @throws NoHostAvailableException if no host in the cluster can be
   *         contacted successfully to execute this statement.
   * @throws QueryExecutionException if the statement triggered an execution
   *         exception, i.e. an exception thrown by Cassandra when it cannot execute
   *         the statement with the requested consistency level successfully.
   * @throws QueryValidationException if the statement is invalid (syntax error,
   *         unauthorized or any other validation problem).
   */
  public void save() {
    StatementBuilder.insert(this).intoAll().execute();
  }

  /**
   * Updates this playlist entry directly in the database.
   *
   * @author paouelle
   *
   * @throws NoHostAvailableException if no host in the cluster can be
   *         contacted successfully to execute this statement.
   * @throws QueryExecutionException if the statement triggered an execution
   *         exception, i.e. an exception thrown by Cassandra when it cannot execute
   *         the statement with the requested consistency level successfully.
   * @throws QueryValidationException if the statement is invalid (syntax error,
   *         unauthorized or any other validation problem).
   */
  public void update() {
    StatementBuilder.update(this).execute();
  }
}

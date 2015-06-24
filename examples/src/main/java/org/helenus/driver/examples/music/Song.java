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
package org.helenus.driver.examples.music;

import java.util.UUID;
import java.util.stream.Stream;

import java.nio.ByteBuffer;

import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.datastax.driver.core.exceptions.QueryValidationException;

import org.helenus.driver.ObjectConversionException;
import org.helenus.driver.StatementBuilder;
import org.helenus.driver.persistence.Column;
import org.helenus.driver.persistence.Entity;
import org.helenus.driver.persistence.Keyspace;
import org.helenus.driver.persistence.Mandatory;
import org.helenus.driver.persistence.PartitionKey;
import org.helenus.driver.persistence.Table;

/**
 * The <code>Song</code> class provides the definition for an actual song
 * provided by a social music service.
 * <p>
 * <ul>
 *   <li>Keyspace: "music"</li>
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
@lombok.ToString(exclude="data")
@lombok.EqualsAndHashCode(of="id")
@Keyspace(suffixes=Constants.MUSIC)
@Table(name=Constants.SONGS)
@Entity
public class Song {
  /**
   * Loads a specific song from the database.
   *
   * @author paouelle
   *
   * @param  id the song identifier
   * @return the song loaded from the db
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
  public static Song load(UUID id) {
    org.apache.commons.lang3.Validate.notNull(id, "invalid null id");
    return StatementBuilder.select(Song.class)
      .all()
      .from(Constants.SONGS)
      .where(StatementBuilder.eq(Constants.ID, id))
      .limit(1)
      .execute()
      .one();
  }

  /**
   * Loads all songs from the database.
   *
   * @author paouelle
   *
   * @return a stream of all songs loaded from the db
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
  public static Stream<Song> all() {
    return StatementBuilder.select(Song.class)
      .all()
      .from(Constants.SONGS)
      .execute()
      .stream();
  }

  /**
   * Holds the unique song id.
   *
   * @author paouelle
   */
  @lombok.NonNull
  @Column(name=Constants.ID)
  @PartitionKey
  @Mandatory
  private UUID id;

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
  @Mandatory
  private String artist;

  /**
   * Holds the song's data.
   *
   * @author paouelle
   */
  @Column(name=Constants.DATA)
  private ByteBuffer data;

  /**
   * Saves this song directly in the database.
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
   * Updates this song directly in the database.
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

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

/**
 * The <code>Constants</code> class defines various constants used by the
 * music service example.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 23, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
@SuppressWarnings("javadoc")
public abstract class Constants {
  /**
   * Holds keyspace constants.
   *
   * @author paouelle
   */
  public static final String MUSIC = "music";

  /**
   * Holds table constants.
   *
   * @author paouelle
   */
  public static final String SONGS = "songs";
  public static final String PLAYLISTS = "playlists";

  /**
   * Holds column constants.
   *
   * @author paouelle
   */
  public static final String ID = "id";
  public static final String TITLE = "title";
  public static final String ALBUM = "album";
  public static final String ARTIST = "artist";
  public static final String DATA = "data";
  public static final String SONG_ORDER = "song_order";
  public static final String SONG_ID = "song_id";

  /**
   * Prevents instantiation.
   *
   * @author paouelle
   */
  private Constants() {}
}

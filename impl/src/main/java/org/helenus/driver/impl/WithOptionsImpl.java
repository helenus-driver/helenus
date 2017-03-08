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
package org.helenus.driver.impl;

import java.io.StringReader;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonReader;
import javax.json.stream.JsonParsingException;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ArrayUtils;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ParseUtils;
import com.datastax.driver.core.TypeCodec;

import org.helenus.driver.WithOptions;
import org.helenus.driver.codecs.provider.VarCharCodecProvider;
import org.helenus.driver.persistence.CompactionClass;
import org.helenus.driver.persistence.Keyspace;
import org.helenus.driver.persistence.PlacementClass;
import org.helenus.driver.persistence.Table;

/**
 * The <code>WithOptionsImpl</code> class defines options to be used when
 * creating indexes.
 *
 * @copyright 2015-2017 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 17, 2017 - paouelle - Creation
 *
 * @since 1.0
 */
public class WithOptionsImpl
  extends Utils.Appendeable
  implements WithOptions {
  /**
   * Holds the name for the options.
   *
   * @author paouelle
   */
  private final String name;

  /**
   * Holds the value for the options.
   *
   * @author paouelle
   */
  private final Object value;

  /**
   * Instantiates a new <code>WithOptionsImpl</code> object.
   *
   * @author paouelle
   *
   * @param  name the name for the option
   * @param  value the option's value
   * @throws NullPointerException if <code>name</code> or <code>value</code> is
   *         <code>null</code>
   */
  public WithOptionsImpl(String name, Object value) {
    org.apache.commons.lang3.Validate.notNull(name, "invalid null name");
    org.apache.commons.lang3.Validate.notNull(value, "invalid null value");
    this.name = name;
    this.value = value;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.Utils.Appendeable#appendTo(java.lang.String, org.helenus.driver.impl.TableInfoImpl, com.datastax.driver.core.TypeCodec, com.datastax.driver.core.CodecRegistry, java.lang.StringBuilder, java.util.List)
   */
  @Override
  void appendTo(
    String keyspace,
    TableInfoImpl<?> tinfo,
    TypeCodec<?> codec,
    CodecRegistry codecRegistry,
    StringBuilder sb,
    List<Object> variables
  ) {
    sb.append(name).append('=');
    if ((codec == null) && (value instanceof JsonObject)) {
      // special case for keyspace options as we want to convert the Json object
      // into a Cassandra map and not in a varchar
      final StringBuilder vsb = new StringBuilder(80);

      Utils.appendValue(
        null, VarCharCodecProvider.JSON_CODEC, codecRegistry, vsb, value, variables
      );
      // strip surrounding quotes and
      // convert all ' in " and vice versa as Cassandra doesn't like "" inside maps
      sb.append(
        ParseUtils.unquote(vsb.toString())
          .replaceAll("'", "'QUOTE'")
          .replaceAll("\"", "'")
          .replaceAll("\\\\'", "\"")
          .replaceAll("'QUOTE'", "\\\\'")
      );
    } else {
      Utils.appendValue(null, codec, codecRegistry, sb, value, variables);
    }
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.Utils.Appendeable#containsBindMarker()
   */
  @Override
  boolean containsBindMarker() {
    return false;
  }

  /**
   * Gets the name of the option.
   *
   * @author paouelle
   *
   * @return the name of the option
   */
  public String getName() {
    return name;
  }

  /**
   * The <code>ReplicationWithImpl</code> class defines the "REPLICATION"
   * option for the "CREATE KEYSPACE" statement.
   *
   * @copyright 2015-2017 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 19, 2015 - paouelle - Creation
   *
   * @since 1.0
   */
  public static class ReplicationWithImpl extends WithOptionsImpl {
    /**
     * Constant for the replication option name.
     *
     * @author paouelle
     */
    public final static String NAME = "REPLICATION";

    /**
     * Computes the replication properties from the specified @Keyspace
     * annotation.
     *
     * @author paouelle
     *
     * @param  cinfo the non-<code>null</code> class info for the pojo class
     *         where to get the replication information
     * @param  mgr the statement manager from which to extract tool options
     * @return the corresponding Json object for the keyspace properties
     */
    @SuppressWarnings("fallthrough")
    private static JsonObject getReplicationProperties(
      ClassInfoImpl<?> cinfo, StatementManagerImpl mgr
    ) {
      final Keyspace keyspace = cinfo.getKeyspace();
      final JsonObjectBuilder jbuild = Json.createObjectBuilder();
      PlacementClass strategy = keyspace.placement();

      switch (strategy) {
        case NETWORK_TOPOLOGY:
          final Map<String, Integer> dcs;

          if (ArrayUtils.isEmpty(keyspace.dataCenter())) {
            // must have default ones then otherwise fall-through
            dcs = mgr.getDefaultDataCenters();
            if (MapUtils.isEmpty(dcs)) {
              strategy = PlacementClass.SIMPLE;
            }
          } else {
            final int[] factors = keyspace.replicationFactor();
            final String[] names = keyspace.dataCenter();

            org.apache.commons.lang3.Validate.isTrue(
              names.length == factors.length,
              "mismatch number of data centers and replication factors for network topology in %s",
              cinfo.getObjectClass().getName()
            );
            dcs = new LinkedHashMap<>(names.length * 3 / 2);
            for (int i = 0; i < names.length; i++) {
              dcs.put(names[i], factors[i]);
            }
          }
          if (strategy == PlacementClass.NETWORK_TOPOLOGY) {
            dcs.entrySet().forEach(e -> jbuild.add(e.getKey(), e.getValue()));
            break;
          } // else - fall-through
        case SIMPLE:
          int replicationFactor;

          if (ArrayUtils.isEmpty(keyspace.replicationFactor())) {
            replicationFactor = 0;
          } else {
            org.apache.commons.lang3.Validate.isTrue(
              keyspace.replicationFactor().length == 1,
              "too many replication factors speecified for simple strategy in %s",
              cinfo.getObjectClass().getName()
            );
            replicationFactor = keyspace.replicationFactor()[0];
          }
          if (replicationFactor == 0) { // fallback to manager's default
            replicationFactor = mgr.getDefaultReplicationFactor();
          }
          if (replicationFactor == 0) { // fallback to system default
            replicationFactor = 3;
          }
          jbuild.add("replication_factor", replicationFactor);
          break;
      }
      jbuild.add("class", strategy.NAME);
      return jbuild.build();
    }

    /**
     * Instantiates a new <code>ReplicationWithImpl</code> object.
     *
     * @author paouelle
     *
     * @param  value the option's value
     * @throws NullPointerException if <code>value</code> is <code>null</code>
     */
    public ReplicationWithImpl(Object value) {
      super(ReplicationWithImpl.NAME, value);
    }

    /**
     * Instantiates a new <code>ReplicationWithImpl</code> object.
     *
     * @author paouelle
     *
     * @param cinfo the non-<code>null</code> class info for the pojo class
     *        where to get the replication information
     * @param mgr the statement manager from which to extract tool options
     */
    public ReplicationWithImpl(ClassInfoImpl<?> cinfo, StatementManagerImpl mgr) {
      this(ReplicationWithImpl.getReplicationProperties(cinfo, mgr));
    }
  }

  /**
   * The <code>CompactionWithImpl</code> class defines the "COMPACTION"
   * option for the "CREATE TABLE" statement.
   *
   * @copyright 2015-2017 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Mar 7, 2017 - paouelle - Creation
   *
   * @since 1.0
   */
  public static class CompactionWithImpl extends WithOptionsImpl {
    /**
     * Constant for the compaction option name.
     *
     * @author paouelle
     */
    public final static String NAME = "COMPACTION";

    /**
     * Computes the compaction properties from the specified @Table annotation.
     *
     * @author paouelle
     *
     * @param  tinfo the non-<code>null</code> table info for the pojo class
     *         where to get the compaction information
     * @param  mgr the statement manager from which to extract tool options
     * @return the corresponding Json object for the table properties
     * @throws JsonParsingException if a JSON object cannot be created due to
     *         incorrect representation
     */
    private static JsonObject getCompactionProperties(
      TableInfoImpl<?> tinfo, StatementManagerImpl mgr
    ) {
      final Table table = tinfo.getTable();
      final JsonObjectBuilder jbuild = Json.createObjectBuilder();
      final CompactionClass compaction = table.compaction();
      final String compactionOptions = table.compactionOptions();

      if (!compactionOptions.isEmpty()) {
        try (
          final JsonReader jr = Json.createReader(new StringReader(compactionOptions.replaceAll("'", "\"")));
        ) {
          final JsonObject jo = jr.readObject();

          jo.forEach((k, v) -> jbuild.add(k, v));
        }
      }
      jbuild.add("class", compaction.NAME);
      return jbuild.build();
    }

    /**
     * Instantiates a new <code>CompactionWithImpl</code> object.
     *
     * @author paouelle
     *
     * @param  value the option's value
     * @throws NullPointerException if <code>value</code> is <code>null</code>
     */
    public CompactionWithImpl(Object value) {
      super(CompactionWithImpl.NAME, value);
    }

    /**
     * Instantiates a new <code>CompactionWithImpl</code> object.
     *
     * @author paouelle
     *
     * @param  tinfo the non-<code>null</code> table info for the pojo class
     *         where to get the compaction information
     * @param  mgr the statement manager from which to extract tool options
     * @throws JsonParsingException if a JSON object cannot be created due to
     *         incorrect representation
     */
    public CompactionWithImpl(TableInfoImpl<?> tinfo, StatementManagerImpl mgr) {
      this(CompactionWithImpl.getCompactionProperties(tinfo, mgr));
    }
  }
}

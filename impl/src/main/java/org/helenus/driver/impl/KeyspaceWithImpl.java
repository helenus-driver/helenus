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
package org.helenus.driver.impl;

import java.util.LinkedHashMap;
import java.util.Map;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ArrayUtils;

import org.helenus.driver.KeyspaceWith;
import org.helenus.driver.persistence.Keyspace;
import org.helenus.driver.persistence.StrategyClass;

/**
 * The <code>KeyspaceWithImpl</code> class defines options to be used when
 * creating keyspaces.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 19, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
public class KeyspaceWithImpl
  extends Utils.Appendeable
  implements KeyspaceWith {
  /**
   * Holds the name for this option.
   *
   * @author paouelle
   */
  private final String name;

  /**
   * Holds the value for the option.
   *
   * @author paouelle
   */
  private final Object value;

  /**
   * Instantiates a new <code>KeyspaceWithImpl</code> object.
   *
   * @author paouelle
   *
   * @param  name the name of the option
   * @param  value the option's value
   * @throws NullPointerException if <code>name</code> or <code>value</code>
   *         is <code>null</code>
   */
  public KeyspaceWithImpl(String name, Object value) {
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
   * @see org.helenus.driver.impl.Utils.Appendeable#appendTo(org.helenus.driver.impl.TableInfoImpl, java.lang.StringBuilder)
   */
  @Override
  void appendTo(TableInfoImpl<?> tinfo, StringBuilder sb) {
    sb.append(name).append("=");
    Utils.appendValue(value, sb);
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
   * @copyright 2015-2015 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jan 19, 2015 - paouelle - Creation
   *
   * @since 1.0
   */
  public static class ReplicationWithImpl extends KeyspaceWithImpl {
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
      StrategyClass strategy = keyspace.strategy();

      switch (strategy) {
        case NETWORK_TOPOLOGY:
          final Map<String, Integer> dcs;

          if (ArrayUtils.isEmpty(keyspace.dataCenter())) {
            // must have default ones then otherwise fall-through
            dcs = mgr.getDefaultDataCenters();
            if (MapUtils.isEmpty(dcs)) {
              strategy = StrategyClass.SIMPLE;
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
          if (strategy == StrategyClass.NETWORK_TOPOLOGY) {
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
            replicationFactor = 2;
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
     * @throws NullPointerException if <code>name</code> or <code>value</code>
     *         is <code>null</code>
     */
    public ReplicationWithImpl(Object value) {
      super("REPLICATION", value);
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
}

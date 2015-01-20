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
package com.github.helenus.driver.impl;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

import com.github.helenus.driver.KeyspaceWith;
import com.github.helenus.persistence.Keyspace;

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
   * @see com.github.helenus.driver.impl.Utils.Appendeable#appendTo(com.github.helenus.driver.impl.TableInfoImpl, java.lang.StringBuilder)
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
     * @param  keyspace the keyspace from which to compute the properties
     * @param mgr the statement manager from which to extract tool options
     * @return the corresponding Json object for the keyspace properties
     */
    private static JsonObject getReplicationProperties(
      Keyspace keyspace, StatementManagerImpl mgr
    ) {
      final JsonObjectBuilder jbuild = Json.createObjectBuilder();

      switch (keyspace.strategy()) {
        case SIMPLE:
          int replicationFactor = keyspace.replicationFactor();

          if (replicationFactor == 0) { // fallback to manager's default
            replicationFactor = mgr.getDefaultReplicationFactor();
          }
          if (replicationFactor == 0) { // fallback to system default
            replicationFactor = 2;
          }
          jbuild.add("replication_factor", replicationFactor);
          break;
        case NETWORK_TOPOLOGY:
          // TODO: handle network topology strategy class
          break;
      }
      jbuild.add("class", keyspace.strategy().NAME);
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
     * @param keyspace the @Keyspace annotation from which to get the
     *        replication option
     * @param mgr the statement manager from which to extract tool options
     */
    public ReplicationWithImpl(Keyspace keyspace, StatementManagerImpl mgr) {
      this(ReplicationWithImpl.getReplicationProperties(keyspace, mgr));
    }
  }
}

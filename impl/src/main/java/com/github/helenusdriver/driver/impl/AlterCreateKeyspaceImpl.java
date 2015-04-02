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
package com.github.helenusdriver.driver.impl;

import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;
import com.github.helenusdriver.driver.StatementBridge;

/**
 * The <code>AlterCreateKeyspaceImpl</code> class defines a ALTER KEYSPACE or a
 * CREATE KEYSPACE statement based on the current schema defined in Cassandra.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Apr 2, 2015 - paouelle - Creation
 *
 * @param <T> The type of POJO associated with this statement.
 *
 * @since 1.0
 */
public class AlterCreateKeyspaceImpl<T> extends CreateKeyspaceImpl<T> {
  /**
   * Instantiates a new <code>AlterCreateKeyspaceImpl</code> object.
   *
   * @author paouelle
   *
   * @param context the non-<code>null</code> class info context for the POJO
   *        associated with this statement
   * @param mgr the non-<code>null</code> statement manager
   * @param bridge the non-<code>null</code> statement bridge
   */
  public AlterCreateKeyspaceImpl(
    ClassInfoImpl<T>.Context context,
    StatementManagerImpl mgr,
    StatementBridge bridge
  ) {
    super(context, mgr, bridge);
    ifNotExists(); // in case we have 2 alter statements in a sequence
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenusdriver.driver.impl.StatementImpl#buildQueryString()
   */
  @SuppressWarnings("synthetic-access")
  @Override
  protected StringBuilder buildQueryString() {
    // start by querying Cassandra system table to figure out the existing schema
    // for the required keyspace
    final Row row = mgr.getSession().executeAsync(new SimpleStatement(
      "SELECT durable_writes,strategy_class,strategy_options FROM system.schema_keyspaces WHERE keyspace_name='"
      + getKeyspace()
      + "' LIMIT 1"
    )).getUninterruptibly().one();

    if (row == null) { // that would mean the keyspace doesn't exist
      return super.buildQueryString();
    }
    final StringBuilder builder = new StringBuilder();

    builder.append("ALTER KEYSPACE ");
    Utils.appendName(getKeyspace(), builder);
    KeyspaceWithImpl.ReplicationWithImpl replication = with.replication;

    if (replication == null) { // default to POJO's details
      replication = new KeyspaceWithImpl.ReplicationWithImpl(
        getContext().getClassInfo(), mgr
      );
    }
    final List<KeyspaceWithImpl> options = new ArrayList<>(with.options.size() + 1);

    options.add(replication);
    options.addAll(with.options);
    builder.append(" WITH ");
    Utils.joinAndAppend(null, builder, " AND ", options);
    builder
      .append(" AND DURABLE_WRITES = ")
      .append(getContext().getClassInfo().getKeyspace().durableWrites());
    return builder;
  }
}

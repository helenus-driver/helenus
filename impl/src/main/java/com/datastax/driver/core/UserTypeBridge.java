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
package com.datastax.driver.core;

import java.util.ArrayList;
import java.util.List;

import org.helenus.driver.impl.FieldInfoImpl;
import org.helenus.driver.impl.StatementManagerImpl;
import org.helenus.driver.impl.TableInfoImpl;
import org.helenus.driver.impl.UDTClassInfoImpl;
import org.helenus.driver.impl.UDTTypeClassInfoImpl;

/**
 * The <code>UserTypeBridge</code> class is used to access protected constructors
 * from the {@link UserType} class.
 *
 * @copyright 2015-2017 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 6, 2017 - paouelle - Creation
 *
 * @since 3.0
 */
public class UserTypeBridge {
  /**
   * Instantiates a new <code>UserType</code> object.
   *
   * @author paouelle
   *
   * @param  mgr the statement manager
   * @param  ucinfo the UDT class info for which to instantiate a Cassandra user type
   * @param  keyspace the keyspace for which to create a user type
   * @return the newly created corresponding Cassandra user type
   */
  public static UserType instantiate(
    StatementManagerImpl mgr, UDTClassInfoImpl<?> ucinfo, String keyspace
  ) {
    final TableInfoImpl<?> table = ucinfo.getTableImpl();
    final List<UserType.Field> fields = new ArrayList<>(table.getColumns().size());

    for (final FieldInfoImpl<?> field: table.getColumnsImpl()) {
      if (field.isTypeKey() && (ucinfo instanceof UDTTypeClassInfoImpl)) {
        // don't persist type keys for those (only for UDT root entities)
        continue;
      }
      fields.add(new UserType.Field(field.getColumnName(), field.getDataType().getDataType()));
    }
    return new UserType(
      keyspace,
      ucinfo.getName(),
      fields,
      mgr.getProtocolVersion(),
      mgr.getCodecRegistry()
    );
  }
}

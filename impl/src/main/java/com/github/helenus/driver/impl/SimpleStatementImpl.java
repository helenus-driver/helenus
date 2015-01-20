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

import org.apache.commons.lang3.StringUtils;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.querybuilder.QueryBuilderBridge;

import com.github.helenus.driver.RegularStatement;
import com.github.helenus.driver.StatementBridge;

/**
 * The <code>SimpleStatementImpl</code> class extends the functionality of Cassandra's
 * {@link com.datastax.driver.core.SimpleStatement} class to provide
 * support for POJOs.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 19, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
public class SimpleStatementImpl
  extends StatementImpl<ResultSet, ResultSetFuture, Void>
  implements RegularStatement {
  /**
   * Holds the query string for the statement.
   *
   * @author paouelle
   */
  private final String query;

  /**
   * Instantiates a new <code>SimpleStatementImpl</code> object.
   *
   * @author paouelle
   *
   * @param  query the query string for the statement
   * @param  mgr the non-<code>null</code> statement manager
   * @param  bridge the non-<code>null</code> statement bridge
   * @throws NullPointerException if <code>query</code> is <code>null</code>
   */
  public SimpleStatementImpl(
    String query,
    StatementManagerImpl mgr,
    StatementBridge bridge
  ) {
    super(ResultSet.class, (String)null, mgr, bridge);
    org.apache.commons.lang3.Validate.notNull(query, "invalid null query");
    this.query = query.trim();
  }

  /**
   * Instantiates a new <code>SimpleStatementImpl</code> object.
   *
   * @author paouelle
   *
   * @param  statement the raw statement to wrap as a simple one
   * @param  mgr the non-<code>null</code> statement manager
   * @param  bridge the non-<code>null</code> statement bridge
   * @throws NullPointerException if <code>statement</code> is <code>null</code>
   */
  public SimpleStatementImpl(
    com.datastax.driver.core.RegularStatement statement,
    StatementManagerImpl mgr,
    StatementBridge bridge
  ) {
    super(
      ResultSet.class,
      (statement instanceof com.datastax.driver.core.SimpleStatement)
        ? ((com.datastax.driver.core.SimpleStatement)statement).getKeyspace()
        : QueryBuilderBridge.getKeyspace(statement),
      mgr,
      bridge
    );
    org.apache.commons.lang3.Validate.notNull(statement, "invalid null statement");
    this.query = statement.getQueryString().trim();
    setCounterOp(QueryBuilderBridge.isCounterOp(statement));
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenus.driver.impl.StatementImpl#buildQueryStrings()
   */
  @Override
  protected StringBuilder[] buildQueryStrings() {
    return new StringBuilder[] { new StringBuilder(query) };
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see com.github.helenus.driver.impl.StatementImpl#getQueryString()
   */
  @Override
  public String getQueryString() {
    return query;
  }

  /**
   * Checks if the query string represents a "select" statement.
   *
   * @author paouelle
   *
   * @return <code>true</code> if the query string represents a "select";
   *         <code>false</code> otherwise
   */
  public boolean isSelect() {
    // don't any better ways to do this
    return StringUtils.startsWithIgnoreCase(query, "select");
  }

  /**
   * Checks if the query string represents a "batch" statement.
   *
   * @author paouelle
   *
   * @return <code>true</code> if the query string represents a "batch";
   *         <code>false</code> otherwise
   */
  public boolean isBatch() {
    // don't any better ways to do this
    return StringUtils.startsWithIgnoreCase(query, "begin batch");
  }
}

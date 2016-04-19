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

import java.util.List;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.policies.RetryPolicy;
import com.google.common.util.concurrent.ListenableFuture;

import org.helenus.driver.GenericStatement;

/**
 * The <code>ForwardingStatementImpl</code> class defines a utility class to
 * create a statement that encapsulate another one.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 19, 2015 - paouelle - Creation
 *
 * @param <R> The type of result returned when executing this statement
 * @param <F> The type of future result returned when executing this statement
 * @param <T> The type of POJO associated with this statement
 * @param <S> The type of statement encapsulated
 *
 * @since 1.0
 */
public abstract class ForwardingStatementImpl<R, F extends ListenableFuture<R>, T, S extends StatementImpl<R, F, T>>
  extends StatementImpl<R, F, T> {
  /**
   * Holds the encapsulated statement.
   *
   * @author paouelle
   */
  protected S statement;

  /**
   * Instantiates a new <code>ForwardingStatementImpl</code> object.
   *
   * @author paouelle
   *
   * @param statement the encapsulated statement
   */
  protected ForwardingStatementImpl(S statement) {
    super(statement);
    this.statement = statement;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.StatementImpl#buildQueryStrings()
   */
  @Override
  protected StringBuilder[] buildQueryStrings() {
    return statement.buildQueryStrings();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.StatementImpl#appendGroupType(java.lang.StringBuilder)
   */
  @Override
  protected void appendGroupType(StringBuilder builder) {
    statement.appendGroupSubType(builder);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.StatementImpl#appendGroupSubType(java.lang.StringBuilder)
   */
  @Override
  protected void appendGroupSubType(StringBuilder builder) {
    statement.appendGroupSubType(builder);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.StatementImpl#appendOptions(java.lang.StringBuilder)
   */
  @Override
  protected void appendOptions(StringBuilder builder) {
    statement.appendOptions(builder);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.StatementImpl#buildStatements()
   */
  @Override
  protected List<StatementImpl<?, ?, ?>> buildStatements() {
    return statement.buildStatements();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.StatementImpl#buildQueryString()
   */
  @Override
  protected StringBuilder buildQueryString() {
    return statement.buildQueryString();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.StatementImpl#setDirty()
   */
  @Override
  protected void setDirty() {
    statement.setDirty();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.StatementImpl#setDirty(boolean)
   */
  @Override
  protected void setDirty(boolean recurse) {
    statement.setDirty(recurse);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.StatementImpl#isDirty()
   */
  @Override
  protected boolean isDirty() {
    return statement.isDirty();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.StatementImpl#simpleSize()
   */
  @Override
  protected int simpleSize() {
    return statement.simpleSize();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.StatementImpl#isCounterOp()
   */
  @Override
  protected boolean isCounterOp() {
    return statement.isCounterOp();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.StatementImpl#setCounterOp(boolean)
   */
  @Override
  protected void setCounterOp(boolean isCounterOp) {
    statement.setCounterOp(isCounterOp);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.StatementImpl#getObjectClass()
   */
  @Override
  public Class<T> getObjectClass() {
    return statement.getObjectClass();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.StatementImpl#getContext()
   */
  @Override
  public ClassInfoImpl<T>.Context getContext() {
    return statement.getContext();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.StatementImpl#getPOJOContext()
   */
  @Override
  public ClassInfoImpl<T>.POJOContext getPOJOContext() {
    return statement.getPOJOContext();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.StatementImpl#getKeyspace()
   */
  @Override
  public String getKeyspace() {
    return statement.getKeyspace();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.StatementImpl#enable()
   */
  @Override
  public GenericStatement<R, F> enable() {
    return statement.enable();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.StatementImpl#disable()
   */
  @Override
  public GenericStatement<R, F> disable() {
    return statement.disable();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.StatementImpl#isEnabled()
   */
  @Override
  public boolean isEnabled() {
    return statement.isEnabled();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.StatementImpl#setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel)
   */
  @Override
  public GenericStatement<R, F> setConsistencyLevel(ConsistencyLevel consistency) {
    return statement.setConsistencyLevel(consistency);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.StatementImpl#getConsistencyLevel()
   */
  @Override
  public ConsistencyLevel getConsistencyLevel() {
    return statement.getConsistencyLevel();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.StatementImpl#setSerialConsistencyLevel(com.datastax.driver.core.ConsistencyLevel)
   */
  @Override
  public GenericStatement<R, F> setSerialConsistencyLevel(
    ConsistencyLevel serialConsistency
      ) {
    return statement.setSerialConsistencyLevel(serialConsistency);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.StatementImpl#getSerialConsistencyLevel()
   */
  @Override
  public ConsistencyLevel getSerialConsistencyLevel() {
    return statement.getSerialConsistencyLevel();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.StatementImpl#enableTracing()
   */
  @Override
  public GenericStatement<R, F> enableTracing() {
    return statement.enableTracing();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.StatementImpl#enableTracing(java.lang.String)
   */
  @Override
  public GenericStatement<R, F> enableTracing(String prefix) {
    return statement.enableTracing(prefix);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.StatementImpl#disableTracing()
   */
  @Override
  public GenericStatement<R, F> disableTracing() {
    return statement.disableTracing();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.StatementImpl#isTracing()
   */
  @Override
  public boolean isTracing() {
    return statement.isTracing();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.StatementImpl#enableErrorTracing()
   */
  @Override
  public GenericStatement<R, F> enableErrorTracing() {
    return statement.enableErrorTracing();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.StatementImpl#enableErrorTracing(java.lang.String)
   */
  @Override
  public GenericStatement<R, F> enableErrorTracing(String prefix) {
    return statement.enableErrorTracing(prefix);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.StatementImpl#disableErrorTracing()
   */
  @Override
  public GenericStatement<R, F> disableErrorTracing() {
    return statement.disableErrorTracing();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.StatementImpl#isErrorTracing()
   */
  @Override
  public boolean isErrorTracing() {
    return statement.isErrorTracing();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.StatementImpl#setRetryPolicy(com.datastax.driver.core.policies.RetryPolicy)
   */
  @Override
  public GenericStatement<R, F> setRetryPolicy(RetryPolicy policy) {
    return statement.setRetryPolicy(policy);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.StatementImpl#getRetryPolicy()
   */
  @Override
  public RetryPolicy getRetryPolicy() {
    return statement.getRetryPolicy();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.StatementImpl#setFetchSize(int)
   */
  @Override
  public GenericStatement<R, F> setFetchSize(int fetchSize) {
    return statement.setFetchSize(fetchSize);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.StatementImpl#getFetchSize()
   */
  @Override
  public int getFetchSize() {
    return statement.getFetchSize();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.StatementImpl#getQueryString()
   */
  @Override
  public String getQueryString() {
    return statement.getQueryString();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.StatementImpl#getUserData()
   */
  @Override
  public <U> U getUserData() {
    return statement.getUserData();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.StatementImpl#setUserData(java.lang.Object)
   */
  @Override
  public <U> void setUserData(U data) {
    statement.setUserData(data);
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.StatementImpl#execute()
   */
  @Override
  public R execute() {
    return statement.execute();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.StatementImpl#executeAsync()
   */
  @Override
  public F executeAsync() {
    return statement.executeAsync();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.StatementImpl#executeRaw()
   */
  @Override
  public ResultSet executeRaw() {
    return statement.executeRaw();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.helenus.driver.impl.StatementImpl#executeAsyncRaw()
   */
  @Override
  public ResultSetFuture executeAsyncRaw() {
    return statement.executeAsyncRaw();
  }
}

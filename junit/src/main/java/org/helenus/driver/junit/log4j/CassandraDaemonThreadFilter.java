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
package org.helenus.driver.junit.log4j;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.config.Node;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.filter.AbstractFilter;
import org.apache.logging.log4j.message.Message;

import org.helenus.driver.junit.HelenusJUnit;

/**
 * The <code>CassandraDaemonThreadFilter</code> class provides an implementation
 * for a log4j filter that checks if the current thread part of the Cassandra
 * daemon.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jun 28, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
@Plugin(
  name="CassandraDaemonThreadFilter",
  category=Node.CATEGORY,
  elementType=Filter.ELEMENT_TYPE,
  printObject=true
)
public class CassandraDaemonThreadFilter extends AbstractFilter {
  /**
   * Holds the serialVersionUID.
   *
   * @author paouelle
   */
  private static final long serialVersionUID = -1128269232208515069L;

  /**
   * Create a new <code>CassandraDaemonThreadFilter</code> object.
   *
   * @author paouelle
   *
   * @param  onMatch the result to return when a match occurs
   * @param  onMismatch the result to return when a match does not occur
   * @return the corresponding non-<code>null</code> filter
   */
  @PluginFactory
  public static CassandraDaemonThreadFilter createFilter(
    @PluginAttribute(value="onMatch") Result onMatch,
    @PluginAttribute(value="onMismatch") Result onMismatch
  ) {
    return new CassandraDaemonThreadFilter(onMatch, onMismatch);
  }

  /**
   * Instantiates a new <code>CassandraDaemonThreadFilter</code> object.
   *
   * @author paouelle
   *
   * @param onMatch the result to return when a match occurs
   * @param onMismatch the result to return when a match does not occur
   */
  public CassandraDaemonThreadFilter(Result onMatch, Result onMismatch) {
    super(onMatch, onMismatch);
  }

  /**
   * Checks if the current thread is a Cassandra daemon thread and return either
   * {@link #onMatch} or {@link #onMismatch}.
   *
   * @author paouelle
   *
   * @return the result that corresponds to this filter's condition
   */
  private Result filter() {
    if (HelenusJUnit.isCassandraDaemonThread(Thread.currentThread())) {
      return onMatch;
    }
    return onMismatch;
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.apache.logging.log4j.core.filter.AbstractFilter#filter(org.apache.logging.log4j.core.LogEvent)
   */
  @Override
  public Result filter(LogEvent event) {
    return filter();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.apache.logging.log4j.core.filter.AbstractFilter#filter(org.apache.logging.log4j.core.Logger, org.apache.logging.log4j.Level, org.apache.logging.log4j.Marker, org.apache.logging.log4j.message.Message, java.lang.Throwable)
   */
  @Override
  public Result filter(
    Logger logger,
    Level level,
    Marker marker,
    Message msg,
    Throwable t
  ) {
    return filter();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.apache.logging.log4j.core.filter.AbstractFilter#filter(org.apache.logging.log4j.core.Logger, org.apache.logging.log4j.Level, org.apache.logging.log4j.Marker, java.lang.Object, java.lang.Throwable)
   */
  @Override
  public Result filter(
    Logger logger,
    Level level,
    Marker marker,
    Object msg,
    Throwable t
  ) {
    return filter();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.apache.logging.log4j.core.filter.AbstractFilter#filter(org.apache.logging.log4j.core.Logger, org.apache.logging.log4j.Level, org.apache.logging.log4j.Marker, java.lang.String, java.lang.Object[])
   */
  @Override
  public Result filter(
    Logger logger,
    Level level,
    Marker marker,
    String msg,
    Object... params
  ) {
    return filter();
  }
}

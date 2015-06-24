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
package org.helenus.driver.catalina;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.datastax.driver.core.Cluster;

import org.apache.catalina.LifecycleException;
import org.apache.catalina.core.StandardService;
import org.helenus.driver.impl.StatementManagerImpl;

/**
 * The <code>HelenusService</code> extends on the {@link StandardService} in
 * order to provide support for Cassandra by initializing the Helenus driver.
 * <p>
 * To configure Tomcat, simply modify the ${TOMCAT_HOME}/conf/server.xml
 * by replacing the line:
 *   &lt;Service name="Catalina"&gt;
 * with:
 *   &lt;Service className="com.github.helenusdriver.driver.catalina.HelenusService" name="Catalina" hosts="127.0.0.1" port="9042"&gt;
 * <p>
 * Multiple seed hosts can be specified by separating them with a colon (:).
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Apr 16, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
public class HelenusService extends StandardService {
  /**
   * Holds the logger.
   *
   * @author paouelle
   */
  private final static Logger logger = LogManager.getFormatterLogger(HelenusService.class);

  /**
   * Holds the Cassandra host seeds to connect to.
   *
   * @author paouelle
   */
  private volatile String[] hosts = new String[] { "127.0.0.1" };

  /**
   * Holds the Cassandra port to connect to.
   *
   * @author paouelle
   */
  private volatile int port = 9042;

  /**
   * Holds the statement manager.
   *
   * @author paouelle
   */
  private volatile StatementManagerImpl mgr = null;

  /**
   * Instantiates a new <code>HelenusService</code> object.
   *
   * @author paouelle
   */
  public HelenusService() {
    logger.entry();
    logger.exit();
  }

  /**
   * Sets the Cassandra host seeds to connect to (separated by a ':').
   *
   * @author paouelle
   *
   * @param hosts the seed hosts to connect to
   */
  public void setHosts(String hosts) {
    logger.entry(hosts);
    this.hosts = hosts.split(":");
    logger.exit();
  }

  /**
   * Sets the Cassandra port to connect to.
   *
   * @author paouelle
   *
   * @param port the port to connect to
   */
  public void setPort(int port) {
    logger.entry(port);
    this.port = port;
    logger.exit();
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.apache.catalina.core.StandardService#startInternal()
   */
  @Override
  protected void startInternal() throws LifecycleException {
    logger.entry();
    try {
      super.startInternal();
      logger.exit();
    } catch (LifecycleException e) {
      throw logger.throwing(e);
    }
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.apache.catalina.core.StandardService#stopInternal()
   */
  @Override
  protected void stopInternal() throws LifecycleException {
    logger.entry();
    try {
      super.stopInternal();
      logger.exit();
    } catch (LifecycleException e) {
      throw logger.throwing(e);
    }
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.apache.catalina.core.StandardService#initInternal()
   */
  @Override
  protected void initInternal() throws LifecycleException {
    try {
      logger.entry();
      if (mgr == null) {
        this.mgr = new StatementManagerImpl(
          Cluster
            .builder()
            .withPort(port)
            .addContactPoints(hosts)
            .withQueryOptions(null),
          true
        );
      }
      super.initInternal();
      logger.exit();
    } catch (LifecycleException e) {
      throw logger.throwing(e);
    }
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.apache.catalina.core.StandardService#destroyInternal()
   */
  @Override
  protected void destroyInternal() throws LifecycleException {
    logger.entry();
    try {
      super.destroyInternal();
      if (mgr != null) {
        mgr.close(); // shutdown and wait for its completion
      }
      logger.exit();
    } catch (LifecycleException e) {
      throw logger.throwing(e);
    }
  }
}

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
package org.helenus.driver.junit;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import java.net.ServerSocket;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.service.CassandraDaemon;
import org.helenus.commons.collections.iterators.CombinationIterator;
import org.helenus.driver.CreateSchema;
import org.helenus.driver.Sequence;
import org.helenus.driver.StatementBuilder;
import org.helenus.driver.impl.ClassInfoImpl;
import org.helenus.driver.impl.StatementManagerImpl;
import org.helenus.driver.info.ClassInfo;
import org.helenus.driver.info.FieldInfo;
import org.helenus.driver.junit.util.Strings;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.yaml.snakeyaml.reader.UnicodeReader;

/**
 * The <code>HelenusJUnit</code> class provides the JUnit 4 definition for an
 * <code>@Rule</code> service that will take care of initializing an embedded
 * Cassandra server and the Helenus driver.
 * <p>
 * <i>Note:</i> This file is largely based on cassandra-unit.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jun 27, 2015 - paouelle - Creation
 *
 * @since 1.0
 */
public class HelenusJUnit implements TestRule {
  /**
   * Holds the logger.
   *
   * @author paouelle
   */
  final static Logger logger = LogManager.getFormatterLogger(HelenusJUnit.class);

  /**
   * Constant for the default timeout to wait for the Cassandra daemon to start
   * before failing.
   *
   * @author paouelle
   */
  public final static long DEFAULT_STARTUP_TIMEOUT = 10000L;

  /**
   * Constant for the default Cassandra configuration file which starts the
   * Cassandra daemon on a random free port.
   *
   * @author paouelle
   */
  public final static String DEFAULT_CFG_FILE = "cassandra.yaml";

  /**
   * Constant for the default runtime directory used by the embedded Cassandra
   * daemon.
   *
   * @author paouelle
   */
  private final static String RUNTIME_DIR = "target/helenus-unit";

  /**
   * Holds the regex pattern used to parse the Cassandra yaml configuration
   * file in search of ports.
   *
   * @author paouelle
   */
  private static Pattern pattern = Pattern.compile(
    "^([a-z_]+_port:\\s*)([0-9]+)\\s*$", Pattern.MULTILINE
  );

  /**
   * Holds the thread group used to combine all Cassandra daemon threads
   * together.
   *
   * @author paouelle
   */
  private static volatile ThreadGroup group = null;

  /**
   * Holds the Cassandra daemon when started.
   *
   * @author paouelle
   */
  private static CassandraDaemon daemon = null;

  /**
   * Holds the Helenus statement manager when started.
   *
   * @author paouelle
   */
  private static StatementManagerImpl manager = null;

  /**
   * Hold a set of pojo class for which we have loaded the schema definition
   * onto the embedded cassandra database.
   *
   * @author paouelle
   */
  private static final Set<Class<?>> schemas = new HashSet<>();

  /**
   * Holds the description for the current test method.
   *
   * @author paouelle
   */
  private static volatile Description description = null;

  /**
   * Holds the Cassandra config file name used to start the Cassandra daemon.
   *
   * @author paouelle
   */
  private static String config = null;

  /**
   * Reads the specified file fully based on the appropriate encoding.
   *
   * @author paouelle
   *
   * @param  file the non-<code>null</code> file to read
   * @return a non-<code>null</code> string of the complete content of the
   *         specified file
   * @throws IOException if an I/O error occurs while reading the file
   */
  private static String readFully(File file) throws IOException {
    try (
      final FileInputStream fis = new FileInputStream(file);
      final UnicodeReader ur = new UnicodeReader(fis);
    ) {
      return IOUtils.toString(ur);
    }
  }

  /**
   * Updates the specified yaml config file by searching for ports configured
   * with the value 0 and finding a free port to use instead.
   *
   * @author paouelle
   *
   * @param  cfgfile the non-<code>null</code> config file to update
   * @throws IOException if an I/O error occurs while updating the config file
   */
  private static void update(File cfgfile) throws IOException {
    final String yaml = HelenusJUnit.readFully(cfgfile);
    final StringBuffer sb = new StringBuffer(yaml.length() + 40);
    final Matcher m = HelenusJUnit.pattern.matcher(yaml);
    boolean updated = false; // until proven otherwise

    while (m.find()) {
      final String pname = m.group(1);
      final int port = Integer.parseInt(m.group(2));

      if (port == 0) {
        try (
          final ServerSocket socket = new ServerSocket(0);
        ) {
          m.appendReplacement(sb, pname + socket.getLocalPort());
          updated = true;
        }
      } else {
        m.appendReplacement(sb, m.group());
      }
    }
    m.appendTail(sb);
    if (updated) { // update the config file with the updated ports
      FileUtils.writeStringToFile(cfgfile, sb.toString(), "utf-8");
    }
  }

  /**
   * Cleans up the Cassandra runtime directories if they exist and creates
   * them if they do not exists.
   *
   * @author paouelle
   */
  private static void cleanupAndCreateCassandraDirectories() {
    try {
      DatabaseDescriptor.createAllDirectories(); // make sure they exist before cleanup up to avoid failures
      final File dir = new File(DatabaseDescriptor.getCommitLogLocation());

      if (!dir.exists()) {
        throw new FileNotFoundException(
          "missing cassandra commit directory: " + dir.getAbsolutePath()
        );
      }
      FileUtils.deleteDirectory(dir);
      for (final String dname: DatabaseDescriptor.getAllDataFileLocations()) {
        final File d = new File(dname);

        if (!d.exists()) {
          throw new FileNotFoundException(
            "missing cassandra data directory: " + d.getAbsolutePath()
          );
        }
        FileUtils.deleteDirectory(dir);
      }
      DatabaseDescriptor.createAllDirectories();
      CommitLog.instance.resetUnsafe(); // cleanup screws w/ CommitLog, this brings it back to safe state
    } catch (IOException e) {
      throw new AssertionError(e);
    }
  }

  /**
   * Checks if the specified suffix key value is a match for the specified pojo
   * class.
   *
   * @author paouelle
   *
   * @param  skv the non-<code>null</code> suffix key value annotation to check
   * @param  clazz the non-<code>null</code> pojo class to check
   * @return <code>true</code> if the suffix key value annotation is a match for
   *         the specified pojo class; <code>false</code> otherwise
   */
  private static boolean matches(SuffixKeyValues skv, Class<?> clazz) {
    // first check for class matches
    for (final Class<?> skvc: skv.classes()) {
      if (clazz.equals(skvc)) {
        return true;
      }
    }
    // next check for package match
    final String pkg = clazz.getPackage().getName();

    for (final String skvp: skv.packages()) {
      if (pkg.equals(skvp)) {
        return true;
      }
    }
    // finally check for the default (no classes & no pkgs defined)
    if ((skv.classes().length == 0) && (skv.packages().length == 0)) {
      return true;
    }
    return false;
  }

  /**
   * Find the suffix key values from the specified test definition that matches
   * the specified pojo class.
   *
   * @author paouelle
   *
   * @param  description the non-<code>null</code> test description
   * @param  cinfo the non-<code>null</code> pojo class information
   * @return a non-<code>null</code> collection of all suffix values keyed by
   *         their names based on the specified class info (each entry in the main
   *         collection provides another collection of all possible values for a
   *         given suffix)
   */
  private static Collection<Collection<Strings>> getSuffixKeyValues(
    Description description, ClassInfo<?> cinfo
  ) {
    final Class<?> clazz = cinfo.getObjectClass();
    final Map<String, Set<String>> suffixes = new LinkedHashMap<>(12);
    final Consumer<SuffixKeyValues> insert = skv -> {
      final FieldInfo<?> finfo = cinfo.getSuffixKeyByType(skv.type());

      if (finfo == null) { // pojo doesn't define this suffix type
        return;
      }
      suffixes.compute(
        finfo.getSuffixKeyName(), (t, s) -> {
          if (s == null) {
            s = new LinkedHashSet<>(Math.max(1, skv.values().length) * 3 / 2);
          }
          for (final String v: skv.values()) {
            s.add(v);
          }
          return s;
        }
      );
    };
    // start by looking at the annotations of the test method
    // no choices to check for the @Repeatable first as the Description class
    // doesn't support those yet
    final SuffixKeyValuess askvss = description.getAnnotation(SuffixKeyValuess.class);

    if (askvss != null) {
      for (final SuffixKeyValues askvs: askvss.value()) {
        if (HelenusJUnit.matches(askvs, clazz)) {
          insert.accept(askvs);
        }
      }
    }
    // and now check for a single SuffixKeyValues annotation
    final SuffixKeyValues askvs = description.getAnnotation(SuffixKeyValues.class);

    if ((askvs != null) && HelenusJUnit.matches(askvs, clazz)) {
      insert.accept(askvs);
    }
    // now check the class annotations for a match
    for (final SuffixKeyValues acskvs: description.getTestClass().getAnnotationsByType(SuffixKeyValues.class)) {
      if (HelenusJUnit.matches(acskvs, clazz)) {
        insert.accept(acskvs);
      }
    }
    return suffixes.entrySet().stream()
      .map(e -> e.getValue().stream()
        .map(v -> new Strings(e.getKey(), v))
        .collect(Collectors.toList())
      )
      .collect(Collectors.toList());
  }

  /**
   * Starts the embedded Cassandra daemon and the Helenus statement manager.
   *
   * @author paouelle
   *
   * @param  cfgname the non-<code>null</code> cassandra config resource name
   * @param  timeout the timeout to wait for the Cassandra daemon to start
   *         before failing
   * @throws IOException if an I/O error occurs while starting everything
   */
  private static synchronized void start(String cfgname, long timeout)
    throws IOException {
    if (HelenusJUnit.daemon != null) {
      // check if we are starting it with the same config
      if (config.equals(cfgname)) {
        logger.debug("Helenus already started using configuration '%s'", cfgname);
        return;
      }
      throw new AssertionError(
        "Helenus cannot be started again with a different configuration"
      );
    }
    HelenusJUnit.config = cfgname;
    logger.info("Starting Helenus...");
    // make sure the config resource is absolute
    cfgname = StringUtils.prependIfMissing(cfgname, "/");
    final File dir = new File(HelenusJUnit.RUNTIME_DIR);
    final File cfgfile = new File(
      dir, cfgname.substring(cfgname.lastIndexOf('/'))
    );

    // cleanup the runtime directory
    FileUtils.deleteDirectory(dir);
    // create the runtime directory
    FileUtils.forceMkdir(dir);
    // copy the resource config to the runtime directory
    final InputStream cfgis = HelenusJUnit.class.getResourceAsStream(cfgname);

    if (cfgis == null) {
      throw new AssertionError("failed to locate config resource: " + cfgname);
    }
    FileUtils.copyInputStreamToFile(cfgis, cfgfile);
    // now update the config with appropriate port numbers if random ports are requested
    HelenusJUnit.update(cfgfile);
    // update system properties for cassandra
    System.setProperty("cassandra.config", "file:" + cfgfile.getAbsolutePath());
    System.setProperty("cassandra-foreground", "true");
    System.setProperty("cassandra.native.epoll.enabled", "false"); // JNA doesn't cope with relocated netty
    // create a thread group for all Cassandra threads to be able to detect them
    HelenusJUnit.group = new ThreadGroup("Cassandra Daemon Group");
    // startup the cassandra daemon
    final CountDownLatch latch = new CountDownLatch(1);
    final Thread thread = new Thread(
      HelenusJUnit.group,
      new Runnable() {
        @SuppressWarnings("synthetic-access")
        @Override
        public void run() {
          // make sure the Cassandra runtime directories exists and are cleaned up
          HelenusJUnit.cleanupAndCreateCassandraDirectories();
          HelenusJUnit.daemon = new CassandraDaemon();
          daemon.activate();
          latch.countDown();
        }
      }
    );

    thread.start();
    // wait for the Cassandra daemon to start properly
    try {
      if (!latch.await(timeout, TimeUnit.MILLISECONDS)) {
        logger.error(
          "Cassandra daemon failed to start within "
          + timeout
          + "ms; increase the timeout"
        );
        throw new AssertionError(
          "cassandra daemon failed to start within timeout"
        );
      }
    } catch (InterruptedException e) {
      logger.error("interrupted waiting for cassandra daemon to start", e);
      throw new AssertionError(e);
    } finally {
      thread.interrupt();;
    }
    final String host = DatabaseDescriptor.getRpcAddress().getHostName();
    final int port = DatabaseDescriptor.getNativeTransportPort();

    logger.info("Cassandra started on '%s:%d'", host, port);
    // finally initializes helenus
    HelenusJUnit.manager = new StatementManagerUnitImpl(
      Cluster
        .builder()
        .withPort(port)
        .addContactPoint(host)
        .withQueryOptions(null)
    );
    // force default replication factor to 1
    HelenusJUnit.manager.setDefaultReplicationFactor(1);
  }

  /**
   * Checks if the specified thread is a Cassandra daemon thread.
   *
   * @author paouelle
   *
   * @param  thread the thread to check if it is a Cassandra daemon thread
   * @return <code>true</code> if the specified is a Cassandra daemon thread;
   *         <code>false</code> if it is not
   */
  public static boolean isCassandraDaemonThread(Thread thread) {
    if (thread != null) {
      ThreadGroup group = thread.getThreadGroup();

      while (group != null) {
        if (group == HelenusJUnit.group) {
          return true;
        }
        group = group.getParent();
      }
    }
    return false;
  }

  /**
   * Clears the database by resetting it to the same state it was before the
   * previous test case.
   *
   * @author paouelle
   */
  public static synchronized void clear() {
    // first drop all non-system keyspaces
    for (final KeyspaceMetadata keyspace: HelenusJUnit.manager.getCluster().getMetadata().getKeyspaces()) {
      final String kname = keyspace.getName();

      if (!"system".equals(kname) && !"system_auth".equals(kname) && !"system_traces".equals(kname)) {
        HelenusJUnit.manager.getSession().execute("DROP KEYSPACE " + kname);
      }
    }
    // next clear the cache of schemas
    HelenusJUnit.schemas.clear();
    // we can leave the pojo class infos loaded in Helenus'statement manager as
    // our hook will properly re-create the schemas if required
  }

  /**
   * Creates the schema for the specified pojo class onto the embedded Cassandra
   * database.
   *
   * @author paouelle
   *
   * @param  clazz the pojo class for which to create the schema
   * @throws NullPointerException if <code>clazz</code> is <code>null</code>
   */
  public static void createSchema(Class<?> clazz) {
    org.apache.commons.lang3.Validate.notNull(clazz, "invalid null class");
    // check whether the schema for this pojo has already been loaded
    synchronized (HelenusJUnit.schemas) {
      if (!HelenusJUnit.schemas.add(clazz)) {
        return; // nothing to do more
      }
      logger.debug("Creating schema for %s", clazz.getSimpleName());
      final ClassInfo<?> cinfo = manager.getClassInfoImpl(clazz);
      final Description description = HelenusJUnit.description;
      // find all suffixes that are defined for this classes
      final Collection<Collection<Strings>> suffixes = ((description != null)
        ? HelenusJUnit.getSuffixKeyValues(description, cinfo)
        : null
      );

      // now check if we have the right number of suffixes as if we don't, we
      // cannot create this schema
      if (cinfo.getNumSuffixKeys() != CollectionUtils.size(suffixes)) {
        throw new AssertionError(
          "unable to create schema for '"
          + clazz.getSimpleName()
          + "'; missing required suffix keys"
        );
      }
      // now generate as many create schemas statement as required by the combination
      // of all suffix values
      if (CollectionUtils.isEmpty(suffixes)) {
        // no suffixes so just the one create schema required
        final CreateSchema<?> cs = StatementBuilder.createSchema(clazz);

        cs.ifNotExists();
        cs.execute();
      } else {
        final Sequence s = StatementBuilder.sequence();

        for (final Iterator<List<Strings>> i = new CombinationIterator<>(Strings.class, suffixes); i.hasNext(); ) {
          final CreateSchema<?> cs = StatementBuilder.createSchema(clazz);

          cs.ifNotExists();
          // pass all required suffixes
          for (final Strings ss: i.next()) {
            // register the suffix value with the corresponding suffix name
            cs.where(StatementBuilder.eq(ss.key, ss.value));
          }
          s.add(cs);
        }
        s.execute();
      }
    }
  }

  /**
   * Holds the Cassandra config file name.
   *
   * @author paouelle
   */
  private final String cfgname;

  /**
   * Holds the startup timeout to wait for the Cassandra daemon to start before
   * failing.
   *
   * @author paouelle
   */
  private final long timeout;

  /**
   * Instantiates a new <code>HelenusJUnit</code> object.
   * <p>
   * <i>Note:</i> Defaults to 10 seconds timeout to wait for the Cassandra
   * daemon to start on a free port before failing.
   *
   * @author paouelle
   */
  public HelenusJUnit() {
    this(HelenusJUnit.DEFAULT_STARTUP_TIMEOUT);
  }

  /**
   * Instantiates a new <code>HelenusJUnit</code> object.
   * <p>
   * <i>Note:</i> Starts the Cassandra daemon on a free port before failing.
   *
   * @author paouelle
   *
   * @param timeout the maximum amount of time in milliseconds to wait for
   *        Cassandra daemon to start before failing
   */
  public HelenusJUnit(long timeout) {
    this(HelenusJUnit.DEFAULT_CFG_FILE, timeout);
  }

  /**
   * Instantiates a new <code>HelenusJUnit</code> object.
   *
   * @author paouelle
   *
   * @param cfgname the config file resource name to use to initialize the
   *        Cassandra daemon
   * @param timeout the maximum amount of time in milliseconds to wait for
   *        Cassandra daemon to start before failing
   * @throws NullPointerException if <code>cfgname</code> is <code>null</code>
   */
  public HelenusJUnit(String cfgname, long timeout) {
    org.apache.commons.lang3.Validate.notNull(cfgname, "invalid null config file");
    this.cfgname = cfgname;
    this.timeout = timeout;
  }

  /**
   * Called to initialize the Helenus/Cassandra environment.
   *
   * @author paouelle
   *
   * @param  description the description for the test method
   * @throws IOException if an I/O error occurs while initializing the cassandra
   *         daemon or the helenus statement manager
   */
  protected void before(Description description) throws IOException {
    // start embedded cassandra daemon
    synchronized (HelenusJUnit.class) {
      HelenusJUnit.start(cfgname, timeout);
      // make sure we are not running another test case
      if (HelenusJUnit.description != null) {
        throw logger.throwing(new AssertionError(
          "already running test case: " + HelenusJUnit.description
        ));
      }
      HelenusJUnit.description = description;
      // finally cleanup the database for this new test
      HelenusJUnit.clear();
    }
  }

  /**
   * Called to tear down the Helenus/Cassandra environment.
   *
   * @author paouelle
   *
   * @param description the description for the test method
   */
  protected void after(Description description) {
    // clear the current test description
    synchronized (HelenusJUnit.class) {
      if (HelenusJUnit.description == description) { // should always be true
        HelenusJUnit.description = null;
      }
    }
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.junit.rules.TestRule#apply(org.junit.runners.model.Statement, org.junit.runner.Description)
   */
  @Override
  public Statement apply(Statement base, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        before(description);
        try {
          base.evaluate();
        } finally {
          after(description);
        }
      }
    };
  }

  /**
   * The <code>StatementManagerUnitImpl</code> class extends the Helenus one
   * in order to hook the Helenus unit engine in it.
   *
   * @copyright 2015-2015 The Helenus Driver Project Authors
   *
   * @author  The Helenus Driver Project Authors
   * @version 1 - Jun 28, 2015 - paouelle - Creation
   *
   * @since 1.0
   */
  private static class StatementManagerUnitImpl extends StatementManagerImpl {
    /**
     * Instantiates a new <code>StatementManagerImpl</code> object.
     *
     * @author paouelle
     *
     * @param  initializer the cluster initializer to use to initialize Cassandra's
     *         cluster
     * @throws NullPointerException if <code>initializer</code> is <code>null</code>
     * @throws IllegalArgumentException if the list of contact points provided
     *         by <code>initializer</code> is empty or if not all those contact
     *         points have the same port.
     * @throws NoHostAvailableException if no Cassandra host amongst the contact
     *         points can be reached
     * @throws SecurityException if the statement manager reference has already
     *         been set
     */
    StatementManagerUnitImpl(Cluster.Initializer initializer) {
      super(initializer, true);
    }

    /**
     * {@inheritDoc}
     *
     * Overridden to automatically load the schemas of referenced pojos.
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.StatementManagerImpl#getClassInfoImpl(java.lang.Class)
     */
    @Override
    public <T> ClassInfoImpl<T> getClassInfoImpl(Class<T> clazz) {
      final ClassInfoImpl<T> classInfo = super.getClassInfoImpl(clazz);

      // load the schemas for the pojo if required
      HelenusJUnit.createSchema(clazz);
      return classInfo;
    }
  }
}


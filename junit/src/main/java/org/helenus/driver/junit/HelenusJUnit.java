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

import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import java.net.ServerSocket;

import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
import org.helenus.commons.lang3.reflect.ReflectionUtils;
import org.helenus.driver.Batch;
import org.helenus.driver.CreateSchema;
import org.helenus.driver.Sequence;
import org.helenus.driver.StatementBuilder;
import org.helenus.driver.impl.ClassInfoImpl;
import org.helenus.driver.impl.StatementManagerImpl;
import org.helenus.driver.info.ClassInfo;
import org.helenus.driver.info.FieldInfo;
import org.helenus.driver.junit.util.ReflectionJUnitUtils;
import org.helenus.driver.junit.util.Strings;
import org.junit.rules.MethodRule;
import org.junit.runners.model.FrameworkMethod;
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
public class HelenusJUnit implements MethodRule {
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
  public final static long DEFAULT_STARTUP_TIMEOUT = 15000L;

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
  private static StatementManagerUnitImpl manager = null;

  /**
   * Hold a set of pojo class for which we have loaded the schema definition
   * onto the embedded cassandra database.
   *
   * @author paouelle
   */
  private static final Set<Class<?>> schemas = new HashSet<>();

  /**
   * Holds the test method currently running.
   *
   * @author paouelle
   */
  private static volatile FrameworkMethod method = null;

  /**
   * Holds the test object on which the test method is currently running
   *
   * @author paouelle
   */
  private static volatile Object target = null;

  /**
   * Holds the suffix key values for the current test method keyed by suffix
   * types.
   *
   * @author paouelle
   */
  private static volatile Map<String, Set<String>> suffixKeyValues = null;

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
   * Find the suffix key values from the specified test definition that matches
   * the specified pojo class.
   *
   * @author paouelle
   *
   * @param  cinfo the non-<code>null</code> pojo class information
   * @return a non-<code>null</code> collection of all suffix values keyed by
   *         their names based on the specified class info (each entry in the main
   *         collection provides another collection of all possible values for a
   *         given suffix)
   */
  private static Collection<Collection<Strings>> getSuffixKeyValues(
    ClassInfo<?> cinfo
  ) {
    final Map<String, Set<String>> skvss = HelenusJUnit.suffixKeyValues;

    if (skvss == null) {
      return Collections.emptyList();
    }
    return skvss.entrySet().stream()
      .map(e -> {
        final FieldInfo<?> finfo = cinfo.getSuffixKeyByType(e.getKey());

        if (finfo != null) { // pojo defines this suffix type
          return e.getValue().stream()
            .map(v -> new Strings(finfo.getSuffixKeyName(), v))
            .collect(Collectors.toList());
        }
        return null;
      })
      .filter(p -> p != null)
      .collect(Collectors.toList());
  }

  /**
   * Processes the @BeforeObjects method by calling it and inserting all
   * returned pojo objects into the database.
   *
   * @author paouelle
   *
   * @param batch the non-<code>null</code> batch to insert the objects to create in
   * @param m the non-<code>null</code> method to invoke to get the initial objects
   * @param target the test object for which we are calling the method
   * @param suffixes the map of suffixes to pass to the method
   * @param onlyIfRequiresSuffixes <code>true</code> if the method should
   *        not be called if it doesn't require suffixes
   */
  private static void processBeforeObjects(
    Batch batch,
    Method m,
    Object target, Map<String, String> suffixes,
    boolean onlyIfRequiresSuffixes
  ) {
    try {
      final Class<?>[] cparms = m.getParameterTypes();
      final Object ret;

      // check if the method expects a map of suffixes
      if (cparms.length == 0) { // doesn't care about suffixes
        if (!onlyIfRequiresSuffixes) {
          ret = m.invoke(target);
        } else {
          ret = null;
        }
      } else {
        final Type[] tparms = m.getGenericParameterTypes();

        if ((cparms.length != 1)
            || !Map.class.isAssignableFrom(cparms[0])
            || (tparms.length != 1)
            || !(tparms[0] instanceof ParameterizedType)) {
          throw new AssertionError(
            "expecting one Map<String, String> parameter for @BeforeObjects method "
            + m.getName()
            + "("
            + m.getDeclaringClass().getName()
            + ")"
          );
        }
        final ParameterizedType ptype = (ParameterizedType)tparms[0];

        // maps will always have 2 arguments
        for (final Type atype: ptype.getActualTypeArguments()) {
          final Class<?> aclazz = ReflectionUtils.getRawClass(atype);

          if (String.class != aclazz) {
            throw new AssertionError(
              "expecting one Map<String, String> parameter for @BeforeObjects method "
              + m.getName()
              + "("
              + m.getDeclaringClass().getName()
              + ")"
            );
          }
        }
        ret = m.invoke(target, suffixes);
      }
      if (ret == null) { // nothing to do
        return;
      }
      // validate the return type is either an array, a collection, or a stream
      final Class<?> type = m.getReturnType();

      if (type.isArray()) {
        final int l = Array.getLength(ret);

        for (int i = 0; i < l; i++) {
          batch.add(StatementBuilder.insert(Array.get(ret, i)).intoAll());
        }
      } else if (ret instanceof Collection) {
        ((Collection<?>)ret).forEach(o -> StatementBuilder.insert(0).intoAll());
      } else if (ret instanceof Stream) {
        ((Stream<?>)ret).forEach(o -> StatementBuilder.insert(0).intoAll());
      } else if (ret instanceof Iterator) {
        for (final Iterator<?> i = (Iterator<?>)ret; i.hasNext(); ) {
          batch.add(StatementBuilder.insert(i.next()).intoAll());
        }
      } else if (ret instanceof Enumeration<?>) {
        for (final Enumeration<?> e = (Enumeration<?>)ret; e.hasMoreElements(); ) {
          batch.add(StatementBuilder.insert(e.nextElement()).intoAll());
        }
      } else if (ret instanceof Iterable) {
        for (final Iterator<?> i = ((Iterable<?>)ret).iterator(); i.hasNext(); ) {
          batch.add(StatementBuilder.insert(i.next()).intoAll());
        }
      } else {
        batch.add(StatementBuilder.insert(ret).intoAll());
      }
    } catch (IllegalAccessException e) { // should not happen
      throw new IllegalStateException(e);
    } catch (InvocationTargetException e) {
      final Throwable t = e.getTargetException();

      if (t instanceof Error) {
        throw (Error)t;
      } else if (t instanceof RuntimeException) {
        throw (RuntimeException)t;
      } else { // we don't expect any of those
        throw new IllegalStateException(t);
      }
    }
  }

  /**
   * Processes all methods annotated with @BeforeObjects for the current test
   * target object.
   *
   * @author paouelle
   */
  private static void processBeforeObjects() {
    final Object target = HelenusJUnit.target;

    if (target == null) {
      return;
    }
    final Map<String, Set<String>> suffixeValues = HelenusJUnit.suffixKeyValues;
    final Collection<Collection<Strings>> suffixesByTypes;

    if (suffixeValues != null) {
      suffixesByTypes = suffixeValues.entrySet().stream()
      .map(e -> e.getValue().stream()
        .map(v -> new Strings(e.getKey(), v))
        .collect(Collectors.toList())
      )
      .collect(Collectors.toList());
    } else {
      suffixesByTypes = Collections.emptyList();
    }
    final Set<Method> methods = ReflectionUtils.getAllAnnotationsForMethodsAnnotatedWith(
      target.getClass(), BeforeObjects.class, true
    ).keySet(); // don't care about the @BeforeObjects annotations
    final Batch batch = StatementBuilder.batch();

    if (CollectionUtils.isEmpty(suffixesByTypes)) {
      // no suffixes so call with empty map of suffixes
      methods.forEach(
        m -> HelenusJUnit.processBeforeObjects(
          batch, m, target, Collections.emptyMap(), false
        )
      );
    } else {
      boolean onlyIfRequiresSuffixes = false; // only the first time we call them

      for (final Iterator<List<Strings>> i = new CombinationIterator<>(Strings.class, suffixesByTypes); i.hasNext(); ) {
        final List<Strings> isuffixes = i.next();
        final Map<String, String> suffixes = new HashMap<>(isuffixes.size() * 3 / 2);
        final boolean oirs = onlyIfRequiresSuffixes;

        isuffixes.forEach(ss -> suffixes.put(ss.key,  ss.value));
        methods.forEach(
          m -> HelenusJUnit.processBeforeObjects(
            batch, m, target, suffixes, oirs
          )
        );
        onlyIfRequiresSuffixes = true; // from now on, only call those that requires suffixes
      }
    }
    batch.execute();
  }

  /**
   * Starts the embedded Cassandra daemon and the Helenus statement manager.
   *
   * @author paouelle
   *
   * @param  cfgname the non-<code>null</code> cassandra config resource name
   * @param  timeout the timeout to wait for the Cassandra daemon to start
   *         before failing
   * @throws AssertionError if an I/O error occurs while starting everything
   */
  private static synchronized void start(String cfgname, long timeout) {
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
    try {
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
    } catch (AssertionError|ThreadDeath|StackOverflowError|OutOfMemoryError e) {
      throw e;
    } catch (IOException|Error|RuntimeException e) {
      throw new AssertionError("failed to start Cassandra daemon", e);
    }
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
    final StatementManagerUnitImpl mgr = HelenusJUnit.manager;

    if (mgr != null) {
      // first drop all non-system keyspaces
      for (final KeyspaceMetadata keyspace: HelenusJUnit.manager.getCluster().getMetadata().getKeyspaces()) {
        final String kname = keyspace.getName();

        if (!"system".equals(kname)
            && !"system_auth".equals(kname)
            && !"system_traces".equals(kname)) {
          mgr.getSession().execute("DROP KEYSPACE " + kname);
        }
      }
      // make sure to also clear the pojo class info in order to force the dependencies
      // to other pojos to be re-created when they are referenced
      mgr.clear();
    }
    // finally clear the cache of schemas
    HelenusJUnit.schemas.clear();
  }

  /**
   * Creates the schema for the specified pojo class onto the embedded Cassandra
   * database.
   *
   * @author paouelle
   *
   * @param  clazz the pojo class for which to create the schema
   * @throws NullPointerException if <code>clazz</code> is <code>null</code>
   * @throws AssertionError if a failure occurs while creating the schema
   */
  public static void createSchema(Class<?> clazz) {
    org.apache.commons.lang3.Validate.notNull(clazz, "invalid null class");
    // check whether the schema for this pojo has already been loaded
    synchronized (HelenusJUnit.schemas) {
      if (!HelenusJUnit.schemas.add(clazz)) {
        return; // nothing to do more
      }
      try {
        logger.debug("Creating schema for %s", clazz.getSimpleName());
        final ClassInfo<?> cinfo = manager.getClassInfoImpl(clazz);
        // find all suffixes that are defined for this classes
        final Collection<Collection<Strings>> suffixes = ((target != null)
          ? HelenusJUnit.getSuffixKeyValues(cinfo)
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
      } catch (AssertionError|ThreadDeath|StackOverflowError|OutOfMemoryError e) {
        // make sure to remove cached schema indicator
        HelenusJUnit.schemas.remove(clazz);
        throw e;
      } catch (RuntimeException|Error e) {
        // make sure to remove cached schema indicator
        HelenusJUnit.schemas.remove(clazz);
        throw new AssertionError(
          "failed to create schema for " + clazz.getSimpleName(), e
        );
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
   * <i>Note:</i> Defaults to 15 seconds timeout to wait for the Cassandra
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
   * @param  method the test method to be run
   * @param  target the test object on which the method will be run
   * @throws AssertionError if an I/O error occurs while initializing the cassandra
   *         daemon or the helenus statement manager
   */
  protected void before(FrameworkMethod method, Object target) {
    final Map<String, Set<String>> suffixes = new LinkedHashMap<>(12);

    synchronized (HelenusJUnit.class) {
      // make sure we are not running another test case
      if (HelenusJUnit.method != null) {
        throw logger.throwing(new AssertionError(
          "already running test case "
          + method.getName()
          + "("
          + method.getDeclaringClass().getName()
          + ")"
        ));
      }
      try {
        // start embedded cassandra daemon
        HelenusJUnit.start(cfgname, timeout);
        HelenusJUnit.method = method;
        HelenusJUnit.target = target;
        for (final SuffixKeyValues skvs: ReflectionJUnitUtils.getAnnotationsByType(
              method.getMethod(), SuffixKeyValues.class
            )) {
          suffixes.compute(
            skvs.type(), (t, s) -> {
              if (s == null) {
                s = new LinkedHashSet<>(Math.max(1, skvs.values().length) * 3 / 2);
              }
              for (final String v: skvs.values()) {
                s.add(v);
              }
              return s;
            }
          );
        }
        HelenusJUnit.suffixKeyValues = suffixes;
        // finally cleanup the database for this new test
        HelenusJUnit.clear();
      } catch (AssertionError|ThreadDeath|StackOverflowError|OutOfMemoryError e) {
        // make sure to cleanup
        HelenusJUnit.method = null;
        HelenusJUnit.target = null;
        throw e;
      } catch (RuntimeException|Error e) {
        // make sure to cleanup
        HelenusJUnit.method = null;
        HelenusJUnit.target = null;
        throw new AssertionError("failed to start Cassandra daemon", e);
      }
      try {
        // Process all @BeforeObjects methods found
        HelenusJUnit.processBeforeObjects();
      } catch (AssertionError|ThreadDeath|StackOverflowError|OutOfMemoryError e) {
        // make sure to cleanup
        HelenusJUnit.method = null;
        HelenusJUnit.target = null;
        throw e;
      } catch (RuntimeException|Error e) {
        // make sure to cleanup
        HelenusJUnit.method = null;
        HelenusJUnit.target = null;
        throw new AssertionError("failed to install @BeforeObjects objects into Cassandra", e);
      }
    }
  }

  /**
   * Called to tear down the Helenus/Cassandra environment.
   *
   * @author paouelle
   *
   * @param method the test method to be run
   * @param target the test object on which the method will be run
   */
  protected void after(FrameworkMethod method, Object target) {
    // clear the current test description
    synchronized (HelenusJUnit.class) {
      if ((HelenusJUnit.method == method)
          && (HelenusJUnit.target == target)) { // should always be true
        HelenusJUnit.method = null;
        HelenusJUnit.target = null;
      }
    }
  }

  /**
   * {@inheritDoc}
   *
   * @author paouelle
   *
   * @see org.junit.rules.MethodRule#apply(org.junit.runners.model.Statement, org.junit.runners.model.FrameworkMethod, java.lang.Object)
   */
  @Override
  public Statement apply(Statement base, FrameworkMethod method, Object target) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        before(method, target);
        try {
          base.evaluate();
        } finally {
          after(method, target);
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
     * Clears the cache of all pojo class infos.
     *
     * @author paouelle
     */
    void clear() {
      super.clearCache();
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


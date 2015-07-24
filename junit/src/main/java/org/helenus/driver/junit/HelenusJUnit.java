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
import java.lang.reflect.Field;
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

import java.util.ArrayList;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import org.apache.logging.log4j.Level;
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
import org.helenus.driver.GenericStatement;
import org.helenus.driver.Group;
import org.helenus.driver.Insert;
import org.helenus.driver.Sequence;
import org.helenus.driver.StatementBuilder;
import org.helenus.driver.StatementManager;
import org.helenus.driver.Truncate;
import org.helenus.driver.impl.ClassInfoImpl;
import org.helenus.driver.impl.RootClassInfoImpl;
import org.helenus.driver.impl.StatementImpl;
import org.helenus.driver.impl.StatementManagerImpl;
import org.helenus.driver.impl.TypeClassInfoImpl;
import org.helenus.driver.impl.UDTClassInfoImpl;
import org.helenus.driver.info.ClassInfo;
import org.helenus.driver.info.FieldInfo;
import org.helenus.driver.info.RootClassInfo;
import org.helenus.driver.info.TypeClassInfo;
import org.helenus.driver.junit.util.ReflectionJUnitUtils;
import org.helenus.driver.junit.util.Strings;
import org.junit.rules.MethodRule;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;
import org.yaml.snakeyaml.reader.UnicodeReader;

/**
 * The <code>HelenusJUnit</code> class provides the JUnit 4 definition for a
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
  public final static long DEFAULT_STARTUP_TIMEOUT = 60000L;

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
  private final static String RUNTIME_DIR = "target/helenus-junit";

  /**
   * Holds the regex pattern used to parse the Cassandra yaml configuration
   * file in search of ports.
   *
   * @author paouelle
   */
  private static Pattern portPattern = Pattern.compile(
    "^([a-z_]+_port:\\s*)([0-9]+)\\s*$", Pattern.MULTILINE
  );

  /**
   * Holds the regex pattern used to parse the Cassandra yaml configuration
   * file in search of target directories.
   *
   * @author paouelle
   */
  private static Pattern targetPattern = Pattern.compile(
    "^([a-z_]+:[\\s-]*)(target/helenus-junit)(.*)$", Pattern.MULTILINE
  );

  /**
   * Holds the thread group used to combine all Cassandra daemon threads
   * together.
   *
   * @author paouelle
   */
  private static volatile ThreadGroup group = null;

  /**
   * Holds the Cassandra config file name used to start the Cassandra daemon.
   *
   * @author paouelle
   */
  private static String config = null;

  /**
   * Holds the optional fork number which can be used to run multiple JVMs in
   * parallel to test. When specified, the directory used to store Cassandra's
   * files will have the forkNumber appended to it.
   * <p>
   * This can be used with the maven-surefire-plugin when using a fork count
   * greater than 1 by setting the system property "fork" with
   * "${surefire.forkNumber}".
   * <p>
   * Maven Example:
   *   &lt;argLine&gt;-Dfork=${surefire.forkNumber}&lt;/argLine&gt;
   *
   * @author paouelle
   */
  private static String fork = System.getProperty("fork", "");

  /**
   * Holds the Cassandra daemon when started.
   *
   * @author paouelle
   */
  private static volatile CassandraDaemon daemon = null;

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
  static final Set<Class<?>> schemas = new HashSet<>();

  /**
   * Holds a cache of class info structures created by previous test cases.
   *
   * @author paouelle
   */
  static final Map<Class<?>, ClassInfoImpl<?>> fromPreviousTestsCacheInfoCache
    = new LinkedHashMap<>(64);

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
   * Holds the capture lists.
   *
   * @author paouelle
   */
  @SuppressWarnings("rawtypes")
  static final List<StatementCaptureList<? extends GenericStatement>> captures
    = new ArrayList<>(4);

  /**
   * Holds a flag indicating if capturing is enabled or not.
   *
   * @author paouelle
   */
  static boolean capturing = false;

  /**
   * Gets the caller's information. This would be the information about the
   * method that called this class first.
   *
   * @author paouelle
   *
   * @return the corresponding non-<code>null</code> stack trace element
   * @throws IllegalStateException if no outside method called this class
   */
  private static StackTraceElement getCallerInfo() {
    final Exception e = new Exception();

    for (final StackTraceElement ste: e.getStackTrace()) {
      if (!HelenusJUnit.class.getName().equals(ste.getClassName())) {
        return ste;
      }
    }
    throw new IllegalStateException("missing caller's information");
  }

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
    final Matcher pm = HelenusJUnit.portPattern.matcher(yaml);
    boolean updated = false; // until proven otherwise

    while (pm.find()) {
      final String pname = pm.group(1);
      final int port = Integer.parseInt(pm.group(2));

      if (port == 0) {
        try (
          final ServerSocket socket = new ServerSocket(0);
        ) {
          pm.appendReplacement(sb, pname + socket.getLocalPort());
          updated = true;
          logger.info("Allocated free %s%d", pname, socket.getLocalPort());
        }
      } else {
        pm.appendReplacement(sb, pm.group());
      }
    }
    pm.appendTail(sb);
    if (!StringUtils.isEmpty(HelenusJUnit.fork)) {
      final Matcher tm = HelenusJUnit.targetPattern.matcher(sb.toString());

      sb.setLength(0);
      while (tm.find()) {
        final String pname = tm.group(1);
        final String tname = tm.group(2);
        final String rest = tm.group(3);

        tm.appendReplacement(sb, pname + tname + '/' + HelenusJUnit.fork + rest);
        updated = true;
      }
      tm.appendTail(sb);
    }
    if (updated) { // update the config file with the updated ports and/or target dirs
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
   * Processes the {@link BeforeObjects} annotated methods by calling it and
   * inserting all returned pojo objects into the database.
   *
   * @author paouelle
   *
   * @param batch the non-<code>null</code> batch to insert the objects to create in
   * @param m the non-<code>null</code> method to invoke to get the initial objects
   * @param bo the non-<code>null</code> annotation for the method
   * @param target the test object for which we are calling the method
   * @param method the test method
   * @param suffixes the map of suffixes to pass to the method
   * @param onlyIfRequiresSuffixes <code>true</code> if the method should
   *        not be called if it doesn't require suffixes
   */
  private static void processBeforeObjects(
    Batch batch,
    Method m,
    BeforeObjects bo,
    Object target,
    FrameworkMethod method,
    Map<String, String> suffixes,
    boolean onlyIfRequiresSuffixes
  ) {
    if (!ArrayUtils.isEmpty(bo.value())
        && !ArrayUtils.contains(bo.value(), method.getName())) {
      return;
    }
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
      HelenusJUnit.processObjects(batch, ret);
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
   * Processes the returned object as a {@link Collection}, {@link Stream},
   * {@link Iterator}, {@link Enumeration}, {@link Iterable}, or a single object
   * and insert the object(s) in the specified batch.
   *
   * @author paouelle
   *
   * @param batch the non-<code>null</code> batch to insert the objects to create in
   * @param ret the return object to process
   */
  @SuppressWarnings("unchecked")
  private static void processObjects(Batch batch, Object ret) {
    if (ret == null) { // nothing to do
      return;
    }
    // validate the return type is either an array, a collection, or a stream
    final Class<?> type = ret.getClass();

    if (type.isArray()) {
      final int l = Array.getLength(ret);

      for (int i = 0; i < l; i++) {
        batch.add(StatementBuilder.insert(Array.get(ret, i)).intoAll());
      }
    } else if (ret instanceof Collection) {
      ((Collection<Object>)ret).forEach(o -> StatementBuilder.insert(o).intoAll());
    } else if (ret instanceof Stream) {
      ((Stream<Object>)ret).forEach(o -> StatementBuilder.insert(o).intoAll());
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
  }

  /**
   * Processes all methods annotated with @BeforeObjects for the current test
   * target object.
   *
   * @author paouelle
   */
  private static void processBeforeObjects() {
    final Object target = HelenusJUnit.target;
    final FrameworkMethod method = HelenusJUnit.method;

    if ((target == null) || (method == null)) {
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
    final Map<Method, BeforeObjects[]> methods = ReflectionUtils.getAllAnnotationsForMethodsAnnotatedWith(
      target.getClass(), BeforeObjects.class, true
    );
    final Batch batch = StatementBuilder.batch();

    if (CollectionUtils.isEmpty(suffixesByTypes)) {
      // no suffixes so call with empty map of suffixes
      methods.forEach(
        (m, bos) -> HelenusJUnit.processBeforeObjects(
          batch, m, bos[0], target, method, Collections.emptyMap(), false // BeforeObjects is not repeatable so only 1 in array
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
          (m, bos) -> HelenusJUnit.processBeforeObjects(
            batch, m, bos[0], target, method, suffixes, oirs // BeforeObjects is not repeatable so only 1 in array
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
   * @throws AssertionError if an error occurs while starting everything
   */
  private static synchronized void start0(String cfgname, long timeout) {
    if (HelenusJUnit.config != null) {
      // check if we are starting it with the same config
      if (HelenusJUnit.config.equals(cfgname)) {
        // logger.debug("Helenus already started using configuration '%s'", cfgname);
        return;
      }
      throw new AssertionError(
        "Helenus cannot be started again with a different configuration"
      );
    }
    try {
      HelenusJUnit.config = cfgname;
      final File dir;

      if (StringUtils.isEmpty(HelenusJUnit.fork)) {
        logger.info("Starting Helenus...");
        dir = new File(HelenusJUnit.RUNTIME_DIR);
      } else {
        logger.info("Starting Helenus fork #%s...", HelenusJUnit.fork);
        dir = new File(HelenusJUnit.RUNTIME_DIR + File.separatorChar + HelenusJUnit.fork);
      }
      // make sure the config resource is absolute
      cfgname = StringUtils.prependIfMissing(cfgname, "/");
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
      // and to update the runtime directory if forking
      HelenusJUnit.update(cfgfile);
      // update system properties for cassandra
      System.setProperty("cassandra.config", "file:" + cfgfile.getAbsolutePath());
      System.setProperty("cassandra-foreground", "true");
      System.setProperty("cassandra.native.epoll.enabled", "false"); // JNA doesn't cope with relocated netty
      // create a thread group for all Cassandra threads to be able to detect them
      HelenusJUnit.group = new ThreadGroup("Cassandra Daemon Group");
      // startup the cassandra daemon
      final CountDownLatch latch = new CountDownLatch(1);
      final AtomicReference<Throwable> failed = new AtomicReference<>(); // until proven otherwise
      final Thread thread = new Thread(
        HelenusJUnit.group,
        new Runnable() {
          @SuppressWarnings("synthetic-access")
          @Override
          public void run() {
            try {
              // make sure the Cassandra runtime directories exists and are cleaned up
              HelenusJUnit.cleanupAndCreateCassandraDirectories();
              HelenusJUnit.daemon = new CassandraDaemon();
              HelenusJUnit.daemon.activate();
            } catch (AssertionError|StackOverflowError|OutOfMemoryError|ThreadDeath e) {
              HelenusJUnit.daemon = null;
              throw e;
            } catch (RuntimeException e) {
              HelenusJUnit.daemon = null;
              failed.set(e);
              throw e;
            } catch (Error e) {
              HelenusJUnit.daemon = null;
              failed.set(e);
              throw e;
            } finally {
              latch.countDown();
            }
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
        final Throwable t = failed.get();

        if (t != null) {
          logger.error("Cassandra daemon failed to start; %s", t);
        }
      } catch (InterruptedException e) {
        logger.error("interrupted waiting for cassandra daemon to start", e);
        throw new AssertionError(e);
      } finally {
        thread.interrupt();
      }
      final String host = DatabaseDescriptor.getRpcAddress().getHostName();
      final int port = DatabaseDescriptor.getNativeTransportPort();

      logger.info("Cassandra started on '%s:%d'", host, port);
      // finally initializes helenus
      try {
        HelenusJUnit.manager = new StatementManagerUnitImpl(
          Cluster
            .builder()
            .withPort(port)
            .addContactPoint(host)
            .withQueryOptions(null)
        );
      } catch (SecurityException e) {
        // this shouldn't happen unless someone mocked the StatementManager class
        // or registered a manager of their own
        logger.error("Failed to install Helenus statement manager as one is already registered (maybe through mocking)");
        // use reflection to dump info about it
        try {
          final Field f = StatementManager.class.getDeclaredField("manager");

          f.setAccessible(true);
          final Object mgr = f.get(null);

          logger.error("*** manager: %s", mgr);
          Class<?> c = mgr.getClass();

          while (c != null) {
            logger.debug("*** class: %s", c);
            c = c.getSuperclass();
          }
          for (final Class<?> ci: mgr.getClass().getInterfaces()) {
            logger.debug("*** interface: %s", ci);
          }
          for (final Field cf: mgr.getClass().getDeclaredFields()) {
            cf.setAccessible(true);
            logger.debug("*** field(%s): %s", cf.getName(), cf.get(mgr));
          }
        } catch (Throwable t) {} // ignore
        throw new AssertionError("failed to install Helenus statement manager", e);
      }
    } catch(ThreadDeath|StackOverflowError|OutOfMemoryError e) {
      HelenusJUnit.config = null;
      HelenusJUnit.group = null;
      HelenusJUnit.daemon = null;
      HelenusJUnit.manager = null;
      throw e;
    } catch (AssertionError e) {
      HelenusJUnit.config = null;
      HelenusJUnit.group = null;
      HelenusJUnit.manager = null;
      if (HelenusJUnit.daemon != null) {
        try {
          HelenusJUnit.daemon.deactivate();
        } catch (ThreadDeath|StackOverflowError|OutOfMemoryError ee) {
          throw ee;
        } catch (Throwable tt) { // ignore
        } finally {
          HelenusJUnit.daemon = null;
        }
      }
      throw e;
    } catch (Throwable t) {
      HelenusJUnit.config = null;
      HelenusJUnit.group = null;
      HelenusJUnit.manager = null;
      if (HelenusJUnit.daemon != null) {
        try {
          HelenusJUnit.daemon.deactivate();
        } catch (ThreadDeath|StackOverflowError|OutOfMemoryError ee) {
          throw ee;
        } catch (Throwable tt) { // ignore
        } finally {
          HelenusJUnit.daemon = null;
        }
      }
      throw new AssertionError("failed to start Cassandra daemon", t);
    }
  }

  /**
   * Clears the schema for the specified class info.
   *
   * @author paouelle
   *
   * @param <T> the type of pojo being cleared
   *
   * @param  cinfo the class info to clear the schema
   * @param  seq the sequence to record all statements
   * @throws AssertionError if a failure occurs while clearing the schema
   */
  private static <T> void clearSchema0(ClassInfoImpl<T> cinfo, Sequence seq) {
    if (cinfo instanceof UDTClassInfoImpl) { // nothing to reset but we want the above trace
      return;
    }
    final boolean old = HelenusJUnit.capturing;

    try {
      HelenusJUnit.capturing = false; // disable temporarily capturing
      // find all suffixes that are defined for this classes
      final Collection<Collection<Strings>> suffixes = ((target != null)
        ? HelenusJUnit.getSuffixKeyValues(cinfo)
        : null
      );
      // since we already created the schema, we should have the right number of suffixes
      // now generate as many insert statements for each initial object as
      // required by the combination of all suffix values
      if (CollectionUtils.isEmpty(suffixes)) {
        // no suffixes so just the one truncate
        final Truncate<T> truncate = StatementBuilder.truncate(cinfo.getObjectClass());

        truncate.disableTracing();
        seq.add(truncate);
      } else {
        for (final Iterator<List<Strings>> i = new CombinationIterator<>(Strings.class, suffixes); i.hasNext(); ) {
          final Truncate<T> truncate = StatementBuilder.truncate(cinfo.getObjectClass());

          truncate.disableTracing();
          // pass all required suffixes
          for (final Strings ss: i.next()) {
            // register the suffix value with the corresponding suffix name
            truncate.where(StatementBuilder.eq(ss.key, ss.value));
          }
          seq.add(truncate);
        }
      }
    } catch (AssertionError|ThreadDeath|StackOverflowError|OutOfMemoryError e) {
      throw e;
    } catch (Throwable t) {
      throw new AssertionError(
        "failed to clear schema for "
        + cinfo.getObjectClass().getSimpleName(),
        t
      );
    } finally {
      HelenusJUnit.capturing = old; // restore previous capturing setting
    }
  }

  /**
   * Clears the database and the Helenus driver by completely resetting them to
   * the same state they was before the previous test case.
   *
   * @author paouelle
   *
   * @throws AssertionError if a failure occurs while cleanup
   */
  private static synchronized void fullClear0() {
    synchronized (HelenusJUnit.schemas) {
      final StatementManagerUnitImpl mgr = HelenusJUnit.manager;

      if (mgr != null) {
        try {
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
          mgr.clearCache();
        } catch (AssertionError|ThreadDeath|StackOverflowError|OutOfMemoryError e) {
          throw e;
        } catch (Throwable t) {
          throw new AssertionError("failed to clean Cassandra database", t);
        }
      }
      // clear the cache of schemas
      HelenusJUnit.schemas.clear();
      // clear anything we remembered from the previous test cases
      HelenusJUnit.fromPreviousTestsCacheInfoCache.clear();
    }
  }

  /**
   * Clears the database an the Helenus driver by partially resetting them to a
   * similar state they were before the previous test case.
   *
   * @author paouelle
   *
   * @throws AssertionError if a failure occurs while cleanup
   */
  private static synchronized void clear0() {
    synchronized (HelenusJUnit.schemas) {
      final StatementManagerUnitImpl mgr = HelenusJUnit.manager;

      if (mgr != null) {
        try {
          // start by truncating all loaded pojo tables
          final Sequence seq = StatementBuilder.sequence();

          seq.disableTracing();
          mgr.classInfoImpls()
            .filter(c -> !(c instanceof TypeClassInfo))
            .collect(Collectors.toSet()) // force a snapshot
            .forEach(c -> HelenusJUnit.clearSchema0(c, seq));
          seq.execute();
          // next preserve all class infos already cached
          mgr.classInfoImpls()
            .forEach(c -> HelenusJUnit.fromPreviousTestsCacheInfoCache.put(c.getObjectClass(), c));
          // make sure to also clear the pojo class info in order to force the dependencies
          // to other pojos to be re-created when they are referenced
          mgr.clearCache();
        } catch (AssertionError|ThreadDeath|StackOverflowError|OutOfMemoryError e) {
          throw e;
        } catch (Throwable t) {
          throw new AssertionError("failed to clean Cassandra database", t);
        }
      }
      // clear the cache of schemas
      HelenusJUnit.schemas.clear();
    }
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
  static void createSchema0(Class<?> clazz) {
    org.apache.commons.lang3.Validate.notNull(clazz, "invalid null class");
    // check whether the schema for this pojo has already been loaded
    synchronized (HelenusJUnit.schemas) {
      if (!HelenusJUnit.schemas.add(clazz)) {
        return; // nothing to do more
      }
      final boolean old = HelenusJUnit.capturing;

      try {
        final ClassInfo<?> cinfo = manager.getClassInfoImpl(clazz);

        if (cinfo instanceof TypeClassInfo) {
          // nothing to do for those since everything is done through the root
          return;
        } else if (cinfo instanceof RootClassInfo) {
          logger.debug(
            "Creating schema for %s, %s",
            clazz.getSimpleName(),
            ((RootClassInfo<?>)cinfo).types()
              .map(t -> t.getObjectClass().getSimpleName())
              .collect(Collectors.joining(", "))
          );
        } else {
          logger.debug("Creating schema for %s", clazz.getSimpleName());
        }
        HelenusJUnit.capturing = false; // disable temporarily capturing
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

          cs.disableTracing();
          cs.ifNotExists();
          cs.execute();
        } else {
          final Sequence s = StatementBuilder.sequence();

          s.disableTracing();
          for (final Iterator<List<Strings>> i = new CombinationIterator<>(Strings.class, suffixes); i.hasNext(); ) {
            final CreateSchema<?> cs = StatementBuilder.createSchema(clazz);

            cs.disableTracing();
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
      } catch (Throwable t) {
        // make sure to remove cached schema indicator
        HelenusJUnit.schemas.remove(clazz);
        throw new AssertionError(
          "failed to create schema for " + clazz.getSimpleName(), t
        );
      } finally {
        HelenusJUnit.capturing = old; // restore previous capturing setting
      }
    }
  }

  /**
   * Resets the schema for the specified class info.
   *
   * @author paouelle
   *
   * @param <T> the type of pojo being reset
   *
   * @param  cinfo the class info to reset the schema
   * @throws AssertionError if a failure occurs while reseting the schema
   */
  static <T> void resetSchema0(ClassInfoImpl<T> cinfo) {
    synchronized (HelenusJUnit.schemas) {
      HelenusJUnit.schemas.add(cinfo.getObjectClass());
      if (cinfo instanceof RootClassInfo) {
        logger.debug(
          "Resetting schema for %s, %s",
          cinfo.getObjectClass().getSimpleName(),
          ((RootClassInfoImpl<T>)cinfo).types()
            .map(t -> t.getObjectClass().getSimpleName())
            .collect(Collectors.joining(", "))
        );
      } else {
        logger.debug("Resetting schema for %s", cinfo.getObjectClass().getSimpleName());
      }
      if ((cinfo instanceof TypeClassInfoImpl) || (cinfo instanceof UDTClassInfoImpl)) {
        // nothing to reset but we want the above trace
        return;
      }
      final boolean old = HelenusJUnit.capturing;

      try {
        HelenusJUnit.capturing = false; // disable temporarily capturing
        // find all suffixes that are defined for this classes
        final Collection<Collection<Strings>> suffixes = ((target != null)
          ? HelenusJUnit.getSuffixKeyValues(cinfo)
          : null
        );
        final Batch batch = StatementBuilder.batch();

        batch.disableTracing();
        // since we already created the schema, we should have the right number of suffixes
        // now generate as many insert statements for each initial object as
        // required by the combination of all suffix values
        if (CollectionUtils.isEmpty(suffixes)) {
          for (final T io: cinfo.newContext().getInitialObjects()) {
            final Insert<T> insert = StatementBuilder.insert(io).intoAll();

            insert.disableTracing();
            batch.add(insert);
          }
        } else {
          for (final Iterator<List<Strings>> i = new CombinationIterator<>(Strings.class, suffixes); i.hasNext(); ) {
            final ClassInfoImpl<T>.Context context = cinfo.newContext();

            // pass all required suffixes
            for (final Strings ss: i.next()) {
              context.addSuffix(ss.key, ss.value);
            }
            for (final T io: context.getInitialObjects()) {
              final Insert<T> insert = StatementBuilder.insert(io).intoAll();

              insert.disableTracing();
              batch.add(insert);
            }
          }
        }
        batch.execute();
      } catch (AssertionError|ThreadDeath|StackOverflowError|OutOfMemoryError e) {
        // make sure to remove cached schema indicator
        HelenusJUnit.schemas.remove(cinfo.getObjectClass());
        throw e;
      } catch (Throwable t) {
        // make sure to remove cached schema indicator
        HelenusJUnit.schemas.remove(cinfo.getObjectClass());
        throw new AssertionError(
          "failed to reset schema for "
          + cinfo.getObjectClass().getSimpleName(),
          t
        );
      } finally {
        HelenusJUnit.capturing = old; // restore previous capturing setting
      }
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
   * <i>Note:</i> Defaults to 60 seconds timeout to wait for the Cassandra
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
   * @throws AssertionError if an error occurs while initializing the cassandra
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
        HelenusJUnit.capturing = false;
        // start embedded cassandra daemon
        HelenusJUnit.start0(cfgname, timeout);
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
        HelenusJUnit.clear0();
      } catch (AssertionError|ThreadDeath|StackOverflowError|OutOfMemoryError e) {
        // make sure to cleanup
        HelenusJUnit.method = null;
        HelenusJUnit.target = null;
        throw e;
      } catch (Throwable t) {
        // make sure to cleanup
        HelenusJUnit.method = null;
        HelenusJUnit.target = null;
        throw new AssertionError("failed to start Cassandra daemon", t);
      } finally {
        HelenusJUnit.captures.clear();
      }
      try {
        // Process all @BeforeObjects methods found
        HelenusJUnit.processBeforeObjects();
      } catch (AssertionError|ThreadDeath|StackOverflowError|OutOfMemoryError e) {
        // make sure to cleanup
        HelenusJUnit.method = null;
        HelenusJUnit.target = null;
        throw e;
      } catch (Throwable t) {
        // make sure to cleanup
        HelenusJUnit.method = null;
        HelenusJUnit.target = null;
        throw new AssertionError("failed to install @BeforeObjects objects into Cassandra", t);
      } finally {
        HelenusJUnit.captures.clear();
      }
      HelenusJUnit.capturing = true;
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
        HelenusJUnit.capturing = false;
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
   * Clears the database and the Helenus layer by resetting them to the same
   * state they were before the previous test case.
   *
   * @author paouelle
   *
   * @return this for chaining
   * @throws AssertionError if a failure occurs while cleanup
   */
  public HelenusJUnit clear() {
    logger.debug("Clearing all schemas");
    HelenusJUnit.fullClear0();
    return this;
  }

  /**
   * Creates the schema for the specified pojo class onto the embedded Cassandra
   * database.
   *
   * @author paouelle
   *
   * @param  clazz the pojo class for which to create the schema
   * @throws NullPointerException if <code>clazz</code> is <code>null</code>
   * @return this for chaining
   * @throws AssertionError if a failure occurs while creating the schema
   */
  public HelenusJUnit createSchema(Class<?> clazz) {
    HelenusJUnit.createSchema0(clazz);
    return this;
  }

  /**
   * Populates the database with objects returned by the specified supplier.
   * <p>
   * <i>Note:</i> The supplier can return an array, a {@link Collection},
   * an {@link Iterable}, an {@link Iterator}, an {@link Enumeration}, or a
   * {@link Stream} of pojo objects or a single object to insert in the database.
   *
   * @author paouelle
   *
   * @param  objs the supplier of objects to populate the database with
   * @return this for chaining
   * @throws AssertionError if any error occurs
   */
  public HelenusJUnit populate(Supplier<? super Object> objs) {
    if (objs == null) {
      return this;
    }
    synchronized (HelenusJUnit.class) {
      final boolean old = HelenusJUnit.capturing;

      try {
        HelenusJUnit.capturing = false; // disable temporarily capturing
        final Batch batch = StatementBuilder.batch();

        HelenusJUnit.processObjects(batch, objs.get());
        batch.execute();
      } catch (AssertionError|ThreadDeath|StackOverflowError|OutOfMemoryError e) {
        throw e;
      } catch (Throwable t) {
        throw new AssertionError("failed to populate objects", t);
      } finally {
        HelenusJUnit.capturing = old; // restore previous capturing setting
      }
    }
    return this;
  }

  /**
   * Populates the database with objects returned by the specified function. This
   * version of the <code>populate()</code> allows one to receive a map of all
   * suffix key values defined in the test environment using the
   * {@link SuffixKeyValues} annotations. As such, the function might be called
   * multiple times with each combination of suffix key values.
   * <p>
   * <i>Note:</i> The function can return an array, a {@link Collection},
   * an {@link Iterable}, an {@link Iterator}, an {@link Enumeration}, or a
   * {@link Stream} of pojo objects or a single object to insert in the database.
   *
   * @author paouelle
   *
   * @param  objs the function to receive a map of suffix key values and return
   *         objects to populate the database with
   * @return this for chaining
   * @throws AssertionError if any error occurs
   */
  public HelenusJUnit populate(Function<Map<String, String>, ? super Object> objs) {
    if (objs == null) {
      return this;
    }
    synchronized (HelenusJUnit.class) {
      final boolean old = HelenusJUnit.capturing;

      try {
        HelenusJUnit.capturing = false; // disable temporarily capturing
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
        final Batch batch = StatementBuilder.batch();

        if (CollectionUtils.isEmpty(suffixesByTypes)) {
          // no suffixes so call with empty map of suffixes
          HelenusJUnit.processObjects(batch, objs.apply(Collections.emptyMap()));
        } else {
          for (final Iterator<List<Strings>> i = new CombinationIterator<>(Strings.class, suffixesByTypes); i.hasNext(); ) {
            final List<Strings> isuffixes = i.next();
            final Map<String, String> suffixes = new HashMap<>(isuffixes.size() * 3 / 2);

            isuffixes.forEach(ss -> suffixes.put(ss.key,  ss.value));
            HelenusJUnit.processObjects(batch, objs.apply(suffixes));
          }
        }
        batch.execute();
      } catch (AssertionError|ThreadDeath|StackOverflowError|OutOfMemoryError e) {
        throw e;
      } catch (Throwable t) {
        throw new AssertionError("failed to populate objects", t);
      } finally {
        HelenusJUnit.capturing = old; // restore previous capturing setting
      }
    }
    return this;
  }

  /**
   * Populates the database with the specified objects.
   *
   * @author paouelle
   *
   * @param  objs the objects to populate the database with
   * @return this for chaining
   * @throws AssertionError if any error occurs
   */
  public HelenusJUnit populate(Object... objs) {
    if (objs == null) {
      return this;
    }
    return populate(Stream.of(objs));
  }

  /**
   * Populates the database with the specified objects.
   *
   * @author paouelle
   *
   * @param  objs the objects to populate the database with
   * @return this for chaining
   * @throws AssertionError if any error occurs
   */
  public HelenusJUnit populate(Iterable<? super Object> objs) {
    if (objs == null) {
      return this;
    }
    return populate(objs.iterator());
  }

  /**
   * Populates the database with the specified objects.
   *
   * @author paouelle
   *
   * @param  objs the objects to populate the database with
   * @return this for chaining
   * @throws AssertionError if any error occurs
   */
  public HelenusJUnit populate(Iterator<? super Object> objs) {
    if (objs == null) {
      return this;
    }
    synchronized (HelenusJUnit.class) {
      final boolean old = HelenusJUnit.capturing;

      try {
        HelenusJUnit.capturing = false; // disable temporarily capturing
        final Batch batch = StatementBuilder.batch();

        while (objs.hasNext()) {
          batch.add(StatementBuilder.insert(objs.next()).intoAll());
        }
        batch.execute();
      } catch (AssertionError|ThreadDeath|StackOverflowError|OutOfMemoryError e) {
        throw e;
      } catch (Throwable t) {
        throw new AssertionError("failed to populate objects", t);
      } finally {
        HelenusJUnit.capturing = old; // restore previous capturing setting
      }
    }
    return this;
  }

  /**
   * Populates the database with the specified objects.
   *
   * @author paouelle
   *
   * @param  objs the objects to populate the database with
   * @return this for chaining
   * @throws AssertionError if any error occurs
   */
  public HelenusJUnit populate(Stream<? super Object> objs) {
    if (objs == null) {
      return this;
    }
    synchronized (HelenusJUnit.class) {
      final boolean old = HelenusJUnit.capturing;

      try {
        HelenusJUnit.capturing = false; // disable temporarily capturing
        final Batch batch = StatementBuilder.batch();

        objs.forEachOrdered(o -> batch.add(StatementBuilder.insert(o).intoAll()));
        batch.execute();
      } catch (AssertionError|ThreadDeath|StackOverflowError|OutOfMemoryError e) {
        throw e;
      } catch (Throwable t) {
        throw new AssertionError("failed to populate objects", t);
      } finally {
        HelenusJUnit.capturing = old; // restore previous capturing setting
      }
    }
    return this;
  }

  /**
   * Truncates all tables for the specified pojo classes from the database.
   *
   * @author paouelle
   *
   * @param  classes the pojo classes for which to truncate all the tables
   *         content
   * @return this for chaining
   * @throws AssertionError if any error occurs
   */
  public HelenusJUnit truncate(Class<?>... classes) {
    if (classes == null) {
      return this;
    }
    return truncate(Stream.of(classes));
  }

  /**
   * Truncates all tables for the specified pojo classes from the database.
   *
   * @author paouelle
   *
   * @param  classes the pojo classes for which to truncate all the tables
   *         content
   * @return this for chaining
   * @throws AssertionError if any error occurs
   */
  public HelenusJUnit truncate(Iterable<Class<?>> classes) {
    if (classes == null) {
      return this;
    }
    return populate(classes.iterator());
  }

  /**
   * Truncates all tables for the specified pojo classes from the database.
   *
   * @author paouelle
   *
   * @param  classes the pojo classes for which to truncate all the tables
   *         content
   * @return this for chaining
   * @throws AssertionError if any error occurs
   */
  public HelenusJUnit truncate(Iterator<Class<?>> classes) {
    if (classes == null) {
      return this;
    }
    synchronized (HelenusJUnit.class) {
      final boolean old = HelenusJUnit.capturing;

      try {
        HelenusJUnit.capturing = false; // disable temporarily capturing
        final Sequence sequence = StatementBuilder.sequence();

        while (classes.hasNext()) {
          sequence.add(StatementBuilder.truncate(classes.next()));
        }
        sequence.execute();
      } catch (AssertionError|ThreadDeath|StackOverflowError|OutOfMemoryError e) {
        throw e;
      } catch (Throwable t) {
        throw new AssertionError("failed to truncate classes", t);
      } finally {
        HelenusJUnit.capturing = old; // restore previous capturing setting
      }
    }
    return this;
  }

  /**
   * Truncates all tables for the specified pojo classes from the database.
   *
   * @author paouelle
   *
   * @param  classes the pojo classes for which to truncate all the tables
   *         content
   * @return this for chaining
   * @throws AssertionError if any error occurs
   */
  public HelenusJUnit truncate(Stream<Class<?>> classes) {
    if (classes == null) {
      return this;
    }
    synchronized (HelenusJUnit.class) {
      final boolean old = HelenusJUnit.capturing;

      try {
        HelenusJUnit.capturing = false; // disable temporarily capturing
        final Sequence sequence = StatementBuilder.sequence();

        classes.forEach(c -> {
          final ClassInfo<?> cinfo = StatementBuilder.getClassInfo(c);
          // find all suffixes that are defined for this classes
          final Collection<Collection<Strings>> suffixes = ((target != null)
            ? HelenusJUnit.getSuffixKeyValues(cinfo)
            : null
          );
          // since we already created the schema, we should have the right number of suffixes
          // now generate as many insert statements for each initial object as
          // required by the combination of all suffix values
          if (CollectionUtils.isEmpty(suffixes)) {
            // no suffixes so just the one truncate
            final Truncate<?> truncate = StatementBuilder.truncate(c);

            truncate.disableTracing();
            sequence.add(truncate);
          } else {
            for (final Iterator<List<Strings>> i = new CombinationIterator<>(Strings.class, suffixes); i.hasNext(); ) {
              final Truncate<?> truncate = StatementBuilder.truncate(c);

              truncate.disableTracing();
              // pass all required suffixes
              for (final Strings ss: i.next()) {
                // register the suffix value with the corresponding suffix name
                truncate.where(StatementBuilder.eq(ss.key, ss.value));
              }
              sequence.add(truncate);
            }
          }
        });
        sequence.execute();
      } catch (AssertionError|ThreadDeath|StackOverflowError|OutOfMemoryError e) {
        throw e;
      } catch (Throwable t) {
        throw new AssertionError("failed to truncate classes", t);
      } finally {
        HelenusJUnit.capturing = old; // restore previous capturing setting
      }
    }
    return this;
  }

  /**
   * Starts capturing all object statements that have had their executions
   * requested with the Helenus statement manager in the order they are occurring
   * to the returned list. Capture lists are automatically removed at the end
   * of and right before a test execution.
   * <p>
   * <i>Note:</i> In the case of {@link Group}-based statements, all grouped
   * statements will be captured individually.
   *
   * @author paouelle
   *
   * @return a non-<code>null</code> statements capture list where the statements
   *         will be recorded
   */
  @SuppressWarnings("rawtypes")
  public StatementCaptureList<GenericStatement> withCapture() {
    return withCapture(GenericStatement.class);
  }

  /**
   * Starts capturing all statements of the specified class that have had their
   * executions requested with the Helenus statement manager in the order they
   * are occurring to the returned list. Capture lists are automatically removed
   * at the end of and right before a test execution.
   * <p>
   * <i>Note:</i> In the case of {@link Group}-based statements, all grouped
   * statements will be captured individually.
   *
   * @author paouelle
   *
   * @param <T> the type of statements to capture
   *
   * @param  clazz the class of object statements to capture
   * @return a non-<code>null</code> statements capture list where the statements
   *         will be recorded
   * @throws NullPointerException if <code>clazz</code> is <code>null</code>
   */
  @SuppressWarnings("rawtypes")
  public <T extends GenericStatement> StatementCaptureList<T> withCapture(
    Class<T> clazz
  ) {
    org.apache.commons.lang3.Validate.notNull(clazz, "invalid null class");
    synchronized (HelenusJUnit.class) {
      final StatementCaptureList<T> cl
        = new StatementCaptureList<>(HelenusJUnit.getCallerInfo(), clazz);

      HelenusJUnit.captures.add(cl);
      return cl;
    }
  }

  /**
   * Stops capturing object statements with all capture lists.
   *
   * @author paouelle
   *
   * @return this for chaining
   */
  public HelenusJUnit withoutCapture() {
    synchronized (HelenusJUnit.class) {
      HelenusJUnit.captures.forEach(cl -> cl.stop());
    }
    return this;
  }

  /**
   * Dumps the content of all the capture lists.
   *
   * @author paouelle
   *
   * @param  logger the logger where to dump the content of the capture lists
   * @param  level the log level to use when dumping
   * @return <code>true</code> if anything was dumped; <code>false</code> otherwise
   */
  public boolean dumpCaptures(Logger logger, Level level) {
    if (!HelenusJUnit.captures.isEmpty() && logger.isEnabled(level)) {
      logger.log(level, "StatementCaptureLists:");
      HelenusJUnit.captures.forEach(cl -> cl.dump(logger, level));
      return true;
    }
    return false;
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
      // force default replication factor to 1
      super(initializer, 1, true);
    }

    /**
     * Clears the cache of pojo class info.
     *
     * @author paouelle
     */
    protected void clearCache() {
      synchronized (super.classInfoCache) {
        super.classInfoCache.clear();
      }
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.StatementManagerImpl#executing(org.helenus.driver.impl.StatementImpl)
     */
    @SuppressWarnings("rawtypes")
    @Override
    protected void executing(StatementImpl statement) {
      synchronized (HelenusJUnit.class) {
        if (!HelenusJUnit.capturing || HelenusJUnit.captures.isEmpty()) { // not capturing
          return;
        }
        HelenusJUnit.captures.forEach(l -> l.executing(statement));
      }
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
      synchronized (HelenusJUnit.schemas) {
        // first check if we have already loaded it in previous test cases and if so, reset the schema for it
        @SuppressWarnings("unchecked")
        final ClassInfoImpl<T> cinfo
          = (ClassInfoImpl<T>)HelenusJUnit.fromPreviousTestsCacheInfoCache.remove(clazz);

        if (cinfo != null) {
          if (cinfo instanceof TypeClassInfoImpl) {
            // force root to be re-cached first
            getClassInfoImpl(((TypeClassInfoImpl<T>)cinfo).getRoot().getObjectClass());
          }
          synchronized (super.classInfoCache) {
            super.classInfoCache.put(clazz,  cinfo);
          }
          // first truncate all loaded pojo tables and re-insert any schema defined
          // initial objects
          HelenusJUnit.resetSchema0(cinfo);
          return cinfo;
        }
      }
      // if we get here then the cinfo was not loaded in previous tests
      final ClassInfoImpl<T> classInfo = super.getClassInfoImpl(clazz);

      // load the schemas for the pojo if required
      HelenusJUnit.createSchema0(clazz);
      return classInfo;
    }
  }
}

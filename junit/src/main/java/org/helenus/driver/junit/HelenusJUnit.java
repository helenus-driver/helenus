/*
 * Copyright (C) 2015-2016 The Helenus Driver Project Authors.
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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
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
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.WriteType;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.policies.LoggingRetryPolicy;
import com.datastax.driver.core.policies.RetryPolicy;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.service.CassandraDaemon;
import org.helenus.commons.collections.iterators.CombinationIterator;
import org.helenus.commons.lang3.reflect.ReflectionUtils;
import org.helenus.driver.AlterSchema;
import org.helenus.driver.AlterSchemas;
import org.helenus.driver.CreateIndex;
import org.helenus.driver.CreateKeyspace;
import org.helenus.driver.CreateSchema;
import org.helenus.driver.CreateSchemas;
import org.helenus.driver.CreateTable;
import org.helenus.driver.CreateType;
import org.helenus.driver.Delete;
import org.helenus.driver.ExcludedKeyspaceKeyException;
import org.helenus.driver.GenericStatement;
import org.helenus.driver.Group;
import org.helenus.driver.Insert;
import org.helenus.driver.Select;
import org.helenus.driver.Sequence;
import org.helenus.driver.StatementBuilder;
import org.helenus.driver.StatementManager;
import org.helenus.driver.Truncate;
import org.helenus.driver.Update;
import org.helenus.driver.impl.ClassInfoImpl;
import org.helenus.driver.impl.CreateSchemaImpl;
import org.helenus.driver.impl.GroupImpl;
import org.helenus.driver.impl.RootClassInfoImpl;
import org.helenus.driver.impl.StatementImpl;
import org.helenus.driver.impl.StatementManagerImpl;
import org.helenus.driver.impl.TypeClassInfoImpl;
import org.helenus.driver.impl.UDTClassInfoImpl;
import org.helenus.driver.impl.UDTRootClassInfoImpl;
import org.helenus.driver.impl.UDTTypeClassInfoImpl;
import org.helenus.driver.info.ClassInfo;
import org.helenus.driver.info.FieldInfo;
import org.helenus.driver.info.TableInfo;
import org.helenus.driver.info.TypeClassInfo;
import org.helenus.driver.junit.util.ReflectionJUnitUtils;
import org.helenus.driver.junit.util.Strings;
import org.helenus.driver.persistence.Keyspace;
import org.helenus.driver.persistence.Table;
import org.helenus.util.function.ERunnable;
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
 * @copyright 2015-2016 The Helenus Driver Project Authors
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
   * Holds a trace prefix for all CQL statements traced.
   *
   * @author paouelle
   */
  final static String TRACE_PREFIX = "HELENUS-JUNIT ";

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
   * Holds the number of times to retry a read operation that timed out.
   * <p>
   * <i>Note:</i> Defaults to none.
   *
   * @author paouelle
   */
  static volatile int numReadRetries = 0;

  /**
   * Holds the number of times to retry a write operation that timed out.
   * <p>
   * <i>Note:</i> Defaults to none.
   *
   * @author paouelle
   */
  static volatile int numWriteRetries = 0;

  /**
   * Holds a flag to control whether to trace the full statement or part of it
   * when it exceeds 2K in size.
   *
   * @author paouelle
   */
  private static volatile boolean fullTraces = false;

  /**
   * Holds a flag indicating if all statements should be traced regardless of
   * the statement tracing setting
   * (see {@link org.helenus.driver.Statement#isTracing}).
   *
   * @author paouelle
   */
  private static volatile boolean allStatementTraces = false;

  /**
   * Holds the keyspaces created so far via create schema statements.
   *
   * @author <a href="mailto:paouelle@enlightedinc.com">paouelle</a>
   */
  private static final Set<Pair<String, Keyspace>> keyspaces
    = ConcurrentHashMap.newKeySet();

  /**
   * Holds the tables created so far via create schema statements.
   *
   * @author <a href="mailto:paouelle@enlightedinc.com">paouelle</a>
   */
  private static final Map<Pair<String, Keyspace>, Set<Table>> tables
    = new ConcurrentHashMap<>();

  /**
   * Hold a set of pojo class for which we have loaded the schema definition
   * onto the embedded cassandra database along with the complete set of initial
   * objects to re-insert when resetting the schema. The value corresponds to a
   * future's representation of the schema creation for the corresponding class.
   *
   * @author paouelle
   */
  static final Map<Class<?>, SchemaFuture> schemas = new ConcurrentHashMap<>();

  /**
   * Hold the complete set of initial objects to re-insert when resetting the
   * schema along with the groups to do so.
   *
   * @author paouelle
   */
  static final Map<Class<?>, MutablePair<List<Object>, Group>> initials
    = new ConcurrentHashMap<>();

  /**
   * Holds a cache of class info structures created by previous test cases.
   *
   * @author paouelle
   */
  static final Map<Class<?>, ClassInfoImpl<?>> fromPreviousTestsCacheInfoCache
    = new ConcurrentHashMap<>(64);

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
   * Holds the keyspace key values for the current test method keyed by keyspace
   * key types.
   *
   * @author paouelle
   */
  private static volatile Map<String, Set<String>> keyspaceKeyValues = null;

  /**
   * Holds the capture lists. Use a concurrent list.
   *
   * @author paouelle
   */
  @SuppressWarnings("rawtypes")
  static final List<StatementCaptureList<? extends GenericStatement>> captures
    = new CopyOnWriteArrayList<>();

  /**
   * Holds the sent lists. Use a concurrent list.
   *
   * @author paouelle
   */
  static final List<Consumer<GenericStatement<?, ?>>> sent
    = new CopyOnWriteArrayList<>();

  /**
   * Holds a depth counter indicating if capturing is enabled or not when greater
   * than 0.
   *
   * @author paouelle
   */
  static final AtomicInteger capturing = new AtomicInteger(0);

  /**
   * Holds a depth counter indicating if we are recursing within helenus-junit
   * or not when greater than 0.
   *
   * @author paouelle
   */
  static final AtomicInteger recursing = new AtomicInteger(0);

  /**
   * Holds a flag indicating if internal CQL statements should be traced or not.
   *
   * @author paouelle
   */
  static volatile boolean traceInternalCQL = false;

  /**
   * Initializes tracing for the specified statement.
   *
   * @author paouelle
   *
   * @param  s the statement to initializes traces for
   * @return the same statement
   */
  private static <S extends GenericStatement<?, ?>> S initTrace(S s) {
    if (HelenusJUnit.traceInternalCQL) {
      s.enableTracing(HelenusJUnit.TRACE_PREFIX);
    }
    s.enableErrorTracing(HelenusJUnit.TRACE_PREFIX);
    return s;
  }

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
      CommitLog.instance.getContext(); // wait for commit log allocator instantiation to avoid hanging on a race condition
      CommitLog.instance.resetUnsafe(true); // cleanup screws with CommitLog, this brings it back to safe state
    } catch (IOException e) {
      throw new AssertionError(e);
    }
  }

  /**
   * Find the keyspace key values from the specified test definition that matches
   * the specified pojo class.
   *
   * @author paouelle
   *
   * @param  cinfo the non-<code>null</code> pojo class information
   * @return a non-<code>null</code> collection of all keyspace key values keyed by
   *         their names based on the specified class info (each entry in the main
   *         collection provides another collection of all possible values for a
   *         given keyspace key)
   */
  private static Collection<Collection<Strings>> getKeyspaceKeyValues(
    ClassInfo<?> cinfo
  ) {
    final Map<String, Set<String>> skvss = HelenusJUnit.keyspaceKeyValues;

    if (skvss == null) {
      return Collections.emptyList();
    }
    return skvss.entrySet().stream()
      .map(e -> {
        final FieldInfo<?> finfo = cinfo.getKeyspaceKeyByType(e.getKey());

        if (finfo != null) { // pojo defines this keyspace key type
          return e.getValue().stream()
            .map(v -> new Strings(finfo.getKeyspaceKeyName(), v))
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
   * @param group the non-<code>null</code> group to insert the objects to
   *        create in
   * @param m the non-<code>null</code> method to invoke to get the initial objects
   * @param bo the non-<code>null</code> annotation for the method
   * @param target the test object for which we are calling the method
   * @param method the test method
   * @param kkeys the map of keyspace keys to pass to the method
   * @param onlyIfRequiresKeyspaceKeys <code>true</code> if the method should
   *        not be called if it doesn't require keyspace keys
   */
  private static void processBeforeObjects(
    Group group,
    Method m,
    BeforeObjects bo,
    Object target,
    FrameworkMethod method,
    Map<String, String> kkeys,
    boolean onlyIfRequiresKeyspaceKeys
  ) {
    if (!ArrayUtils.isEmpty(bo.value())
        && !ArrayUtils.contains(bo.value(), method.getName())) {
      return;
    }
    try {
      final Class<?>[] cparms = m.getParameterTypes();
      final Object ret;

      // check if the method expects a map of keyspace keys
      if (cparms.length == 0) { // doesn't care about keyspace keys
        if (!onlyIfRequiresKeyspaceKeys) {
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
        ret = m.invoke(target, kkeys);
      }
      HelenusJUnit.processObjects(group, ret);
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
   * @param group the non-<code>null</code> group to insert the objects to
   *        create in
   * @param ret the return object to process
   */
  @SuppressWarnings("unchecked")
  private static void processObjects(Group group, Object ret) {
    if (ret == null) { // nothing to do
      return;
    }
    // validate the return type is either an array, a collection, or a stream
    final Class<?> type = ret.getClass();

    if (type.isArray()) {
      final int l = Array.getLength(ret);

      for (int i = 0; i < l; i++) {
        group.add(HelenusJUnit.initTrace(
          StatementBuilder.insert(Array.get(ret, i)).intoAll()
        ));
      }
    } else if (ret instanceof Collection) {
      ((Collection<Object>)ret).forEach(o -> {
        group.add(HelenusJUnit.initTrace(
          StatementBuilder.insert(o).intoAll()
        ));
      });
    } else if (ret instanceof Stream) {
      ((Stream<Object>)ret).forEach(o -> StatementBuilder.insert(o).intoAll());
    } else if (ret instanceof Iterator) {
      for (final Iterator<?> i = (Iterator<?>)ret; i.hasNext(); ) {
        group.add(HelenusJUnit.initTrace(
          StatementBuilder.insert(i.next()).intoAll()
        ));
      }
    } else if (ret instanceof Enumeration<?>) {
      for (final Enumeration<?> e = (Enumeration<?>)ret; e.hasMoreElements(); ) {
        group.add(HelenusJUnit.initTrace(
          StatementBuilder.insert(e.nextElement()).intoAll()
        ));
      }
    } else if (ret instanceof Iterable) {
      for (final Iterator<?> i = ((Iterable<?>)ret).iterator(); i.hasNext(); ) {
        group.add(HelenusJUnit.initTrace(
          StatementBuilder.insert(i.next()).intoAll()
        ));
      }
    } else {
      group.add(HelenusJUnit.initTrace(
        StatementBuilder.insert(ret).intoAll()
      ));
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
    final Map<String, Set<String>> kkeys = HelenusJUnit.keyspaceKeyValues;
    final Collection<Collection<Strings>> keyspaceKeysByTypes;

    if (kkeys != null) {
      keyspaceKeysByTypes = kkeys.entrySet().stream()
        .map(e -> e.getValue().stream()
          .map(v -> new Strings(e.getKey(), v))
          .collect(Collectors.toList())
        )
        .collect(Collectors.toList());
    } else {
      keyspaceKeysByTypes = Collections.emptyList();
    }
    final Map<Method, BeforeObjects[]> methods = ReflectionUtils.getAllAnnotationsForMethodsAnnotatedWith(
      target.getClass(), BeforeObjects.class, true
    );
    final Group group = HelenusJUnit.initTrace(StatementBuilder.group());

    if (CollectionUtils.isEmpty(keyspaceKeysByTypes)) {
      // no keyspace keys so call with empty map of keyspace keys
      methods.forEach(
        (m, bos) -> HelenusJUnit.processBeforeObjects(
          group, m, bos[0], target, method, Collections.emptyMap(), false // BeforeObjects is not repeatable so only 1 in array
        )
      );
    } else {
      boolean keyspace = false; // only the first time we call them

      for (final Iterator<List<Strings>> i = new CombinationIterator<>(Strings.class, keyspaceKeysByTypes); i.hasNext(); ) {
        final List<Strings> ikkeys = i.next();
        final Map<String, String> kkeyvalues = new HashMap<>(ikkeys.size() * 3 / 2);
        final boolean oirs = keyspace;

        ikkeys.forEach(ss -> kkeyvalues.put(ss.key, ss.value));
        methods.forEach(
          (m, bos) -> HelenusJUnit.processBeforeObjects(
            group, m, bos[0], target, method, kkeyvalues, oirs // BeforeObjects is not repeatable so only 1 in array
          )
        );
        keyspace = true; // from now on, only call those that requires keyspace keys
      }
    }
    group.execute();
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
      HelenusJUnit.group = new ThreadGroup("Cassandra Daemon Group") {
        @Override
        public void uncaughtException(Thread t, Throwable e) {
          logger.error("uncaught exception from cassandra daemon thread '" + t.getName() + "': ", e);
          super.uncaughtException(t, e);
        }
      };
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
            } catch (RuntimeException|Error e) {
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
          Thread.getAllStackTraces().forEach((t, se) -> {
            if (HelenusJUnit.isCassandraDaemonThread(t)) {
              logger.error(t);
              for (final StackTraceElement te: se) {
                logger.error("\tat %s", te);
              }
            }
          });
          logger.error("Cassandra daemon failed to start within timeout");
          throw new AssertionError(
            "cassandra daemon failed to start within timeout"
          );
        }
        final Throwable t = failed.get();

        if (t != null) {
          logger.error("Cassandra daemon failed to start; %s", t);
          throw new AssertionError(
            "cassandra daemon failed to start", t
          );
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
            .withQueryOptions(new QueryOptions().setRefreshSchemaIntervalMillis(0)) // disable debouncing schema updates since we are unit testing!
            .withRetryPolicy(new LoggingRetryPolicy(new RetryPolicy() {
              @Override
              public RetryDecision onReadTimeout(
                com.datastax.driver.core.Statement statement,
                ConsistencyLevel cl,
                int requiredResponses,
                int receivedResponses,
                boolean dataRetrieved,
                int nbRetry
              ) {
                if (nbRetry < HelenusJUnit.numReadRetries) {
                  return RetryDecision.rethrow();
                }
                return RetryDecision.retry(cl);
              }
              @Override
              public RetryDecision onWriteTimeout(
                com.datastax.driver.core.Statement statement,
                ConsistencyLevel cl,
                WriteType writeType,
                int requiredAcks,
                int receivedAcks,
                int nbRetry
              ) {
                if (nbRetry < HelenusJUnit.numWriteRetries) {
                  return RetryDecision.rethrow();
                }
                return RetryDecision.retry(cl);
              }
              @Override
              public RetryDecision onUnavailable(
                com.datastax.driver.core.Statement statement,
                ConsistencyLevel cl,
                int requiredReplica,
                int aliveReplica,
                int nbRetry
              ) {
                return RetryDecision.rethrow();
              }
            }))
            .addContactPoint(host)
            .withQueryOptions(null)
        );
        if (HelenusJUnit.fullTraces) {
          HelenusJUnit.manager.enableFullTraces();
        } else {
          HelenusJUnit.manager.disableFullTraces();
        }
        if (HelenusJUnit.allStatementTraces) {
          HelenusJUnit.manager.enableAllStatementsTraces();
        } else {
          HelenusJUnit.manager.disableAllStatementsTraces();
        }
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
   * @param  group the group to record all statements
   * @throws AssertionError if a failure occurs while clearing the schema
   */
  private static <T> void clearSchema0(ClassInfoImpl<T> cinfo, Group group) {
    if (cinfo instanceof UDTClassInfoImpl) { // nothing to reset but we want the above trace
      return;
    }
    try {
      HelenusJUnit.capturing.incrementAndGet(); // disable temporarily capturing
      HelenusJUnit.recursing.incrementAndGet(); // enable temporarily recursing
      // find all keyspace keys that are defined for this classes
      final Collection<Collection<Strings>> kkeys = ((target != null)
        ? HelenusJUnit.getKeyspaceKeyValues(cinfo)
        : null
      );
      // since we already created the schema, we should have the right number of keyspace keys
      // now generate as many insert statements for each initial object as
      // required by the combination of all keyspace key values
      if (CollectionUtils.isEmpty(kkeys)) {
        // no keyspace keys so just the one truncate
        group.add(HelenusJUnit.initTrace(
          StatementBuilder.truncate(cinfo.getObjectClass())
        ));
      } else {
        next_combination:
        for (final Iterator<List<Strings>> i = new CombinationIterator<>(Strings.class, kkeys); i.hasNext(); ) {
          final Truncate<T> truncate = HelenusJUnit.initTrace(
            StatementBuilder.truncate(cinfo.getObjectClass())
          );

          // pass all required keyspace keys
          for (final Strings ss: i.next()) {
            // register the keyspace key value with the corresponding keyspace key name
            try {
              truncate.where(StatementBuilder.eq(ss.key, ss.value));
            } catch (ExcludedKeyspaceKeyException e) {// ignore this combination
              continue next_combination;
            }
          }
          group.add(truncate);
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
      HelenusJUnit.recursing.decrementAndGet(); // restore previous recursing setting
      HelenusJUnit.capturing.decrementAndGet(); // restore previous capturing setting
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
    // clear all initials objects
    HelenusJUnit.initials.clear();
  }

  /**
   * Clears the database and the Helenus driver by partially resetting them to a
   * similar state they were before the previous test case.
   *
   * @author paouelle
   *
   * @throws AssertionError if a failure occurs while cleanup
   */
  private static synchronized void clear0() {
    final StatementManagerUnitImpl mgr = HelenusJUnit.manager;

    if (mgr != null) {
      try {
        // start by truncating all loaded pojo tables
        final Group group = HelenusJUnit.initTrace(StatementBuilder.group());

        mgr.classInfoImpls()
          .filter(c -> !(c instanceof TypeClassInfo))
          .collect(Collectors.toSet()) // force a snapshot
          .forEach(c -> HelenusJUnit.clearSchema0(c, group));
        group.execute();
        // next preserve all class infos already cached
        mgr.classInfoImpls()
          .forEach(c -> HelenusJUnit.fromPreviousTestsCacheInfoCache.put(
            c.getObjectClass(), c
          ));
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

  /**
   * Creates the schema for the specified pojo class onto the embedded Cassandra
   * database.
   *
   * @author paouelle
   *
   * @param  <T> the class of POJO to have the schema created
   * @param  cinfo the class info already defined by the manager
   * @return <code>cinfo</code>
   * @throws NullPointerException if <code>cinfo</code> is <code>null</code>
   * @throws AssertionError if a failure occurs while creating the schema
   */
  static <T> ClassInfoImpl<T> createSchema0(ClassInfoImpl<T> cinfo) {
    org.apache.commons.lang3.Validate.notNull(cinfo, "invalid null class info");
    final Class<T> clazz = cinfo.getObjectClass();
    // check whether the schema for this pojo has already been loaded
    final SchemaFuture result = new SchemaFuture(true, clazz);
    final SchemaFuture future = HelenusJUnit.schemas.putIfAbsent(cinfo.getObjectClass(), result);

    if (future != null) { // another thread is creating this schema so wait for it to complete
      future.waitForCompletion();
      return cinfo; // done
    } // else - we are in charge of creating the schema!!!
    try {
      logger.debug("Creating schema for %s", clazz.getSimpleName());
      HelenusJUnit.capturing.incrementAndGet(); // disable temporarily capturing
      HelenusJUnit.recursing.incrementAndGet(); // enable temporarily recursing
      // find all keyspace keys that are defined for this classes
      final Collection<Collection<Strings>> kkeys = ((target != null)
        ? HelenusJUnit.getKeyspaceKeyValues(cinfo)
        : null
      );

      // now check if we have the right number of keyspace keys as if we don't, we
      // cannot create this schema
      if (cinfo.getNumKeyspaceKeys() != CollectionUtils.size(kkeys)) {
        throw new AssertionError(
          "unable to create schema for '"
          + clazz.getSimpleName()
          + "'; missing required keyspace keys"
        );
      }
      // now generate as many create schemas statement as required by the combination
      // of all keyspace key values, at the same time compile separately the complete
      // list of initial objects so we have it already computed the next time
      // we reset the schema for this clazz
      final boolean initialsOnly = ((cinfo instanceof TypeClassInfoImpl) && !((TypeClassInfoImpl<?>)cinfo).isDynamic());
      final List<Object> initials = new ArrayList<>();
      final Sequence sequence = StatementBuilder.sequence();
      // create one group to aggregate all create table, index, type, and initial objects
      // create groups to aggregate all create keyspaces, table, create index, and initial objects
      final GroupImpl kgroup = HelenusJUnit.initTrace((GroupImpl)StatementBuilder.group());
      final GroupImpl tgroup = HelenusJUnit.initTrace((GroupImpl)StatementBuilder.group());
      final GroupImpl igroup = HelenusJUnit.initTrace((GroupImpl)StatementBuilder.group());
      final GroupImpl ygroup = HelenusJUnit.initTrace((GroupImpl)StatementBuilder.group());
      final GroupImpl group = HelenusJUnit.initTrace((GroupImpl)StatementBuilder.group());

      if (CollectionUtils.isEmpty(kkeys)) {
        initials.addAll(cinfo.newContext().getInitialObjects());
        if (initialsOnly) {
          // no need to create the schema as it would have already been created
          // by the root, we only need to insert initial objects, done later
        } else {
          // no keyspace keys so just the one create schema required
          final CreateSchemaImpl<?> cs = HelenusJUnit.initTrace(
            (CreateSchemaImpl<?>)StatementBuilder.createSchema(clazz)
          );

          cs.ifNotExists();
          initials.addAll(cs.getContext().getInitialObjects());
          if (!initialsOnly) {
            // safe to combine types since we are creating only one type anyway
            cs.buildSequencedStatements(
              HelenusJUnit.keyspaces, HelenusJUnit.tables, kgroup, tgroup, igroup, ygroup, group
            );
          }
        }
      } else {
        next_combination:
        for (final Iterator<List<Strings>> i = new CombinationIterator<>(Strings.class, kkeys); i.hasNext(); ) {
          final CreateSchemaImpl<?> cs = HelenusJUnit.initTrace(
            (CreateSchemaImpl<?>)StatementBuilder.createSchema(clazz)
          );

          cs.ifNotExists();
          // pass all required keyspace keys
          for (final Strings ss: i.next()) {
            // register the keyspace key value with the corresponding keyspace key name
            try {
              cs.where(StatementBuilder.eq(ss.key, ss.value));
            } catch (ExcludedKeyspaceKeyException e) { // ignore this combination
              continue next_combination;
            }
          }
          initials.addAll(cs.getContext().getInitialObjects());
          if (!initialsOnly) {
            // safe to combine types since we are creating only one type anyway
            // for multiple keyspaces
            cs.buildSequencedStatements(
              HelenusJUnit.keyspaces, HelenusJUnit.tables, kgroup, tgroup, igroup, ygroup, group
            );
          }
        }
      }
      HelenusJUnit.initials.put(clazz, MutablePair.of(initials, null)); // cache all initials objects for next time
      if (initialsOnly) {
        // no need to create the schema as it would have already been created
        // by the root, we only need to insert initial objects now that we have
        // cached the set of initial objects, reset the schema to get them
        // inserted which is done for Type POJOs only
        resetSchema0(cinfo, result);
      } else {
        if (!kgroup.isEmpty()) {
          sequence.add(kgroup);
        }
        if (!ygroup.isEmpty()) {
          sequence.add(ygroup);
        }
        if (!tgroup.isEmpty()) {
          sequence.add(tgroup);
        }
        if (!igroup.isEmpty()) {
          sequence.add(igroup);
        }
        if (!group.isEmpty()) {
          sequence.add(group);
        }
        sequence.execute();
      }
      result.completed(); // done with creating the schema
      return cinfo;
    } catch (AssertionError|ThreadDeath|StackOverflowError|OutOfMemoryError e) {
      // make sure to remove cached schema indicator
      HelenusJUnit.schemas.remove(clazz);
      HelenusJUnit.initials.remove(clazz);
      throw result.failed(e); // failed creating the schema
    } catch (Throwable t) {
      // make sure to remove cached schema indicator
      HelenusJUnit.schemas.remove(clazz);
      HelenusJUnit.initials.remove(clazz);
      throw result.failed(t); // failed creating the schema
    } finally {
      HelenusJUnit.recursing.decrementAndGet(); // restore previous recursing setting
      HelenusJUnit.capturing.decrementAndGet(); // restore previous capturing setting
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
   * @param  result the result where to record the completion or <code>null</code>
   *         to check if it is in progress and update our own result
   * @throws NullPointerException if <code>cinfo</code> is <code>null</code>
   * @throws AssertionError if a failure occurs while reseting the schema
   */
  @SuppressWarnings("unchecked")
  static <T> void resetSchema0(
    ClassInfoImpl<T> cinfo, SchemaFuture result
  ) {
    org.apache.commons.lang3.Validate.notNull(cinfo, "invalid null class info");
    final Class<T> clazz = cinfo.getObjectClass();

    if (result == null) {
      final SchemaFuture myresult = new SchemaFuture(false, clazz);
      final SchemaFuture future = HelenusJUnit.schemas.putIfAbsent(
        cinfo.getObjectClass(), myresult
      );

      if (future != null) { // another thread is resetting this schema so wait for it to complete
        future.waitForCompletion();
        return; // done
      } // else - we are in charge of resetting the schema!!!
      logger.debug("Resetting schema for %s", clazz.getSimpleName());
      result = myresult; // continue with our results
    } // else - we are in charge of resetting the schema!!!
    try {
      HelenusJUnit.capturing.incrementAndGet(); // disable temporarily capturing
      HelenusJUnit.recursing.incrementAndGet(); // enable temporarily recursing
      MutablePair<List<Object>, Group> p = HelenusJUnit.initials.get(clazz);
      final Group group;

      if (p == null) {
        // hum! it means that somehow the schema was created but we didn't
        // have time to compute the initial objects - so let's do it here
        // find all keyspace keys that are defined for this classes
        final Collection<Collection<Strings>> kkeys = ((target != null)
          ? HelenusJUnit.getKeyspaceKeyValues(cinfo)
          : null
        );
        final boolean initialsOnly = ((cinfo instanceof TypeClassInfoImpl) && !((TypeClassInfoImpl<?>)cinfo).isDynamic());
        final List<Object> initials = new ArrayList<>();

        if (CollectionUtils.isEmpty(kkeys)) {
          initials.addAll(cinfo.newContext().getInitialObjects());
          if (!initialsOnly) {
            // no keyspace keys so just the one schema required
            final CreateSchemaImpl<?> cs = HelenusJUnit.initTrace(
              (CreateSchemaImpl<?>)StatementBuilder.createSchema(clazz)
            );

            initials.addAll(cs.getContext().getInitialObjects());
          }
        } else {
          next_combination:
          for (final Iterator<List<Strings>> i = new CombinationIterator<>(Strings.class, kkeys); i.hasNext(); ) {
            final CreateSchemaImpl<?> cs = HelenusJUnit.initTrace(
              (CreateSchemaImpl<?>)StatementBuilder.createSchema(clazz)
            );

            // pass all required keyspace keys
            for (final Strings ss: i.next()) {
              // register the keyspace key value with the corresponding keyspace key name
              try {
                cs.where(StatementBuilder.eq(ss.key, ss.value));
              } catch (ExcludedKeyspaceKeyException e) { // ignore this combination
                continue next_combination;
              }
            }
            initials.addAll(cs.getContext().getInitialObjects());
          }
        }
        p = MutablePair.of(initials, null);
        HelenusJUnit.initials.put(clazz, p); // cache all initials objects for next time
      }
      if (p.getRight() != null) {
        group = p.getRight();
      } else {
        group = HelenusJUnit.initTrace(StatementBuilder.group());
        for (final Object io: p.getLeft()) {
          group.add(HelenusJUnit.initTrace(
            StatementBuilder.insert((T)io).intoAll()
          ));
        }
        p.setRight(group); // cache the group for next time
      }
      group.execute();
      result.completed(); // done with resetting the schema
    } catch (AssertionError|ThreadDeath|StackOverflowError|OutOfMemoryError e) {
      // make sure to remove cached schema indicator
      HelenusJUnit.schemas.remove(clazz);
      throw result.failed(e); // failed resetting the schema
    } catch (Throwable t) {
      // make sure to remove cached schema indicator
      HelenusJUnit.schemas.remove(clazz);
      throw result.failed(t); // failed resetting the schema
    } finally {
      HelenusJUnit.recursing.decrementAndGet(); // restore previous recursing setting
      HelenusJUnit.capturing.decrementAndGet(); // restore previous capturing setting
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
    final Map<String, Set<String>> kkeys = new LinkedHashMap<>(12);

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
        HelenusJUnit.capturing.set(Integer.MAX_VALUE);
        // start embedded cassandra daemon
        HelenusJUnit.start0(cfgname, timeout);
        HelenusJUnit.method = method;
        HelenusJUnit.target = target;
        for (final PartitionKeyValues skvs: ReflectionJUnitUtils.getAnnotationsByType(
              method.getMethod(), PartitionKeyValues.class
            )) {
          kkeys.compute(
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
        HelenusJUnit.keyspaceKeyValues = kkeys;
        // finally cleanup the database for this new test
        HelenusJUnit.clear0();
      } catch (AssertionError|ThreadDeath|StackOverflowError|OutOfMemoryError e) {
        // make sure to cleanup
        HelenusJUnit.method = null;
        HelenusJUnit.target = null;
        logger.error("Failed to start Cassandra daemon", e);
        // System.exit(200); // force an exit!!!
        throw e;
      } catch (Throwable t) {
        // make sure to cleanup
        HelenusJUnit.method = null;
        HelenusJUnit.target = null;
        logger.error("Failed to start Cassandra daemon", t);
        // System.exit(200); // force an exit!!!
        throw new AssertionError("failed to start Cassandra daemon", t);
      } finally {
        HelenusJUnit.captures.clear();
      }
      try {
        HelenusJUnit.capturing.set(0);
        HelenusJUnit.capturing.incrementAndGet(); // disable temporarily capturing (also used for recursion detection)
        HelenusJUnit.recursing.set(0);
        HelenusJUnit.recursing.incrementAndGet(); // enable temporarily recursing
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
      HelenusJUnit.recursing.set(0);
      HelenusJUnit.capturing.set(0);
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
        HelenusJUnit.capturing.set(0);
      }
    }
  }

  /**
   * Sets the number of times to retry a read or write operation that timed out.
   * <p>
   * <i>Note:</i> Defaults to none.
   *
   * @author paouelle
   *
   * @param retries the number of times to retry a read or write operation that
   *        timed out
   */
  public void setNumberRetries(int retries) {
    HelenusJUnit.numReadRetries = retries;
    HelenusJUnit.numWriteRetries = retries;
  }

  /**
   * Sets the number of times to retry a read operation that timed out.
   * <p>
   * <i>Note:</i> Defaults to none.
   *
   * @author paouelle
   *
   * @param retries the number of times to retry a read operation that timed out
   */
  public void setNumberReadRetries(int retries) {
    HelenusJUnit.numReadRetries = retries;
  }

  /**
   * Sets the number of times to retry a write operation that timed out.
   * <p>
   * <i>Note:</i> Defaults to none.
   *
   * @author paouelle
   *
   * @param retries the number of times to retry a write operation that timed out
   */
  public void setNumberWriteRetries(int retries) {
    HelenusJUnit.numWriteRetries = retries;
  }

  /**
   * Enables internal CQL statements tracing.
   *
   * @author paouelle
   */
  public void enableInternalCQLTracing() {
    HelenusJUnit.traceInternalCQL = true;
  }

  /**
   * Disables internal CQL statements tracing.
   *
   * @author paouelle
   */
  public void disableInternalCQLTracing() {
    HelenusJUnit.traceInternalCQL = false;
  }


  /**
   * Enables tracing large statements beyond 2K.
   *
   * @author paouelle
   */
  public void enableFullTraces() {
    HelenusJUnit.fullTraces = true;
    if (HelenusJUnit.manager != null) {
      HelenusJUnit.manager.enableFullTraces();
    }
  }

  /**
   * Disables tracing large statements beyond 2K.
   *
   * @author paouelle
   */
  public void disableFullTraces() {
    HelenusJUnit.fullTraces = false;
    if (HelenusJUnit.manager != null) {
      HelenusJUnit.manager.disableFullTraces();
    }
  }

  /**
   * Enables all statements to be traced regardless of the statement tracing
   * setting (see {@link org.helenus.driver.Statement#isTracing}).
   *
   * @author paouelle
   */
  public void enableAllStatementsTraces() {
    HelenusJUnit.allStatementTraces = true;
    if (HelenusJUnit.manager != null) {
      HelenusJUnit.manager.enableAllStatementsTraces();
    }
  }

  /**
   * Disables all statements to be traced automatically regardless of the
   * statement tracing setting (see {@link org.helenus.driver.Statement#isTracing}).
   *
   * @author paouelle
   */
  public void disableAllStatementsTraces() {
    HelenusJUnit.allStatementTraces = false;
    if (HelenusJUnit.manager != null) {
      HelenusJUnit.manager.disableAllStatementsTraces();
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
    HelenusJUnit.manager.getClassInfoImpl(clazz);
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
    try {
      HelenusJUnit.capturing.incrementAndGet(); // disable temporarily capturing
      final Group group = HelenusJUnit.initTrace(StatementBuilder.group());

      HelenusJUnit.processObjects(group, objs.get());
      group.execute();
    } catch (AssertionError|ThreadDeath|StackOverflowError|OutOfMemoryError e) {
      throw e;
    } catch (Throwable t) {
      throw new AssertionError("failed to populate objects", t);
    } finally {
      HelenusJUnit.capturing.decrementAndGet(); // restore previous capturing setting
    }
    return this;
  }

  /**
   * Populates the database with objects returned by the specified function. This
   * version of the <code>populate()</code> allows one to receive a map of all
   * keyspace key values defined in the test environment using the
   * {@link PartitionKeyValues} annotations. As such, the function might be called
   * multiple times with each combination of keyspace key values.
   * <p>
   * <i>Note:</i> The function can return an array, a {@link Collection},
   * an {@link Iterable}, an {@link Iterator}, an {@link Enumeration}, or a
   * {@link Stream} of pojo objects or a single object to insert in the database.
   *
   * @author paouelle
   *
   * @param  objs the function to receive a map of keyspace key values and return
   *         objects to populate the database with
   * @return this for chaining
   * @throws AssertionError if any error occurs
   */
  public HelenusJUnit populate(Function<Map<String, String>, ? super Object> objs) {
    if (objs == null) {
      return this;
    }
    try {
      HelenusJUnit.capturing.incrementAndGet(); // disable temporarily capturing
      final Map<String, Set<String>> kkeysValues = HelenusJUnit.keyspaceKeyValues;
      final Collection<Collection<Strings>> kkeysByTypes;

      if (kkeysValues != null) {
        kkeysByTypes = kkeysValues.entrySet().stream()
        .map(e -> e.getValue().stream()
          .map(v -> new Strings(e.getKey(), v))
          .collect(Collectors.toList())
        )
        .collect(Collectors.toList());
      } else {
        kkeysByTypes = Collections.emptyList();
      }
      final Group group = HelenusJUnit.initTrace(StatementBuilder.group());

      if (CollectionUtils.isEmpty(kkeysByTypes)) {
        // no keyspace keys so call with empty map of keyspace keys
        HelenusJUnit.processObjects(group, objs.apply(Collections.emptyMap()));
      } else {
        for (final Iterator<List<Strings>> i = new CombinationIterator<>(Strings.class, kkeysByTypes); i.hasNext(); ) {
          final List<Strings> ikkeys = i.next();
          final Map<String, String> kkeys = new HashMap<>(ikkeys.size() * 3 / 2);

          ikkeys.forEach(ss -> kkeys.put(ss.key, ss.value));
          HelenusJUnit.processObjects(group, objs.apply(kkeys));
        }
      }
      group.execute();
    } catch (AssertionError|ThreadDeath|StackOverflowError|OutOfMemoryError e) {
      throw e;
    } catch (Throwable t) {
      throw new AssertionError("failed to populate objects", t);
    } finally {
      HelenusJUnit.capturing.decrementAndGet(); // restore previous capturing setting
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
    try {
      HelenusJUnit.capturing.incrementAndGet(); // disable temporarily capturing
      final Group group = HelenusJUnit.initTrace(StatementBuilder.group());

      while (objs.hasNext()) {
        group.add(HelenusJUnit.initTrace(
          StatementBuilder.insert(objs.next()).intoAll()
        ));
      }
      group.execute();
    } catch (AssertionError|ThreadDeath|StackOverflowError|OutOfMemoryError e) {
      throw e;
    } catch (Throwable t) {
      throw new AssertionError("failed to populate objects", t);
    } finally {
      HelenusJUnit.capturing.decrementAndGet(); // restore previous capturing setting
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
    try {
      HelenusJUnit.capturing.incrementAndGet(); // disable temporarily capturing
      final Group group = HelenusJUnit.initTrace(StatementBuilder.group());

      objs.forEachOrdered(o -> group.add(HelenusJUnit.initTrace(
        StatementBuilder.insert(o).intoAll()
      )));
      group.execute();
    } catch (AssertionError|ThreadDeath|StackOverflowError|OutOfMemoryError e) {
      throw e;
    } catch (Throwable t) {
      throw new AssertionError("failed to populate objects", t);
    } finally {
      HelenusJUnit.capturing.decrementAndGet(); // restore previous capturing setting
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
    return truncate(classes.iterator());
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
    try {
      HelenusJUnit.capturing.incrementAndGet(); // disable temporarily capturing
      final Group group = HelenusJUnit.initTrace(StatementBuilder.group());

      while (classes.hasNext()) {
        group.add(HelenusJUnit.initTrace(
          StatementBuilder.truncate(classes.next())
        ));
      }
      group.execute();
    } catch (AssertionError|ThreadDeath|StackOverflowError|OutOfMemoryError e) {
      throw e;
    } catch (Throwable t) {
      throw new AssertionError("failed to truncate classes", t);
    } finally {
      HelenusJUnit.capturing.decrementAndGet(); // restore previous capturing setting
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
    try {
      HelenusJUnit.capturing.incrementAndGet(); // disable temporarily capturing
      final Group group = HelenusJUnit.initTrace(StatementBuilder.group());

      classes.forEach(c -> {
        final ClassInfo<?> cinfo = StatementBuilder.getClassInfo(c);
        // find all keyspace keys that are defined for this classes
        final Collection<Collection<Strings>> kkeys = ((target != null)
          ? HelenusJUnit.getKeyspaceKeyValues(cinfo)
          : null
        );

        // since we already created the schema, we should have the right number of keyspace keys
        // now generate as many insert statements for each initial object as
        // required by the combination of all keyspace key values
        if (CollectionUtils.isEmpty(kkeys)) {
          // no keyspace keys so just the one truncate
          group.add(HelenusJUnit.initTrace(
            StatementBuilder.truncate(c)
          ));
        } else {
          next_combination:
          for (final Iterator<List<Strings>> i = new CombinationIterator<>(Strings.class, kkeys); i.hasNext(); ) {
            final Truncate<?> truncate = HelenusJUnit.initTrace(
              StatementBuilder.truncate(c)
            );

            // pass all required keyspace keys
            for (final Strings ss: i.next()) {
              try {
                // register the keyspace key value with the corresponding keyspace key name
                truncate.where(StatementBuilder.eq(ss.key, ss.value));
              } catch (ExcludedKeyspaceKeyException e) {// ignore this combination
                continue next_combination;
              }
            }
            group.add(truncate);
          }
        }
      });
      group.execute();
    } catch (AssertionError|ThreadDeath|StackOverflowError|OutOfMemoryError e) {
      throw e;
    } catch (Throwable t) {
      throw new AssertionError("failed to truncate classes", t);
    } finally {
      HelenusJUnit.capturing.decrementAndGet(); // restore previous capturing setting
    }
    return this;
  }

  /**
   * Executes the specified simple statements.
   *
   * @author paouelle
   *
   * @param  statements the simple statements to execute
   * @return this for chaining
   * @throws AssertionError if any error occurs
   */
  public HelenusJUnit execute(String... statements) {
    if (statements == null) {
      return this;
    }
    return execute(Stream.of(statements));
  }

  /**
   * Executes the specified simple statements.
   *
   * @author paouelle
   *
   * @param  statements the simple statements to execute
   * @return this for chaining
   * @throws AssertionError if any error occurs
   */
  public HelenusJUnit execute(Iterable<String> statements) {
    if (statements == null) {
      return this;
    }
    return execute(statements.iterator());
  }

  /**
   * Execute the specified simple statements in sequence.
   *
   * @author paouelle
   *
   * @param  statements the simple statements to execute
   * @return this for chaining
   * @throws AssertionError if any error occurs
   */
  public HelenusJUnit execute(Iterator<String> statements) {
    if (statements == null) {
      return this;
    }
    try {
      HelenusJUnit.capturing.incrementAndGet(); // disable temporarily capturing
      final Sequence sequence = HelenusJUnit.initTrace(
        StatementBuilder.sequence()
      );

      while (statements.hasNext()) {
        sequence.add(new SimpleStatement(statements.next()));
      }
      sequence.execute();
    } catch (AssertionError|ThreadDeath|StackOverflowError|OutOfMemoryError e) {
      throw e;
    } catch (Throwable t) {
      throw new AssertionError("failed to execute statements", t);
    } finally {
      HelenusJUnit.capturing.decrementAndGet(); // restore previous capturing setting
    }
    return this;
  }

  /**
   * Executes the specified simple statements in sequence.
   *
   * @author paouelle
   *
   * @param  statements the simple statements to execute
   * @return this for chaining
   * @throws AssertionError if any error occurs
   */
  public HelenusJUnit execute(Stream<String> statements) {
    if (statements == null) {
      return this;
    }
    try {
      HelenusJUnit.capturing.incrementAndGet(); // disable temporarily capturing
      final Sequence sequence = HelenusJUnit.initTrace(
        StatementBuilder.sequence()
      );

      statements.forEach(s -> sequence.add(new SimpleStatement(s)));
      sequence.execute();
    } catch (AssertionError|ThreadDeath|StackOverflowError|OutOfMemoryError e) {
      throw e;
    } catch (Throwable t) {
      throw new AssertionError("failed to execute statements", t);
    } finally {
      HelenusJUnit.capturing.decrementAndGet(); // restore previous capturing setting
    }
    return this;
  }

  /**
   * Initializes the schema for the specified pojo classes in the database.
   *
   * @author paouelle
   *
   * @param  classes the pojo classes for which to initialize the schema
   * @return this for chaining
   * @throws AssertionError if any error occurs
   */
  public HelenusJUnit schema(Class<?>... classes) {
    if (classes == null) {
      return this;
    }
    return schema(Stream.of(classes));
  }

  /**
   * Initializes the schema for the specified pojo classes in the database.
   *
   * @author paouelle
   *
   * @param  classes the pojo classes for which to initialize the schema
   * @return this for chaining
   * @throws AssertionError if any error occurs
   */
  public HelenusJUnit schema(Iterable<Class<?>> classes) {
    if (classes == null) {
      return this;
    }
    return schema(classes.iterator());
  }

  /**
   * Initializes the schema for the specified pojo classes in the database.
   *
   * @author paouelle
   *
   * @param  classes the pojo classes for which to initialize the schema
   * @return this for chaining
   * @throws AssertionError if any error occurs
   */
  public HelenusJUnit schema(Iterator<Class<?>> classes) {
    if (classes == null) {
      return this;
    }
    try {
      HelenusJUnit.capturing.incrementAndGet(); // disable temporarily capturing
      while (classes.hasNext()) {
        StatementBuilder.getClassInfo(classes.next());
      }
    } catch (AssertionError|ThreadDeath|StackOverflowError|OutOfMemoryError e) {
      throw e;
    } catch (Throwable t) {
      throw new AssertionError("failed to initialize schemas", t);
    } finally {
      HelenusJUnit.capturing.decrementAndGet(); // restore previous capturing setting
    }
    return this;
  }

  /**
   * Initializes the schema for the specified pojo classes in the database.
   *
   * @author paouelle
   *
   * @param  classes the pojo classes for which to initialize the schema
   * @return this for chaining
   * @throws AssertionError if any error occurs
   */
  public HelenusJUnit schema(Stream<Class<?>> classes) {
    if (classes == null) {
      return this;
    }
    try {
      HelenusJUnit.capturing.incrementAndGet(); // disable temporarily capturing
      classes.forEach(c -> StatementBuilder.getClassInfo(c));
    } catch (AssertionError|ThreadDeath|StackOverflowError|OutOfMemoryError e) {
      throw e;
    } catch (Throwable t) {
      throw new AssertionError("failed to initialize schemas", t);
    } finally {
      HelenusJUnit.capturing.decrementAndGet(); // restore previous capturing setting
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
    final StatementCaptureList<T> cl
      = new StatementCaptureList<>(HelenusJUnit.getCallerInfo(), clazz);

    HelenusJUnit.captures.add(cl);
    return cl;
  }

  /**
   * Stops capturing object statements with all capture lists.
   *
   * @author paouelle
   *
   * @return this for chaining
   */
  public HelenusJUnit withoutCapture() {
    HelenusJUnit.captures.forEach(cl -> cl.stop());
    return this;
  }

  /**
   * Executes a command while disabling all capture lists.
   *
   * @author paouelle
   *
   * @param <E> the type of exceptions that can be thrown out
   *
   * @param  cmd the command to execute while capturing is inhibited
   * @return this for chaining
   * @throws E if thrown by the command
   */
  public <E extends Throwable> HelenusJUnit inhibitCapturing(ERunnable<E> cmd)
    throws E {
    try {
      HelenusJUnit.capturing.incrementAndGet(); // disable temporarily capturing
      cmd.run();
    } finally {
      HelenusJUnit.capturing.decrementAndGet(); // restore previous capturing setting
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
   * Registers a callback to be notified when a statement is sent to Cassandra.
   *
   * @author paouelle
   *
   * @param  consumer the consumer to register to be notified every time a
   *         statement is sent to Cassandra
   * @return this for chaining
   */
  public HelenusJUnit whenSent(Consumer<GenericStatement<?, ?>> consumer) {
    HelenusJUnit.sent.add(consumer);
    return this;
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
     * Handles requests to a root or type entity defined as a base class for
     * other type entities by forcing all sub-types to be cached properly so
     * that all required initial objects be inserted in the DB prior to
     * processing a request to the root.
     *
     * @author paouelle
     *
     * @param cinfo the non-<code>null</code> class info to check if it is
     *        represents a base class of type entities
     */
    private <T> void handleBaseClassInfo(ClassInfo<T> cinfo) {
      if (HelenusJUnit.recursing.get() > 0) {
        // don't bother if we are recursing from within HelenusJUnit
        return;
      }
      if (cinfo instanceof RootClassInfoImpl) {
        ((RootClassInfoImpl<T>)cinfo).types()
          .forEachOrdered(t -> getClassInfoImpl(t.getObjectClass()));
      } else if (cinfo instanceof TypeClassInfoImpl) {
        // make sure to cache all sub-types if any
        final TypeClassInfoImpl<T> tcinfo = (TypeClassInfoImpl<T>)cinfo;

        tcinfo.getRoot().types()
          .filter(t -> !cinfo.getObjectClass().equals(t.getObjectClass())) // skip us
          .filter(t -> cinfo.getObjectClass().isAssignableFrom(t.getObjectClass()))
          .forEachOrdered(t -> getClassInfoImpl(t.getObjectClass()));
      } else if (cinfo instanceof UDTRootClassInfoImpl) {
        ((UDTRootClassInfoImpl<T>)cinfo).types()
          .forEachOrdered(t -> getClassInfoImpl(t.getObjectClass()));
      } else if (cinfo instanceof UDTTypeClassInfoImpl) {
        // make sure to cache all sub-types if any
        final UDTTypeClassInfoImpl<T> tcinfo = (UDTTypeClassInfoImpl<T>)cinfo;

        tcinfo.getRoot().types()
          .filter(t -> !cinfo.getObjectClass().equals(t.getObjectClass())) // skip us
          .filter(t -> cinfo.getObjectClass().isAssignableFrom(t.getObjectClass()))
          .forEachOrdered(t -> getClassInfoImpl(t.getObjectClass()));
      }
    }

    /**
     * Clears the cache of pojo class info.
     *
     * @author paouelle
     */
    protected void clearCache() {
      super.classInfoCache.clear();
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.StatementManagerImpl#executing(org.helenus.driver.impl.StatementImpl)
     */
    @Override
    protected void executing(StatementImpl<?, ?, ?> statement) {
      if (statement.isEnabled()) {
        if ((HelenusJUnit.capturing.get() == 0)
            || HelenusJUnit.captures.isEmpty()) { // capturing
          HelenusJUnit.captures.forEach(l -> l.executing(statement));
        }
      }
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.StatementManagerImpl#sent(org.helenus.driver.impl.StatementImpl, com.datastax.driver.core.ResultSetFuture)
     */
    @Override
    protected ResultSetFuture sent(
      StatementImpl<?, ?, ?> statement, ResultSetFuture future
    ) {
      if (HelenusJUnit.capturing.get() == 0) { // capturing
        HelenusJUnit.sent.forEach(c -> c.accept(statement));
      }
      return future;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.StatementManagerImpl#insert(java.lang.Object)
     */
    @Override
    protected <T> Insert.Builder<T> insert(T object) {
      if (object != null) {
        handleBaseClassInfo(getClassInfo(object.getClass()));
      }
      try {
        HelenusJUnit.recursing.incrementAndGet(); // enable temporarily recursing
        return super.insert(object);
      } finally {
        HelenusJUnit.recursing.decrementAndGet(); // restore recursing previous setting
      }
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.StatementManagerImpl#update(java.lang.Object)
     */
    @Override
    protected <T> Update<T> update(T object) {
      if (object != null) {
        handleBaseClassInfo(getClassInfo(object.getClass()));
      }
      try {
        HelenusJUnit.recursing.incrementAndGet(); // enable temporarily recursing
        return super.update(object);
      } finally {
        HelenusJUnit.recursing.decrementAndGet(); // restore recursing previous setting
      }
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.StatementManagerImpl#update(java.lang.Object, java.lang.String[])
     */
    @Override
    protected <T> Update<T> update(T object, String... tables) {
      if (object != null) {
        handleBaseClassInfo(getClassInfo(object.getClass()));
      }
      try {
        HelenusJUnit.recursing.incrementAndGet(); // enable temporarily recursing
        return super.update(object, tables);
      } finally {
        HelenusJUnit.recursing.decrementAndGet(); // restore recursing previous setting
      }
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.StatementManagerImpl#delete(java.lang.Object, java.lang.String[])
     */
    @Override
    protected <T> Delete.Builder<T> delete(
      T object, String... columns
    ) {
      if (object != null) {
        handleBaseClassInfo(getClassInfo(object.getClass()));
      }
      try {
        HelenusJUnit.recursing.incrementAndGet(); // enable temporarily recursing
        return super.delete(object, columns);
      } finally {
        HelenusJUnit.recursing.decrementAndGet(); // restore recursing previous setting
      }
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.StatementManagerImpl#delete(java.lang.Object)
     */
    @Override
    protected <T> Delete.Selection<T> delete(T object) {
      if (object != null) {
        handleBaseClassInfo(getClassInfo(object.getClass()));
      }
      try {
        HelenusJUnit.recursing.incrementAndGet(); // enable temporarily recursing
        return super.delete(object);
      } finally {
        HelenusJUnit.recursing.decrementAndGet(); // restore recursing previous setting
      }
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.StatementManagerImpl#delete(java.lang.Class, java.lang.String[])
     */
    @Override
    protected <T> Delete.Builder<T> delete(
      Class<T> clazz, String... columns
    ) {
      if (clazz != null) {
        handleBaseClassInfo(getClassInfo(clazz));
      }
      try {
        HelenusJUnit.recursing.incrementAndGet(); // enable temporarily recursing
        return super.delete(clazz, columns);
      } finally {
        HelenusJUnit.recursing.decrementAndGet(); // restore recursing previous setting
      }
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.StatementManagerImpl#delete(java.lang.Class)
     */
    @Override
    protected <T> Delete.Selection<T> delete(Class<T> clazz) {
      if (clazz != null) {
        handleBaseClassInfo(getClassInfo(clazz));
      }
      try {
        HelenusJUnit.recursing.incrementAndGet(); // enable temporarily recursing
        return super.delete(clazz);
      } finally {
        HelenusJUnit.recursing.decrementAndGet(); // restore recursing previous setting
      }
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.StatementManagerImpl#createKeyspace(java.lang.Class)
     */
    @Override
    protected <T> CreateKeyspace<T> createKeyspace(Class<T> clazz) {
      if (clazz != null) {
        handleBaseClassInfo(getClassInfo(clazz));
      }
      try {
        HelenusJUnit.recursing.incrementAndGet(); // enable temporarily recursing
        return super.createKeyspace(clazz);
      } finally {
        HelenusJUnit.recursing.decrementAndGet(); // restore recursing previous setting
      }
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.StatementManagerImpl#createType(java.lang.Class)
     */
    @Override
    protected <T> CreateType<T> createType(Class<T> clazz) {
      if (clazz != null) {
        handleBaseClassInfo(getClassInfo(clazz));
      }
      try {
        HelenusJUnit.recursing.incrementAndGet(); // enable temporarily recursing
        return super.createType(clazz);
      } finally {
        HelenusJUnit.recursing.decrementAndGet(); // restore recursing previous setting
      }
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.StatementManagerImpl#createTable(java.lang.Class)
     */
    @Override
    protected <T> CreateTable<T> createTable(Class<T> clazz) {
      if (clazz != null) {
        handleBaseClassInfo(getClassInfo(clazz));
      }
      try {
        HelenusJUnit.recursing.incrementAndGet(); // enable temporarily recursing
        return super.createTable(clazz);
      } finally {
        HelenusJUnit.recursing.decrementAndGet(); // restore recursing previous setting
      }
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.StatementManagerImpl#createTable(java.lang.Class, java.lang.String[])
     */
    @Override
    protected <T> CreateTable<T> createTable(Class<T> clazz, String... tables) {
      if (clazz != null) {
        handleBaseClassInfo(getClassInfo(clazz));
      }
      try {
        HelenusJUnit.recursing.incrementAndGet(); // enable temporarily recursing
        return super.createTable(clazz, tables);
      } finally {
        HelenusJUnit.recursing.decrementAndGet(); // restore recursing previous setting
      }
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.StatementManagerImpl#createIndex(java.lang.Class)
     */
    @Override
    protected <T> CreateIndex.Builder<T> createIndex(Class<T> clazz) {
      if (clazz != null) {
        handleBaseClassInfo(getClassInfo(clazz));
      }
      try {
        HelenusJUnit.recursing.incrementAndGet(); // enable temporarily recursing
        return super.createIndex(clazz);
      } finally {
        HelenusJUnit.recursing.decrementAndGet(); // restore recursing previous setting
      }
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.StatementManagerImpl#createSchema(java.lang.Class)
     */
    @Override
    protected <T> CreateSchema<T> createSchema(Class<T> clazz) {
      if (clazz != null) {
        handleBaseClassInfo(getClassInfo(clazz));
      }
      try {
        HelenusJUnit.recursing.incrementAndGet(); // enable temporarily recursing
        return super.createSchema(clazz);
      } finally {
        HelenusJUnit.recursing.decrementAndGet(); // restore recursing previous setting
      }
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.StatementManagerImpl#createSchemas(java.lang.String[])
     */
    @Override
    protected CreateSchemas createSchemas(String[] pkgs) {
      final CreateSchemas c;

      try {
        HelenusJUnit.recursing.incrementAndGet(); // enable temporarily recursing
        c = super.createSchemas(pkgs);
      } finally {
        HelenusJUnit.recursing.decrementAndGet(); // restore recursing previous setting
      }
      c.classInfos().forEachOrdered(cinfo -> handleBaseClassInfo(cinfo));
      return c;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.StatementManagerImpl#createMatchingSchemas(java.lang.String[])
     */
    @Override
    protected CreateSchemas createMatchingSchemas(String[] pkgs) {
      final CreateSchemas c;

      try {
        HelenusJUnit.recursing.incrementAndGet(); // enable temporarily recursing
        c = super.createMatchingSchemas(pkgs);
      } finally {
        HelenusJUnit.recursing.decrementAndGet(); // restore recursing previous setting
      }
      c.classInfos().forEachOrdered(cinfo -> handleBaseClassInfo(cinfo));
      return c;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.StatementManagerImpl#alterSchema(java.lang.Class)
     */
    @Override
    protected <T> AlterSchema<T> alterSchema(Class<T> clazz) {
      if (clazz != null) {
        handleBaseClassInfo(getClassInfo(clazz));
      }
      try {
        HelenusJUnit.recursing.incrementAndGet(); // enable temporarily recursing
        return super.alterSchema(clazz);
      } finally {
        HelenusJUnit.recursing.decrementAndGet(); // restore recursing previous setting
      }
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.StatementManagerImpl#alterSchemas(java.lang.String[])
     */
    @Override
    protected AlterSchemas alterSchemas(String[] pkgs) {
      final AlterSchemas a;

      try {
        HelenusJUnit.recursing.incrementAndGet(); // enable temporarily recursing
        a = super.alterSchemas(pkgs);
      } finally {
        HelenusJUnit.recursing.decrementAndGet(); // restore recursing previous setting
      }
      a.classInfos().forEachOrdered(cinfo -> handleBaseClassInfo(cinfo));
      return a;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.StatementManagerImpl#alterMatchingSchemas(java.lang.String[])
     */
    @Override
    protected AlterSchemas alterMatchingSchemas(String[] pkgs) {
      final AlterSchemas a;

      try {
        HelenusJUnit.recursing.incrementAndGet(); // enable temporarily recursing
        a = super.alterMatchingSchemas(pkgs);
      } finally {
        HelenusJUnit.recursing.decrementAndGet(); // restore recursing previous setting
      }
      a.classInfos().forEachOrdered(cinfo -> handleBaseClassInfo(cinfo));
      return a;
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.StatementManagerImpl#truncate(java.lang.Class)
     */
    @Override
    protected <T> Truncate<T> truncate(Class<T> clazz) {
      if (clazz != null) {
        handleBaseClassInfo(getClassInfo(clazz));
      }
      try {
        HelenusJUnit.recursing.incrementAndGet(); // enable temporarily recursing
        return super.truncate(clazz);
      } finally {
        HelenusJUnit.recursing.decrementAndGet(); // restore recursing previous setting
      }
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.StatementManagerImpl#truncate(java.lang.Class, java.lang.String[])
     */
    @Override
    protected <T> Truncate<T> truncate(Class<T> clazz, String... tables) {
      if (clazz != null) {
        handleBaseClassInfo(getClassInfo(clazz));
      }
      try {
        HelenusJUnit.recursing.incrementAndGet(); // enable temporarily recursing
        return super.truncate(clazz, tables);
      } finally {
        HelenusJUnit.recursing.decrementAndGet(); // restore recursing previous setting
      }
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.StatementManagerImpl#select(java.lang.Class, java.lang.CharSequence[])
     */
    @Override
    protected <T> Select.Builder<T> select(Class<T> clazz, CharSequence... columns) {
      if (clazz != null) {
        handleBaseClassInfo(getClassInfo(clazz));
      }
      try {
        HelenusJUnit.recursing.incrementAndGet(); // enable temporarily recursing
        return super.select(clazz, columns);
      } finally {
        HelenusJUnit.recursing.decrementAndGet(); // restore recursing previous setting
      }
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.StatementManagerImpl#select(java.lang.Class)
     */
    @Override
    protected <T> Select.Selection<T> select(Class<T> clazz) {
      if (clazz != null) {
        handleBaseClassInfo(getClassInfo(clazz));
      }
      try {
        HelenusJUnit.recursing.incrementAndGet(); // enable temporarily recursing
        return super.select(clazz);
      } finally {
        HelenusJUnit.recursing.decrementAndGet(); // restore recursing previous setting
      }
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.StatementManagerImpl#selectFrom(org.helenus.driver.info.TableInfo, java.lang.CharSequence[])
     */
    @Override
    protected <T> Select<T> selectFrom(
      TableInfo<T> table, CharSequence... columns
    ) {
      if (table != null) {
        handleBaseClassInfo(table.getClassInfo());
      }
      try {
        HelenusJUnit.recursing.incrementAndGet(); // enable temporarily recursing
        return super.selectFrom(table, columns);
      } finally {
        HelenusJUnit.recursing.decrementAndGet(); // restore recursing previous setting
      }
    }

    /**
     * {@inheritDoc}
     *
     * @author paouelle
     *
     * @see org.helenus.driver.impl.StatementManagerImpl#selectFrom(org.helenus.driver.info.TableInfo)
     */
    @Override
    protected <T> Select.TableSelection<T> selectFrom(TableInfo<T> table) {
      if (table != null) {
        handleBaseClassInfo(table.getClassInfo());
      }
      try {
        HelenusJUnit.recursing.incrementAndGet(); // enable temporarily recursing
        return super.selectFrom(table);
      } finally {
        HelenusJUnit.recursing.decrementAndGet(); // restore recursing previous setting
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
    @SuppressWarnings("unchecked")
    @Override
    public <T> ClassInfoImpl<T> getClassInfoImpl(Class<T> clazz) {
      try {
        HelenusJUnit.recursing.incrementAndGet(); // enable temporarily recursing
        // first check if we have already loaded it in previous test cases and if so, reset the schema for it
        ClassInfoImpl<T> cinfo
          = (ClassInfoImpl<T>)HelenusJUnit.fromPreviousTestsCacheInfoCache.remove(clazz);

        if (cinfo != null) {
          if (cinfo instanceof TypeClassInfoImpl) {
            // force root to be re-cached first
            getClassInfoImpl(((TypeClassInfoImpl<T>)cinfo).getRoot().getObjectClass());
          }
          // first truncate all loaded pojo tables and re-insert any schema defined
          // initial objects
          HelenusJUnit.resetSchema0(cinfo, null);
          super.cacheClassInfoIfAbsent(cinfo);
          return cinfo;
        } // else - check if it is already cached
        cinfo = (ClassInfoImpl<T>)super.classInfoCache.get(clazz);
        if (cinfo != null) {
          // this will be the case if we already retrieved another sub-type for
          // a root in which case we would have already retrieved the root and
          // removed it from the previous tests cache
          return cinfo;
        }
        // if we get here then the cinfo was not loaded in previous tests
        // go to the super's implementation which will take care of calling back
        // this method for the root if this is for a type entity
        // load the schemas for the pojo if required
        return HelenusJUnit.createSchema0(
          super.getClassInfoImpl(clazz) // force generation of the class info if needed
        );
      } finally {
        HelenusJUnit.recursing.decrementAndGet(); // restore recursing previous setting
      }
    }
  }
}

/**
 * The <code>SchemaFuture</code> class defines a specific future used when
 * creating or resetting schemas. It is reentrant from the owner's thread.
 *
 * @copyright 2016-2016 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Feb 1, 2016 - paouelle - Creation
 *
 * @since 1.0
 */
class SchemaFuture {
  /**
   * Holds the the reference to the thread that owns this future.
   *
   * @author paouelle
   */
  private final Thread owner;

  /**
   * Holds a flag indicating if we are creating or reseting the schema.
   *
   * @author paouelle
   */
  private final boolean creating;

  /**
   * Holds the class of the POJO we are creating or reseting the schema for.
   *
   * @author paouelle
   */
  private final Class<?> clazz;

  /**
   * Holds a flag indicating if the schema was created or reseted.
   *
   * @author paouelle
   */
  private volatile boolean done = false;

  /**
   * Holds the error that occurred if the schema creation/reset failed.
   *
   * @author paouelle
   */
  private Throwable error = null;

  /**
   * Instantiates a new <code>SchemaFuture</code> object.
   *
   * @author paouelle
   *
   * @param creating <code>true</code> if we are creating the schema;
   *        <code>false</code> if we are reseting it
   * @param clazz the non-<code>null</code> POJO class we are creating or
   *        reseting the schema for
   */
  SchemaFuture(boolean creating, Class<?> clazz) {
    this.owner = Thread.currentThread();
    this.creating = creating;
    this.clazz = clazz;
  }

  /**
   * Waits for the completion of this task.
   * <p>
   * <i>Note:</i> This method will return right away before the completion of
   * the schema creation/reset if the current thread is the owner of this future.
   * This allows recursive calls to succeed knowing the schema will be properly
   * created/reset upon finishing the recursion.
   *
   * @author paouelle
   *
   * @throws Error if the schema creation/reset failed
   */
  public void waitForCompletion() {
    if (owner != Thread.currentThread()) {
      synchronized (this) {
        boolean interrupted = false;

        try {
          while (!done) {
            try {
              this.wait();
            } catch (InterruptedException e) {
              interrupted = true;
            }
          }
        } finally {
          if (interrupted) { // propagate interruptions
            Thread.currentThread().interrupt();
          }
        }
        if (error instanceof Error) {
          throw (Error)error;
        } else if (error instanceof RuntimeException) {
          throw (RuntimeException)error;
        } else {
          throw new AssertionError(
            "failed to "
            + (creating ? "create" : "reset")
            + " for "
            + clazz.getSimpleName(),
            error
          );
        }
      }
    } // else - the current thread is the owner, therefore assumes it is done!
  }

  /**
   * Notifies of the completion of the schema creation or reset.
   *
   * @author paouelle
   */
  public void completed() {
    if (!done) {
      synchronized (this) {
        this.done = true;
        this.notifyAll(); // wake everybody!
      }
    }
  }

  /**
   * Notifies of the completion of the schema creation or reset with error.
   *
   * @author paouelle
   *
   * @param  t the error that occurred
   * @return never returns anything
   * @throws Error <code>t</code> if it is an error otherwise AssertionError
   *         with <code>t</code> as the cause
   */
  public Error failed(Throwable t) throws Error {
    if (!done) {
      synchronized (this) {
        this.done = true;
        this.error = t;
        this.notifyAll(); // wake everybody!
      }
    }
    if (t instanceof Error) {
      throw (Error)t;
    }
    throw new AssertionError(
      "failed to "
      + (creating ? "create" : "reset")
      + " for "
      + clazz.getSimpleName(),
      t
    );
  }
}
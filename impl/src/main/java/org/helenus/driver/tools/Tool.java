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
package org.helenus.driver.tools;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.codec.binary.Hex;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.module.jsonSchema.JsonSchema;

import org.helenus.commons.cli.RunnableFirstOption;
import org.helenus.commons.cli.RunnableOption;
import org.helenus.commons.collections.DirectedGraph;
import org.helenus.commons.collections.GraphUtils;
import org.helenus.commons.collections.graph.ConcurrentHashDirectedGraph;
import org.helenus.commons.lang3.IllegalCycleException;
import org.helenus.commons.lang3.SerializationUtils;
import org.helenus.commons.lang3.reflect.ReflectionUtils;
import org.helenus.driver.AlterSchemas;
import org.helenus.driver.Batch;
import org.helenus.driver.CreateSchemas;
import org.helenus.driver.GenericStatement;
import org.helenus.driver.Group;
import org.helenus.driver.ObjectClassStatement;
import org.helenus.driver.ObjectSet;
import org.helenus.driver.Sequence;
import org.helenus.driver.StatementBuilder;
import org.helenus.driver.impl.StatementManagerImpl;
import org.helenus.driver.persistence.InitialObjects;
import org.helenus.jackson.jsonSchema.customProperties.JsonAnnotationSchemaFactoryWrapper;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;

/**
 * The <code>Tool</code> class defines a command line tool that can be used
 * along with the driver.
 *
 * @copyright 2015-2015 The Helenus Driver Project Authors
 *
 * @author  The Helenus Driver Project Authors
 * @version 1 - Jan 19, 2015 - paouelle - Creation
 *
 * @since 2.0
 */
public class Tool {
  /**
   * Holds the statement manager.
   *
   * @author paouelle
   */
  private static StatementManagerImpl mgr;

  /**
   * Holds the verbose flag.
   *
   * @author paouelle
   */
  private static boolean vflag = false;

  /**
   * Holds the full verbose flag.
   *
   * @author paouelle
   */
  private static boolean fvflag = false;

  /**
   * Holds the schemas creation action.
   *
   * @author paouelle
   */
  @SuppressWarnings("serial")
  private final static RunnableOption schemas
    = new RunnableOption(
        "s",
        "schemas",
        false,
        "to define schemas for the specified pojo classes and/or packages (separated with :)"
      ) {
        {
          setArgs(Option.UNLIMITED_VALUES);
          setArgName("classes-packages");
          setValueSeparator(':');
        }
        @SuppressWarnings("synthetic-access")
        @Override
        public void run(CommandLine line) throws Exception {
          Tool.createSchemas(line);
        }
      };

  /**
   * Holds the objects creation action.
   *
   * @author paouelle
   */
  @SuppressWarnings("serial")
  private final static RunnableOption objects
    = new RunnableOption(
        "o",
        "objects",
        false,
        "to insert objects using the specified creator classes and/or packages (separated with :)"
      ) {
        {
          setArgs(Option.UNLIMITED_VALUES);
          setArgName("classes-packages");
          setValueSeparator(':');
        }
        @SuppressWarnings("synthetic-access")
        @Override
        public void run(CommandLine line) throws Exception {
          Tool.insertObjects(line);
        }
      };

  /**
   * Holds the json schemas creation action.
   *
   * @author paouelle
   */
  @SuppressWarnings("serial")
  private final static RunnableOption jsons
    = new RunnableOption(
        "j",
        "jsons",
        false,
        "to write json schemas to disk for the specified pojo classes and/or packages (separated with :)"
      ) {
        {
          setArgs(Option.UNLIMITED_VALUES);
          setArgName("classes-packages");
          setValueSeparator(':');
        }
        @SuppressWarnings("synthetic-access")
        @Override
        public void run(CommandLine line) throws Exception {
          Tool.createJsonSchemas(line);
        }
      };

  /**
   * Holds the json schemas view option.
   *
   * @author paouelle
   */
  @SuppressWarnings("static-access")
  private final static Option jsonview = Option.builder()
    .longOpt("view")
    .desc("to specify the Json view to use when generating the schemas (defaults to none)")
    .hasArg()
    .argName("class")
    .build();

  /**
   * Holds the blob deserialization action.
   *
   * @author paouelle
   */
  @SuppressWarnings("serial")
  private final static RunnableFirstOption deserialize
    = new RunnableFirstOption("d", "deserialize", true, "to deserialize a blob") {
        {
          setArgName("blob");
        }
        @Override
        public void run(CommandLine line) throws Exception {
          String s = line.getOptionValue(getLongOpt());

          if (s.startsWith("0x") || s.startsWith("0X")) {
            s = s.substring(2);
          }
          final byte[] blob = Hex.decodeHex(s.toCharArray());

          if ((blob.length >= 2)
              && (blob[0] == (byte)0xac)
              && (blob[1] == (byte)0xed)) { // serialized blob
            final Object obj
              = org.apache.commons.lang3.SerializationUtils.deserialize(blob);

            System.out.println(">> " + obj.getClass());
            System.out.println(">> " + obj);
          } else if ((blob.length >= 4)
              && (blob[0] == (byte)0xca)
              && (blob[1] == (byte)0xfe)
              && (blob[2] == (byte)0xba)
              && (blob[3] == (byte)0xbe)) { // compiled class
            // see http://docs.oracle.com/javase/specs/jvms/se8/html/jvms-4.html
            try (
              final ByteArrayInputStream bais = new ByteArrayInputStream(blob);
              final DataInputStream dis = new DataInputStream(bais);
            ) {
              dis.readLong(); // skip header and class version
              final int cpcnt = (dis.readShort() &0xffff) - 1;
              final int[] classes = new int[cpcnt];
              final String[] strings = new String[cpcnt];

              for (int i = 0; i < cpcnt; i++) {
                final int t = dis.read();

                if (t == 7) { // u2
                  classes[i] = dis.readShort() & 0xffff;
                } else if (t == 1) { // utf8
                  strings[i] = dis.readUTF(); // u2 + x * u1
                } else if ((t == 5) || (t == 6)) { // u8
                  dis.readLong();
                } else if ((t == 8) || (t == 16)) { // u2
                  dis.readShort();
                } else if (t == 15) { // u3
                  dis.read();
                  dis.readShort();
                } else { // u4 - t == 9, 10, 11, 3, 4, 12, 18
                  dis.readInt();
                }
              }
              dis.readShort(); // skip access flags
              System.out.println(
                ">> compiled class "
                + strings[classes[(dis.readShort() & 0xffff) - 1] - 1].replace(
                    '/', '.'
                  )
              );
            }
          } else { // assume compressed serialized blob
            final Object obj
              = SerializationUtils.decompressAndDeserialize(blob);

            System.out.println(">> " + obj.getClass());
            System.out.println(">> " + obj);
          }
          System.exit(0);
        }
      };

  /**
   * Holds the help option.
   *
   * @author paouelle
   */
  @SuppressWarnings("serial")
  private final static RunnableOption help
    = new RunnableOption("?", "help", false, "to print this message") {
        @SuppressWarnings("synthetic-access")
        @Override
        public void run(CommandLine line) {
          // automatically generate the help statement
          final HelpFormatter formatter = new HelpFormatter();

          formatter.printHelp(120, Tool.class.getSimpleName(), "Cassandra Client Tool", Tool.options, null, true);
        }
      };

  /**
   * Holds the verbose option.
   *
   * @author paouelle
   */
  @SuppressWarnings("serial")
  private final static RunnableOption verbose
    = new RunnableOption("v", "verbose", false, "to enable verbose output") {
      @SuppressWarnings("synthetic-access")
      @Override
      public void run(CommandLine line) {
        Tool.vflag = true;
      }
    };

  /**
   * Holds the full_verbose option.
   *
   * @author paouelle
   */
  @SuppressWarnings("serial")
  private final static RunnableOption full_verbose
    = new RunnableOption(null, "full", false, "to enable full verbose output") {
      @SuppressWarnings("synthetic-access")
      @Override
      public void run(CommandLine line) {
        Tool.vflag = true;
        Tool.fvflag = true;
      }
    };

  /**
   * Holds the trace option.
   *
   * @author paouelle
   */
  @SuppressWarnings("serial")
  private final static RunnableOption trace
    = new RunnableOption("t", "trace", false, "to enable trace output") {
      @SuppressWarnings("synthetic-access")
      @Override
      public void run(CommandLine line) {
        Tool.setRootLogLevel(Level.TRACE);
      }
    };

  /**
   * Holds the filtered classes option.
   *
   * @author paouelle
   */
  @SuppressWarnings("static-access")
  private final static Option filters = Option.builder("f")
    .longOpt("filters")
    .desc("to specify entity filter classes to register with the driver (separated with :)")
    .valueSeparator(':')
    .hasArgs()
    .argName("classes")
    .build();

  /**
   * Holds the Cassandra server option.
   *
   * @author paouelle
   */
  @SuppressWarnings("static-access")
  private final static Option server = Option.builder()
    .longOpt("server")
    .desc("to specify the server address for Cassandra (defaults to localhost)")
    .hasArg()
    .argName("host")
    .build();

  /**
   * Holds the Cassandra port option.
   *
   * @author paouelle
   */
  @SuppressWarnings("static-access")
  private final static Option port = Option.builder()
    .longOpt("port")
    .desc("to specify the port number for Cassandra (defaults to 9042)")
    .hasArg()
    .argName("number")
    .build();

  /**
   * Holds the option indicating if only matches should be considered otherwise
   * all of them are created.
   *
   * @author paouelle
   */
  @SuppressWarnings("static-access")
  private final static Option matches_only = Option.builder()
    .longOpt("matches-only")
    .desc("to specify that only keyspace that matches the specified suffixes should be created")
    .build();

  /**
   * Holds the alter option.
   *
   * @author paouelle
   */
  @SuppressWarnings("static-access")
  private final static Option alter = Option.builder()
    .longOpt("alter")
    .desc("to alter the existing schemas instead of creating it if it doesn't exist")
    .build();

  /**
   * Holds the option indicating if dependent creators should not be considered
   * otherwise all of them are created.
   *
   * @author paouelle
   */
  @SuppressWarnings("static-access")
  private final static Option no_dependents = Option.builder()
    .longOpt("no-dependents")
    .desc("to specify that dependent creators should not be considered when creating objects")
    .build();

  /**
   * Holds the Cassandra default replication factor option.
   *
   * @author paouelle
   */
  @SuppressWarnings("static-access")
  private final static Option replicationFactor = Option.builder()
    .longOpt("replication-factor")
    .desc("to specify the default replication factor to use with the simple placement strategy when creating keyspaces (defaults to 2)")
    .hasArg()
    .argName("value")
    .build();

  /**
   * Holds the Cassandra default data centers option.
   *
   * @author paouelle
   */
  @SuppressWarnings("static-access")
  private final static Option dataCenters = Option.builder("D")
    .desc("to specify the default data centers and number of replicas to use with the network topology strategy when creating keyspaces (e.g. -Ddatacenter1=2)")
    .numberOfArgs(2)
    .argName("datacenter=replicas")
    .valueSeparator()
    .build();

  /**
   * Holds the suffix options.
   *
   * @author paouelle
   */
  @SuppressWarnings("static-access")
  private final static Option suffixes = Option.builder("S")
    .desc("to specify value(s) for suffix types (e.g. -Scustomer=acme, -Sregion=emea)")
    .numberOfArgs(2)
    .argName("type=value")
    .valueSeparator()
    .build();

  /**
   * Holds the Cassandra server option.
   *
   * @author paouelle
   */
  @SuppressWarnings("static-access")
  private final static Option output = Option.builder()
    .longOpt("output")
    .desc("to specify the output directory (defaults to current directory)")
    .hasArg()
    .argName("output")
    .build();

  /**
   * Holds the command-line options definition.
   *
   * @author paouelle
   */
  private final static Options options
    = (new Options()
       .addOption(Tool.schemas)
       .addOption(Tool.objects)
       .addOption(Tool.jsons)
       .addOption(Tool.jsonview)
       .addOption(Tool.suffixes)
       .addOption(Tool.server)
       .addOption(Tool.port)
       .addOption(Tool.filters)
       .addOption(Tool.deserialize)
       .addOption(Tool.matches_only)
       .addOption(Tool.alter)
       .addOption(Tool.no_dependents)
       .addOption(Tool.replicationFactor)
       .addOption(Tool.dataCenters)
       .addOption(Tool.output)
       .addOption(Tool.verbose)
       .addOption(Tool.full_verbose)
       .addOption(Tool.trace)
       .addOption(Tool.help)
      );

  /**
   * Gets the Json view name from the specified class.
   *
   * @author paouelle
   *
   * @param  clazz the class for which to get a Json view name
   * @return the corresponding Json view name
   */
  private static String getViewName(Class<?> clazz) {
    final String cname = clazz.getSimpleName();
    final Class<?> pclazz = clazz.getDeclaringClass();

     if (pclazz != null) {
       return pclazz.getSimpleName() + "." + cname;
     }
     return cname;
  }

  /**
   * Executes the specified CQL statement.
   *
   * @author paouelle
   *
   * @param <T> the type of POJO for the statement
   *
   * @param  s the CQL statement to execute
   * @return the result set from the execution
   */
  @SuppressWarnings("unused")
  private static <T> ObjectSet<T> executeCQL(ObjectClassStatement<T> s) {
    s.setConsistencyLevel(ConsistencyLevel.ONE);
    if (Tool.vflag) {
      System.out.println(
        Tool.class.getSimpleName()
        + ": CQL -> "
        + s.getQueryString()
      );
    }
    return s.execute();
  }

  /**
   * Executes the specified CQL statement.
   *
   * @author paouelle
   *
   * @param s the CQL statement to execute
   */
  private static void executeCQL(GenericStatement<?, ?> s) {
    s.setConsistencyLevel(ConsistencyLevel.ONE);
    s.setSerialConsistencyLevel(ConsistencyLevel.SERIAL);
    if (Tool.vflag) {
      final String query = s.getQueryString();

      if (query == null) {
        System.out.println(Tool.class.getSimpleName() + ": CQL -> null");
      } else if (fvflag || (query.length() < 2048)) {
        System.out.println(
          Tool.class.getSimpleName()
          + ": CQL -> "
          + query
        );
      } else if (s instanceof Batch) {
        System.out.println(
          Tool.class.getSimpleName()
          + ": CQL -> "
          + query.substring(0, 2032)
          + " ... APPLY BATCH;"
        );
      } else if (s instanceof Sequence) {
        System.out.println(
          Tool.class.getSimpleName()
          + ": CQL -> "
          + query.substring(0, 2029)
          + " ... APPLY SEQUENCE;"
        );
      } else if (s instanceof Group) {
        System.out.println(
          Tool.class.getSimpleName()
          + ": CQL -> "
          + query.substring(0, 2032)
          + " ... APPLY GROUP;"
        );
      } else {
        System.out.println(
          Tool.class.getSimpleName()
          + ": CQL -> "
          + query.substring(0, 2044)
        );
      }
    }
    s.execute();
  }

  /**
   * Creates all defined schemas based on the provided set of package and/or
   * class names and options and add the statements to the specified sequence.
   *
   * @author paouelle
   *
   * @param  pkgs the set of packages and/or classes to create schemas for
   * @param  suffixes the map of provided suffix values
   * @param  matching whether or not to only create schemas for keyspaces that
   *         matches the specified set of suffixes
   * @param  alter whether to alter or create the schemas
   * @param  s the sequence where to add the generated statements
   * @throws LinkageError if the linkage fails for one of the specified entity
   *         class
   * @throws ExceptionInInitializerError if the initialization provoked by one
   *         of the specified entity class fails
   * @throws IllegalArgumentException if no pojos are found in any of the
   *         specified packages
   */
  private static void createSchemasFromPackagesOrClasses(
    String[] pkgs,
    Map<String, String> suffixes,
    boolean matching,
    boolean alter,
    Sequence s
  ) {
    if (alter) {
      final AlterSchemas cs
        = (matching
           ? StatementBuilder.alterMatchingSchemas(pkgs)
           : StatementBuilder.alterSchemas(pkgs));

      // pass all suffixes
      for (final Map.Entry<String, String> e: suffixes.entrySet()) {
        // register the suffix value with the corresponding suffix type
        cs.where(StatementBuilder.eq(e.getKey(), e.getValue()));
      }
      cs.classInfos().forEachOrdered(cinfo -> System.out.println(
        Tool.class.getSimpleName()
        + ": altering schema for "
        + cinfo.getObjectClass().getName()
      ));
      s.add(cs);
    } else {
      final CreateSchemas cs
        = (matching
            ? StatementBuilder.createMatchingSchemas(pkgs)
            : StatementBuilder.createSchemas(pkgs));

      cs.ifNotExists();
      // pass all suffixes
      for (final Map.Entry<String, String> e: suffixes.entrySet()) {
        // register the suffix value with the corresponding suffix type
        cs.where(StatementBuilder.eq(e.getKey(), e.getValue()));
      }
      cs.classInfos().forEachOrdered(cinfo -> System.out.println(
        Tool.class.getSimpleName()
        + ": creating schema for "
        + cinfo.getObjectClass().getName()
      ));
      s.add(cs);
    }
  }

  /**
   * Creates all defined schemas based on the provided command line information.
   *
   * @author paouelle
   *
   * @param  line the command line information
   * @throws Exception if an error occurs while creating schemas
   * @throws LinkageError if the linkage fails for one of the specified entity
   *         class
   * @throws ExceptionInInitializerError if the initialization provoked by one
   *         of the specified entity class fails
   * @throws IllegalArgumentException if no pojos are found in any of the
   *         specified packages
   */
  private static void createSchemas(CommandLine line) throws Exception {
    final String[] opts = line.getOptionValues(Tool.schemas.getLongOpt());
    @SuppressWarnings({"cast", "unchecked", "rawtypes"})
    final Map<String, String> suffixes
      = (Map<String, String>)(Map)line.getOptionProperties(Tool.suffixes.getOpt());
    final boolean matching = line.hasOption(Tool.matches_only.getLongOpt());
    final boolean alter = line.hasOption(Tool.alter.getLongOpt());
    final Sequence s = StatementBuilder.sequence();

    System.out.print(
      Tool.class.getSimpleName()
      + ": searching for schema definitions in "
      + Arrays.toString(opts)
    );
    if (!suffixes.isEmpty()) {
      System.out.print(
        " with "
        + (matching ? "matching " : "")
        + "suffixes "
        + suffixes
      );
    }
    System.out.println();
    Tool.createSchemasFromPackagesOrClasses(opts, suffixes, matching, alter, s);
    if (s.isEmpty() || (s.getQueryString() == null)) {
      System.out.println(
        Tool.class.getSimpleName()
        + ": no schemas found matching the specified criteria"
      );
    } else {
      executeCQL(s);
    }
  }

  /**
   * Creates all defined Json schemas based on the provided set of package and/or
   * class names and options and add the schemas to the specified map.
   *
   * @author paouelle
   *
   * @param  pkgs the set of packages and/or classes to create Json schemas for
   * @param  suffixes the map of provided suffix values
   * @param  matching whether or not to only create schemas for keyspaces that
   *         matches the specified set of suffixes
   * @param  schemas the map where to record the Json schema for the pojo classes
   *         found
   * @param  view the json view to use when generating the schemas
   * @throws LinkageError if the linkage fails for one of the specified entity
   *         class
   * @throws ExceptionInInitializerError if the initialization provoked by one
   *         of the specified entity class fails
   * @throws IllegalArgumentException if no pojos are found in any of the
   *         specified packages
   * @throws IOException if an I/O error occurs while generating the Json schemas
   */
  @SuppressWarnings("deprecation")
  private static void createJsonSchemasFromPackagesOrClasses(
    String[] pkgs,
    Map<String, String> suffixes,
    boolean matching,
    Map<Class<?>, JsonSchema> schemas,
    Class<?> view
  ) throws IOException {
    final Set<Class<?>> classes = new LinkedHashSet<>();

    for (final String pkg: pkgs) {
      final CreateSchemas cs
        = (matching
           ? StatementBuilder.createMatchingSchemas(pkg)
           : StatementBuilder.createSchemas(pkg));

      // pass all suffixes
      for (final Map.Entry<String, String> e: suffixes.entrySet()) {
        // register the suffix value with the corresponding suffix type
        cs.where(
          StatementBuilder.eq(e.getKey(), e.getValue())
        );
      }
      final Set<Class<?>> csclasses = cs.getObjectClasses();

      if (csclasses.isEmpty()) { // nothing found in helenus, fallback to reflection
        final Reflections reflections = new Reflections(pkgs, new SubTypesScanner(false));

        reflections.getAllTypes().stream()
          .map(n -> {
            try {
              return Class.forName(n);
            } catch (LinkageError|ClassNotFoundException ee) { // ignore
              return null;
            }
          })
          .filter(c -> c != null)
          .forEach(c -> classes.add(c));
        // make sure to also cover enums
        classes.addAll(reflections.getSubTypesOf(Enum.class));
      } else {
        classes.addAll(csclasses);
      }
    }
    for (final Class<?> c: classes) {
      System.out.println(
        Tool.class.getSimpleName()
        + ": creating Json schema for "
        + c.getName()
        + ((view != null) ? " with view '" + Tool.getViewName(view) + "'" : "")
      );
      final ObjectMapper m = new ObjectMapper();
      final JsonAnnotationSchemaFactoryWrapper visitor = new JsonAnnotationSchemaFactoryWrapper();

      if (view != null) {
        m.setConfig(m.getSerializationConfig().withView(view));
      }
      m.registerModules(new Jdk8Module(), new com.fasterxml.jackson.datatype.jsr310.JSR310Module());
      m.enable(SerializationFeature.INDENT_OUTPUT);
      m.acceptJsonFormatVisitor(m.constructType(c), visitor);
      schemas.put(c, visitor.finalSchemaWithTitle());
    }
  }

  /**
   * Creates all defined Json schemas based on the provided command line
   * information.
   *
   * @author paouelle
   *
   * @param  line the command line information
   * @throws Exception if an error occurs while creating schemas
   * @throws LinkageError if the linkage fails for one of the specified entity
   *         class
   * @throws ExceptionInInitializerError if the initialization provoked by one
   *         of the specified entity class fails
   * @throws IllegalArgumentException if no pojos are found in any of the
   *         specified packages
   */
  private static void createJsonSchemas(CommandLine line) throws Exception {
    final String[] opts = line.getOptionValues(Tool.jsons.getLongOpt());
    @SuppressWarnings({"cast", "unchecked", "rawtypes"})
    final Map<String, String> suffixes
      = (Map<String, String>)(Map)line.getOptionProperties(Tool.suffixes.getOpt());
    final boolean matching = line.hasOption(Tool.matches_only.getLongOpt());
    final Map<Class<?>, JsonSchema> schemas = new LinkedHashMap<>();
    final Class<?> view;

    if (line.hasOption(Tool.jsonview.getLongOpt())) {
      view = Class.forName(
        line.getOptionValue(Tool.jsonview.getLongOpt())
      );
    } else {
      view = null;
    }
    System.out.print(
      Tool.class.getSimpleName()
      + ": searching for Json schema definitions in "
      + Arrays.toString(opts)
    );
    if (!suffixes.isEmpty()) {
      System.out.print(
        " with "
        + (matching ? "matching " : "")
        + "suffixes "
        + suffixes
      );
    }
    System.out.println();
    Tool.createJsonSchemasFromPackagesOrClasses(opts, suffixes, matching, schemas, view);
    if (schemas.isEmpty()) {
      System.out.println(
        Tool.class.getSimpleName()
        + ": no Json schemas found matching the specified criteria"
      );
    } else {
      final String output = line.getOptionValue(
        Tool.output.getLongOpt(), "." // defaults to current directory
      );
      final File dir = new File(output);

      if (!dir.exists()) {
        dir.mkdirs();
      }
      org.apache.commons.lang3.Validate.isTrue(
        dir.isDirectory(),
        "not a directory: %s", dir
      );
      final ObjectMapper m = new ObjectMapper();

      m.enable(SerializationFeature.INDENT_OUTPUT);
      for (final Map.Entry<Class<?>, JsonSchema> e: schemas.entrySet()) {
        m.writeValue(new File(dir, e.getKey().getName() + ".json"), e.getValue());
        //System.out.println(s.getType() + " = " + m.writeValueAsString(s));
      }
    }
  }

  /**
   * Gets the initial objects to insert using the specified initial method and
   * suffixes
   *
   * @author paouelle
   *
   * @param  initial a non-<code>null</code> initial method to retrieve objects with
   * @param  suffixes the non-<code>null</code> map of suffixes configured
   * @return a non-<code>null</code> collection of the initial objects to insert in the table
   */
  @SuppressWarnings("unchecked")
  private static Collection<?> getInitialObjects(
    Method initial, Map<String, String> suffixes
  ) {
    try {
      final Object ret;

      if (initial.getParameterCount() == 0) {
        ret = initial.invoke(null);
      } else {
        ret = initial.invoke(null, suffixes);
      }
      if (ret == null) {
        return Collections.emptyList();
      }
      // validate the return type is either an array, a collection, or a stream
      final Class<?> type = initial.getReturnType();

      if (type.isArray()) {
        final int length = Array.getLength(ret);
        final List<Object> objects = new ArrayList<>(length);

        for (int i = 0; i < length; i++) {
          objects.add(Array.get(ret, i));
        }
        return objects;
      } else if (ret instanceof Collection) {
        return (Collection<?>)ret;
      } else if (ret instanceof Stream) {
        return ((Stream<Object>)ret).collect(Collectors.toList());
      } else if (ret instanceof Iterator) {
        final List<Object> objects = new ArrayList<>(32);

        for (final Iterator<?> i = (Iterator<?>)ret; i.hasNext(); ) {
         objects.add(i.next());
        }
        return objects;
      } else if (ret instanceof Enumeration<?>) {
        final List<Object> objects = new ArrayList<>(32);

        for (final Enumeration<?> e = (Enumeration<?>)ret; e.hasMoreElements(); ) {
          objects.add(e.nextElement());
        }
        return objects;
      } else if (ret instanceof Iterable) {
        final List<Object> objects = new ArrayList<>(32);

        for (final Iterator<?> i = ((Iterable<?>)ret).iterator(); i.hasNext(); ) {
          objects.add(i.next());
        }
        return objects;
      } else {
        return Collections.singleton(ret);
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
   * Finds initial objects factory methods and their dependent classes from the
   * specified object creator class.
   *
   * @author paouelle
   *
   * @param  clazz the non-<code>null</code> object creator class
   * @return the initial objects factory method and its set of dependenc classes
   *         or <code>null</code> if none configured
   * @throws IllegalArgumentException if the initial objects method is not
   *         properly defined
   */
  private static Map<Method, Class<?>[]> findInitials(Class<?> clazz) {
    return ReflectionUtils.getAllAnnotationsForMethodsAnnotatedWith(
      clazz, InitialObjects.class, true
    ).entrySet().stream()
      .peek(e -> {
        final Method m = e.getKey();

        // validate the method is static
        if (!Modifier.isStatic(m.getModifiers())) {
          throw new IllegalArgumentException(
            "initial objects method '"
            + m.getName()
            + "' is not static in class: "
            + clazz.getSimpleName()
          );
        }
        // validate that if it has parameters than it must be a Map<String, String>
        // to provide the values for the suffixes when initializing objects
        final Class<?>[] cparms = m.getParameterTypes();

        if (cparms.length > 1) {
          throw new IllegalArgumentException(
            "expecting only one parameter for initial objects method '"
            + m.getName()
            + "' to be of type Map<String, String> in class: "
            + clazz.getSimpleName()
          );
        } else if (cparms.length == 1) {
          if (!Map.class.isAssignableFrom(cparms[0])) {
            throw new IllegalArgumentException(
              "expecting parameter for initial objects method '"
              + m.getName()
              + "' to be of type Map<String, String> in class: "
              + clazz.getSimpleName()
            );
          }
          final Type[] tparms = m.getGenericParameterTypes();

          if (tparms.length != 1) { // should always be 1 as it was already tested above
            throw new IllegalArgumentException(
              "expecting parameter for initial objects method '"
              + m.getName()
              + "' to be of type Map<String, String> in class: "
              + clazz.getSimpleName()
            );
          }
          if (tparms[0] instanceof ParameterizedType) {
            final ParameterizedType ptype = (ParameterizedType)tparms[0];

            // maps will always have 2 arguments
            for (final Type atype: ptype.getActualTypeArguments()) {
              final Class<?> aclazz = ReflectionUtils.getRawClass(atype);

              if (String.class != aclazz) {
                throw new IllegalArgumentException(
                  "expecting parameter for initial objects method '"
                  + m.getName()
                  + "' to be of type Map<String, String> in class: "
                  + clazz.getSimpleName()
                );
              }
            }
          } else {
            throw new IllegalArgumentException(
              "expecting parameter for initial objects method '"
              + m.getName()
              + "' to be of type Map<String, String> in class: "
              + clazz.getSimpleName()
            );
          }
        }
      })
      .collect(Collectors.toMap(Map.Entry::getKey, e -> (e.getValue()[0]).dependsOn()));
    // the array of InitialObjects annotation will always have only 1 entry as
    // it is not repeatable
  }

  /**
   * Finds object creators with their dependencies based on the provided set of
   * package and/or class names.
   *
   * @author paouelle
   *
   * @param  classes the graph where to record creator classes
   * @param  pkgs the set of packages and/or classes to for creator objects
   * @param  no_dependents if dependents creators should not be considered
   * @throws LinkageError if the linkage fails for one entity class
   * @throws ExceptionInInitializerError if the initialization provoked by one
   *         of one the entity class fails
   */
  private static void findCreatorsFromPackagesOrClasses(
    DirectedGraph<Class<?>> classes, String[] pkgs, boolean no_dependents
  ) {
    for (final String pkg: pkgs) {
      if (pkg == null) {
        continue;
      }
      // search for all object creator classes
      for (final Class<?> clazz: new Reflections(pkg).getTypesAnnotatedWith(
        org.helenus.driver.persistence.ObjectCreator.class, true
      )) {
        final Map<Method, Class<?>[]> initials = Tool.findInitials(clazz);

        if (initials.isEmpty()) {
          System.out.println(
            Tool.class.getSimpleName()
            + ": no objects found using "
            + clazz.getName()
          );
          continue;
        }
        classes.add(clazz);
        if (!no_dependents) {
          final DirectedGraph.Node<Class<?>> node = classes.get(clazz);

          initials.forEach((m, cs) -> {
            for (final Class<?> c: cs) {
              node.add(c);
            }
          });
        }
      }
    }
  }

  /**
   * Inserts objects from all object creators based on the provided collection
   * of classes and options.
   *
   * @author paouelle
   *
   * @param classes the collection of classes for object creators
   * @param suffixes the map of provided suffix values
   */
  private static void insertObjectsFromClasses(
    Collection<Class<?>> classes, Map<String, String> suffixes
  ) {
    for (final Class<?> clazz: classes) {
      final Map<Method, Class<?>[]> initials = Tool.findInitials(clazz);

      if (initials.isEmpty()) { // should not happen!
        System.out.println(
          Tool.class.getSimpleName()
          + ": no objects found using "
          + clazz.getName()
        );
        continue;
      }
      final Group group = StatementBuilder.group();

      for (final Map.Entry<Method, Class<?>[]> e: initials.entrySet()) {
        final Method m = e.getKey();
        final Collection<?> ios = Tool.getInitialObjects(m, suffixes);

        System.out.println(
          Tool.class.getSimpleName()
          + ": inserting "
          + ios.size()
          + " object"
          + (ios.size() == 1 ? "" : "s")
          + " using "
          + clazz.getName()
          + "."
          + m.getName()
          + "()"
        );
        for (final Object io: ios) {
          group.add(StatementBuilder.insert(io).intoAll());
        }
      }
      if (group.isEmpty()) {
        System.out.println(Tool.class.getSimpleName() + ": no objects to insert");
      } else {
        executeCQL(group);
      }
    }
  }

  /**
   * Inserts all defined objects based on the provided command line information.
   *
   * @author paouelle
   *
   * @param  line the command line information
   * @throws Exception if an error occurs while inserting objects
   * @throws LinkageError if the linkage fails for one of the specified entity
   *         class
   * @throws ExceptionInInitializerError if the initialization provoked by one
   *         of the specified entity class fails
   * @throws ClassNotFoundException if one of the object creator class is not
   *         found
   * @throws IllegalCycleException if a dependency cycle is detected in the
   *         classes found
   */
  private static void insertObjects(CommandLine line) throws Exception {
    final String[] opts = line.getOptionValues(Tool.objects.getLongOpt());
    @SuppressWarnings({"cast", "unchecked", "rawtypes"})
    final Map<String, String> suffixes
      = (Map<String, String>)(Map)line.getOptionProperties(Tool.suffixes.getOpt());
    final boolean no_dependents = line.hasOption(Tool.no_dependents.getLongOpt());

    System.out.print(
      Tool.class.getSimpleName()
      + ": searching for object creators in "
      + Arrays.toString(opts)
    );
    if (!suffixes.isEmpty()) {
      System.out.print(" with suffixes " + suffixes);
    }
    if (no_dependents) {
      System.out.print(" not including dependent creators");
    }
    System.out.println();
    final DirectedGraph<Class<?>> classes = new ConcurrentHashDirectedGraph<>();

    Tool.findCreatorsFromPackagesOrClasses(classes, opts, no_dependents);
    // now do a reverse topological sort of the specified graph of classes such that
    // we end up creating objects in the dependent order
    try {
      final List<Class<?>> cs = GraphUtils.sort(classes);

      Collections.reverse(cs);
      Tool.insertObjectsFromClasses(cs, suffixes);
    } catch (IllegalCycleException e) {
      System.out.println(
        Tool.class.getSimpleName()
        + ": circular creator dependency detected: " + e.getCycle()
      );
      throw e;
    }
  }

  /**
   * Sets the root and helenus loggers log level.
   *
   * @author paouelle
   *
   * @param level the level to which the root logger should be at
   */
  private static void setRootLogLevel(Level level) {
    final LoggerContext ctx = (LoggerContext)LogManager.getContext(false);
    final Configuration config = ctx.getConfiguration();
    final String pkg = Tool.class.getPackage().getName();

    for (final String name: new String[] {
           LogManager.ROOT_LOGGER_NAME,
           pkg.subSequence(0, pkg.lastIndexOf('.', pkg.lastIndexOf('.') - 1)).toString(),
           "com.datastax"
         }) {
      final LoggerConfig loggerConfig = config.getLoggerConfig(name);

      loggerConfig.setLevel(level);
    }
    // This causes all Loggers to re-fetch information from their LoggerConfig.
    ctx.updateLoggers();
  }

  /**
   * Main point of entry for this tool
   *
   * @author paouelle
   *
   * @param args the command-line arguments to the tool
   */
  public static void main(String[] args) {
    Tool.setRootLogLevel(Level.OFF); // disable logging by default
    try {
      final CommandLineParser parser = new DefaultParser();
      final CommandLine line = parser.parse(Tool.options, args);

      if (line.hasOption(Tool.verbose.getOpt())) { // do this one first
        Tool.verbose.run(line);
      }
      if (line.hasOption(Tool.trace.getOpt())) { // do this one next
        Tool.trace.run(line);
      }
      for (final Option option: line.getOptions()) { // run these first
        if (option instanceof RunnableFirstOption) {
          ((RunnableFirstOption)option).run(line);
        }
      }
      final String server = line.getOptionValue(
        Tool.server.getLongOpt(), "127.0.0.1" // defaults to local host
      );
      final int port = Integer.parseInt(line.getOptionValue(
        Tool.port.getLongOpt(), "9042"
      ));

      final boolean connect = (
        line.hasOption(Tool.objects.getLongOpt())
        || line.hasOption(Tool.schemas.getLongOpt())
      );

      Tool.mgr = new StatementManagerImpl(
        Cluster
          .builder()
          .withPort(port)
          .addContactPoint(server)
          .withQueryOptions(null),
        connect,
        line.getOptionValues(Tool.filters.getLongOpt())
      );
      if (fvflag) {
        Tool.mgr.enableFullTraces();
      }
      if (line.hasOption(Tool.replicationFactor.getLongOpt())) {
        mgr.setDefaultReplicationFactor(
          Integer.parseInt(
            line.getOptionValue(Tool.replicationFactor.getLongOpt())
          )
        );
      }
      if (line.hasOption(Tool.dataCenters.getOpt())) {
        @SuppressWarnings({"cast", "unchecked", "rawtypes"})
        final Map<String, String> dcss
          = (Map<String, String>)(Map)line.getOptionProperties(Tool.dataCenters.getOpt());
        final Map<String, Integer> dcs = new LinkedHashMap<>(dcss.size() * 3 / 2);

        for (final Map.Entry<String, String> e: dcss.entrySet()) {
          dcs.put(e.getKey(),  Integer.parseInt(e.getValue()));
        }
        mgr.setDefaultDataCenters(dcs);
      }
      try {
        if (connect && Tool.vflag) {
          System.out.println(
            Tool.class.getSimpleName()
            + ": connected to Cassandra on: "
            + server
          );
        }
        for (final Option option: line.getOptions()) {
          if (option instanceof RunnableOption) {
            ((RunnableOption)option).run(line);
          }
        }
      } finally {
        Tool.mgr.close(); // shutdown and wait for its completion
      }
    } catch (Exception e) {
      System.err.print(
        Tool.class.getSimpleName() + ": unexpected exception: "
      );
      e.printStackTrace(System.err);
      System.exit(1);
    }
  }
}

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
package com.github.helenusdriver.driver.tools;

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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.tuple.Pair;

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
import com.fasterxml.jackson.datatype.jsr310.JSR310Module;
import com.fasterxml.jackson.module.jsonSchema.JsonSchema;
import com.fasterxml.jackson.module.jsonSchema.factories.SchemaFactoryWrapper;
import com.github.helenusdriver.commons.cli.RunnableFirstOption;
import com.github.helenusdriver.commons.cli.RunnableOption;
import com.github.helenusdriver.commons.collections.DirectedGraph;
import com.github.helenusdriver.commons.collections.GraphUtils;
import com.github.helenusdriver.commons.collections.graph.ConcurrentHashDirectedGraph;
import com.github.helenusdriver.commons.lang3.IllegalCycleException;
import com.github.helenusdriver.commons.lang3.SerializationUtils;
import com.github.helenusdriver.commons.lang3.reflect.ReflectionUtils;
import com.github.helenusdriver.driver.AlterSchema;
import com.github.helenusdriver.driver.AlterSchemas;
import com.github.helenusdriver.driver.Batch;
import com.github.helenusdriver.driver.CreateSchema;
import com.github.helenusdriver.driver.CreateSchemas;
import com.github.helenusdriver.driver.GenericStatement;
import com.github.helenusdriver.driver.ObjectClassStatement;
import com.github.helenusdriver.driver.ObjectSet;
import com.github.helenusdriver.driver.Sequence;
import com.github.helenusdriver.driver.StatementBuilder;
import com.github.helenusdriver.driver.impl.StatementManagerImpl;
import com.github.helenusdriver.driver.info.ClassInfo;
import com.github.helenusdriver.driver.info.FieldInfo;
import com.github.helenusdriver.persistence.InitialObjects;

import org.reflections.Reflections;

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
   * Holds the schemas creation action.
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
  private final static Option filters = OptionBuilder
    .withLongOpt("filters")
    .withDescription("to specify entity filter classes to register with the driver (separated with :)")
    .withValueSeparator(':')
    .hasArgs()
    .withArgName("classes")
    .create("f");

  /**
   * Holds the Cassandra server option.
   *
   * @author paouelle
   */
  @SuppressWarnings("static-access")
  private final static Option server = OptionBuilder
    .withLongOpt("server")
    .withDescription("to specify the server address for Cassandra (defaults to localhost)")
    .hasArg()
    .withArgName("host")
    .create();

  /**
   * Holds the Cassandra port option.
   *
   * @author paouelle
   */
  @SuppressWarnings("static-access")
  private final static Option port = OptionBuilder
    .withLongOpt("port")
    .withDescription("to specify the port number for Cassandra (defaults to 9042)")
    .hasArg()
    .withArgName("number")
    .create();

  /**
   * Holds the option indicating if only matches should be considered otherwise
   * all of them are created.
   *
   * @author paouelle
   */
  @SuppressWarnings("static-access")
  private final static Option matches_only = OptionBuilder
    .withLongOpt("matches-only")
    .withDescription("to specify that only keyspace that matches the specified suffixes should be created")
    .create();

  /**
   * Holds the alter option.
   *
   * @author paouelle
   */
  @SuppressWarnings("static-access")
  private final static Option alter = OptionBuilder
    .withLongOpt("alter")
    .withDescription("to alter the existing schemas instead of creating it if it doesn't exist")
    .create();

  /**
   * Holds the option indicating if dependent creators should not be considered
   * otherwise all of them are created.
   *
   * @author paouelle
   */
  @SuppressWarnings("static-access")
  private final static Option no_dependents = OptionBuilder
    .withLongOpt("no-dependents")
    .withDescription("to specify that dependent creators should not be considered when creating objects")
    .create();

  /**
   * Holds the Cassandra default replication factor option.
   *
   * @author paouelle
   */
  @SuppressWarnings("static-access")
  private final static Option replicationFactor = OptionBuilder
    .withLongOpt("replication-factor")
    .withDescription("to specify the default replication factor to use with the simple placement strategy when creating keyspaces (defaults to 2)")
    .hasArg()
    .withArgName("value")
    .create();

  /**
   * Holds the Cassandra default data centers option.
   *
   * @author paouelle
   */
  @SuppressWarnings("static-access")
  private final static Option dataCenters = OptionBuilder
    .withDescription("to specify the default data centers and number of replicas to use with the network topology strategy when creating keyspaces (e.g. -Ddatacenter1=2)")
    .hasArgs(2)
    .withArgName("datacenter=replicas")
    .withValueSeparator()
    .create("D");

  /**
   * Holds the suffix options.
   *
   * @author paouelle
   */
  @SuppressWarnings("static-access")
  private final static Option suffixes = OptionBuilder
    .withDescription("to specify value(s) for suffix types (e.g. -Scustomer=acme, -Sregion=emea)")
    .hasArgs(2)
    .withArgName("type=value")
    .withValueSeparator()
    .create("S");

  /**
   * Holds the Cassandra server option.
   *
   * @author paouelle
   */
  @SuppressWarnings("static-access")
  private final static Option output = OptionBuilder
    .withLongOpt("output")
    .withDescription("to specify the output directory (defaults to current directory)")
    .hasArg()
    .withArgName("output")
    .create();

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
      } else if (fvflag || (query.length() < 2048) || !((s instanceof Batch) || (s instanceof Sequence))) {
        System.out.println(
          Tool.class.getSimpleName()
          + ": CQL -> "
          + query
        );
      } else {
        if (s instanceof Batch) {
          System.out.println(
            Tool.class.getSimpleName()
            + ": CQL -> "
            + query.substring(0, 2024)
            + " ... APPLY BATCH;"
          );
        } else {
          System.out.println(
            Tool.class.getSimpleName()
            + ": CQL -> "
            + query.substring(0, 2024)
            + " ... APPLY SEQUENCE;"
          );
        }
      }
    }
    s.execute();
  }

  /**
   * Creates all defined schemas based on the provided set of class names and
   * options and add the statements to the specified sequence. For each class
   * found; the corresponding array element will be nulled. All others are
   * simply skipped.
   *
   * @author paouelle
   *
   * @param  cnames the set of class names to create schemas for
   * @param  suffixes the map of provided suffix values
   * @param  matching whether or not to only create schemas for keyspaces that
   *         matches the specified set of suffixes
   * @param  alter whether to alter or create the schemas
   * @param  s the sequence where to add the generated statements
   * @throws LinkageError if the linkage fails for one of the specified entity
   *         class
   * @throws ExceptionInInitializerError if the initialization provoked by one
   *         of the specified entity class fails
   */
  private static void createSchemasFromClasses(
    String[] cnames,
    Map<String, String> suffixes,
    boolean matching,
    boolean alter,
    Sequence s
  ) {
    next_class:
    for (int i = 0; i < cnames.length; i++) {
      try {
        final Class<?> clazz = Class.forName(cnames[i]);

        cnames[i] = null; // clear since we found a class
        if (alter) {
          final AlterSchema<?> cs = StatementBuilder.alterSchema(clazz);

          // pass all required suffixes
          for (final Map.Entry<String, String> e: suffixes.entrySet()) {
            // check if this suffix type is defined
            final FieldInfo<?> suffix = cs.getClassInfo().getSuffixKeyByType(e.getKey());

            if (suffix != null) {
              // register the suffix value with the corresponding suffix name
              cs.where(
                StatementBuilder.eq(suffix.getSuffixKeyName(), e.getValue())
              );
            } else if (matching) {
              // we have one more suffix then defined with this pojo
              // and we were requested to only do does that match the provided
              // suffixes so skip the class
              continue next_class;
            }
          }
          s.add(cs);
          for (final ClassInfo<?> cinfo: cs.getClassInfos()) {
            System.out.println(
              Tool.class.getSimpleName()
              + ": altering schema for "
              + cinfo.getObjectClass().getName()
            );
          }
        } else {
          final CreateSchema<?> cs = StatementBuilder.createSchema(clazz);

          cs.ifNotExists();
          // pass all required suffixes
          for (final Map.Entry<String, String> e: suffixes.entrySet()) {
            // check if this suffix type is defined
            final FieldInfo<?> suffix = cs.getClassInfo().getSuffixKeyByType(e.getKey());

            if (suffix != null) {
              // register the suffix value with the corresponding suffix name
              cs.where(
                StatementBuilder.eq(suffix.getSuffixKeyName(), e.getValue())
              );
            } else if (matching) {
              // we have one more suffix then defined with this pojo
              // and we were requested to only do does that match the provided
              // suffixes so skip the class
              continue next_class;
            }
          }
          s.add(cs);
          for (final ClassInfo<?> cinfo: cs.getClassInfos()) {
            System.out.println(
              Tool.class.getSimpleName()
              + ": creating schema for "
              + cinfo.getObjectClass().getName()
            );
          }
        }
      } catch (ClassNotFoundException e) { // ignore and continue
      }
    }
  }

  /**
   * Creates all defined schemas based on the provided set of package names and
   * options and add the statements to the specified sequence.
   *
   * @author paouelle
   *
   * @param  pkgs the set of packages to create schemas for
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
  private static void createSchemasFromPackages(
    String[] pkgs,
    Map<String, String> suffixes,
    boolean matching,
    boolean alter,
    Sequence s
  ) {
    for (final String pkg: pkgs) {
      if (pkg == null) {
        continue;
      }
      if (alter) {
        final AlterSchemas cs
          = (matching
              ? StatementBuilder.alterMatchingSchemas(pkg)
              : StatementBuilder.alterSchemas(pkg));

        // pass all suffixes
        for (final Map.Entry<String, String> e: suffixes.entrySet()) {
          // register the suffix value with the corresponding suffix type
          cs.where(
            StatementBuilder.eq(e.getKey(), e.getValue())
          );
        }
        for (final ClassInfo<?> cinfo: cs.getClassInfos()) {
          System.out.println(
            Tool.class.getSimpleName()
            + ": altering schema for "
            + cinfo.getObjectClass().getName()
          );
        }
        s.add(cs);
      } else {
        final CreateSchemas cs
          = (matching
              ? StatementBuilder.createMatchingSchemas(pkg)
              : StatementBuilder.createSchemas(pkg));

        cs.ifNotExists();
        // pass all suffixes
        for (final Map.Entry<String, String> e: suffixes.entrySet()) {
          // register the suffix value with the corresponding suffix type
          cs.where(
            StatementBuilder.eq(e.getKey(), e.getValue())
          );
        }
        for (final ClassInfo<?> cinfo: cs.getClassInfos()) {
          System.out.println(
            Tool.class.getSimpleName()
            + ": creating schema for "
            + cinfo.getObjectClass().getName()
          );
        }
        s.add(cs);
      }
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
    // start by assuming we have classes; if we do they will be nulled from the array
    Tool.createSchemasFromClasses(opts, suffixes, matching, alter, s);
    // now deal with the rest as if they were packages
    Tool.createSchemasFromPackages(opts, suffixes, matching, alter, s);
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
   * Creates all defined Json schemas based on the provided set of class names
   * and options and add the schemas to the specified map. For each class found;
   * the corresponding array element will be nulled. All others are simply
   * skipped.
   *
   * @author paouelle
   *
   * @param  cnames the set of class names to create Json schemas for
   * @param  suffixes the map of provided suffix values
   * @param  matching whether or not to only create schemas for keyspaces that
   *         matches the specified set of suffixes
   * @param  schemas the map where to record the Json schema for the pojo classes
   *         found
   * @throws LinkageError if the linkage fails for one of the specified entity
   *         class
   * @throws ExceptionInInitializerError if the initialization provoked by one
   *         of the specified entity class fails
   * @throws IOException if an I/O error occurs while generating the Json schemas
   */
  private static void createJsonSchemasFromClasses(
    String[] cnames,
    Map<String, String> suffixes,
    boolean matching,
    Map<Class<?>, JsonSchema> schemas
  ) throws IOException {
    next_class:
    for (int i = 0; i < cnames.length; i++) {
      try {
        final Class<?> clazz = Class.forName(cnames[i]);
        Collection<Class<?>> classes;

        cnames[i] = null; // clear since we found a class
        try {
          final CreateSchema<?> cs = StatementBuilder.createSchema(clazz);

          // pass all required suffixes
          for (final Map.Entry<String, String> e: suffixes.entrySet()) {
            // check if this suffix type is defined
            final FieldInfo<?> suffix = cs.getClassInfo().getSuffixKeyByType(e.getKey());

            if (suffix != null) {
              // register the suffix value with the corresponding suffix name
              cs.where(
                StatementBuilder.eq(suffix.getSuffixKeyName(), e.getValue())
              );
            } else if (matching) {
              // we have one more suffix then defined with this pojo
              // and we were requested to only do does that match the provided
              // suffixes so skip the class
              continue next_class;
            }
          }
          classes = cs.getObjectClasses();
        } catch (IllegalArgumentException e) { // ignore and continue with class only
          classes = Arrays.asList(clazz);
        }
        for (final Class<?> c: classes) {
          System.out.println(
            Tool.class.getSimpleName()
            + ": creating Json schema for "
            + c.getName()
          );
          final ObjectMapper m = new ObjectMapper();
          final SchemaFactoryWrapper visitor = new SchemaFactoryWrapper();

          m.registerModules(new Jdk8Module(), new JSR310Module());
          m.enable(SerializationFeature.INDENT_OUTPUT);
          m.acceptJsonFormatVisitor(m.constructType(c), visitor);
          schemas.put(c, visitor.finalSchema());
        }
      } catch (ClassNotFoundException e) { // ignore and continue
      }
    }
  }

  /**
   * Creates all defined Json schemas based on the provided set of package names
   * and options and add the schemas to the specified map.
   *
   * @author paouelle
   *
   * @param  pkgs the set of packages to create Json schemas for
   * @param  suffixes the map of provided suffix values
   * @param  matching whether or not to only create schemas for keyspaces that
   *         matches the specified set of suffixes
   * @param  schemas the map where to record the Json schema for the pojo classes
   *         found
   * @throws LinkageError if the linkage fails for one of the specified entity
   *         class
   * @throws ExceptionInInitializerError if the initialization provoked by one
   *         of the specified entity class fails
   * @throws IllegalArgumentException if no pojos are found in any of the
   *         specified packages
   * @throws IOException if an I/O error occurs while generating the Json schemas
   */
  private static void createJsonSchemasFromPackages(
    String[] pkgs,
    Map<String, String> suffixes,
    boolean matching,
    Map<Class<?>, JsonSchema> schemas
  ) throws IOException {
    for (final String pkg: pkgs) {
      if (pkg == null) {
        continue;
      }
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
      for (final Class<?> c: cs.getObjectClasses()) {
        System.out.println(
          Tool.class.getSimpleName()
          + ": creating Json schema for "
          + c.getName()
        );
        final ObjectMapper m = new ObjectMapper();
        final SchemaFactoryWrapper visitor = new SchemaFactoryWrapper();

        m.registerModule(new Jdk8Module());
        m.enable(SerializationFeature.INDENT_OUTPUT);
        m.acceptJsonFormatVisitor(m.constructType(c), visitor);
        schemas.put(c, visitor.finalSchema());
      }
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
    // start by assuming we have classes; if we do they will be nulled from the array
    Tool.createJsonSchemasFromClasses(opts, suffixes, matching, schemas);
    // now deal with the rest as if they were packages
    Tool.createJsonSchemasFromPackages(opts, suffixes, matching, schemas);
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
   * @param  initial a non-<code>null</code> initial method to retreive objects with
   * @param  suffixes the non-<code>null</code> map of suffixes configured
   * @return the initial objects to insert in the table or <code>null</code>
   *         if none needs to be inserted
   */
  private static List<Object> getInitialObjects(
    Method initial, Map<String, String> suffixes
  ) {
    try {
      final Object array = initial.invoke(null, suffixes);

      if (array == null) {
        return Collections.emptyList();
      }
      final int length = Array.getLength(array);
      final List<Object> objects = new ArrayList<>(length);

      for (int i = 0; i < length; i++) {
        objects.add(Array.get(array, i));
      }
      return objects;
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
   * Finds an initial objects factory method and its dependent classes from the
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
  private static Pair<Method, Class<?>[]> findInitial(Class<?> clazz) {
    final InitialObjects io = clazz.getAnnotation(InitialObjects.class);

    if (io != null) {
      final String mname = io.staticMethod();

      try {
        Method m;

        try { // first look for one with a map for suffixes
          m = clazz.getMethod(mname, Map.class);
          // validate that if suffixes are defined, the method expects a Map<String, String>
          // to provide the values for the suffixes when initializing objects
          final Class<?>[] cparms = m.getParameterTypes();

          // should always be 1 as we used only 1 class in getMethod()
          if (cparms.length != 1) {
            throw new IllegalArgumentException(
              "expecting one Map<String, String> parameter for initial objects method '"
              + mname
              + "' in class: "
              + clazz.getSimpleName()
            );
          }
          // should always be a map as we used a Map to find the method
          if (!Map.class.isAssignableFrom(cparms[0])) {
            throw new IllegalArgumentException(
              "expecting parameter for initial objects method '"
              + mname
              + "' to be of type Map<String, String> in class: "
              + clazz.getSimpleName()
            );
          }
          final Type[] tparms = m.getGenericParameterTypes();

          // should always be 1 as we used only 1 class in getMethod()
          if (tparms.length != 1) { // should always be 1 as it was already tested above
            throw new IllegalArgumentException(
              "expecting one Map<String, String> parameter for initial objects method '"
              + mname
              + "' in class: "
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
                  "expecting a Map<String, String> parameter for initial objects method '"
                  + mname
                  + "' in class: "
                  + clazz.getSimpleName()
                );
              }
            }
          } else {
            throw new IllegalArgumentException(
              "expecting a Map<String, String> parameter for initial objects method '"
              + mname
              + "' in class: "
              + clazz.getSimpleName()
            );
          }
        } catch (NoSuchMethodException e) { // fallback to one with no map
          m = clazz.getMethod(mname);
        }
        // validate the method is static
        if (!Modifier.isStatic(m.getModifiers())) {
          throw new IllegalArgumentException(
            "initial objects method '"
            + mname
            + "' is not static in class: "
            + clazz.getSimpleName()
          );
        }
        // validate the return type is an array
        final Class<?> type = m.getReturnType();

        if (!type.isArray()) {
          throw new IllegalArgumentException(
            "initial objects method '"
            + mname
            + "' doesn't return an array in class: "
            + clazz.getSimpleName()
          );
        }
        return Pair.of(m, io.dependsOn());
      } catch (NoSuchMethodException e) {
        throw new IllegalArgumentException(
          "missing initial objects method '"
          + mname
          + "' in class: "
          + clazz.getSimpleName(),
          e
        );
      }
    }
    return null;
  }

  /**
   * Finds object creators with their dependencies based on the provided set of
   * class names. For each class found; the corresponding array element will be
   * nulled. All others are simply skipped.
   *
   * @author paouelle
   *
   * @param  classes the graph where to record creator classes
   * @param  cnames the set of class names for object creators
   * @param  no_dependents if dependents creators should not be considered
   * @throws LinkageError if the linkage fails for one entity class
   * @throws ExceptionInInitializerError if the initialization provoked by one
   *         of one the entity class fails
   */
  private static void findCreatorsFromClasses(
    DirectedGraph<Class<?>> classes, String[] cnames, boolean no_dependents
  ) {
    for (int i = 0; i < cnames.length; i++) {
      try {
        final Class<?> clazz = Class.forName(cnames[i]);

        cnames[i] = null; // clear since we found a class
        final Pair<Method, Class<?>[]> initial = Tool.findInitial(clazz);

        if (initial == null) {
          System.out.println(
            Tool.class.getSimpleName()
            + ": no objects found using "
            + clazz.getName()
          );
          continue;
        }
        classes.add(clazz);
        final DirectedGraph.Node<Class<?>> node = classes.get(clazz);

        if (!no_dependents) {
          for (final Class<?> c: initial.getRight()) {
            node.add(c);
          }
        }
      } catch (ClassNotFoundException e) { // ignore and continue
      }
    }
  }

  /**
   * Finds object creators with their dependencies based on the provided set of
   * package names.
   *
   * @author paouelle
   *
   * @param  classes the graph where to record creator classes
   * @param  pkgs the set of packages to for creator objects
   * @param  no_dependents if dependents creators should not be considered
   * @throws LinkageError if the linkage fails for one entity class
   * @throws ExceptionInInitializerError if the initialization provoked by one
   *         of one the entity class fails
   */
  private static void findCreatorsFromPackages(
    DirectedGraph<Class<?>> classes, String[] pkgs, boolean no_dependents
  ) {
    for (final String pkg: pkgs) {
      if (pkg == null) {
        continue;
      }
      // search for all object creator classes
      for (final Class<?> clazz: new Reflections(pkg).getTypesAnnotatedWith(
        com.github.helenusdriver.persistence.InitialObjects.class, true
      )) {
        final Pair<Method, Class<?>[]> initial = Tool.findInitial(clazz);

        if (initial == null) {
          System.out.println(
            Tool.class.getSimpleName()
            + ": no objects found using "
            + clazz.getName()
          );
          continue;
        }
        classes.add(clazz);
        final DirectedGraph.Node<Class<?>> node = classes.get(clazz);

        if (!no_dependents) {
          for (final Class<?> c: initial.getRight()) {
            node.add(c);
          }
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
      final Pair<Method, Class<?>[]> initial = Tool.findInitial(clazz);

      if (initial == null) { // should not happen!
        System.out.println(
          Tool.class.getSimpleName()
          + ": no objects found using "
          + clazz.getName()
        );
        continue;
      }
      final List<Object> ios
        = Tool.getInitialObjects(initial.getLeft(), suffixes);

      System.out.println(
        Tool.class.getSimpleName()
        + ": inserting "
        + ios.size()
        + " object"
        + (ios.size() == 1 ? "" : "s")
        + " using "
        + clazz.getName()
      );
      final Batch b = StatementBuilder.batch();

      for (final Object io: ios) {
        b.add(StatementBuilder.insert(io).intoAll());
      }
      if (b.isEmpty() || (b.getQueryString() == null)) {
        System.out.println(Tool.class.getSimpleName() + ": no objects to insert");
      } else {
        executeCQL(b);
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

    // start by assuming we have classes; if we do they will be nulled from the array
    Tool.findCreatorsFromClasses(classes, opts, no_dependents);
    // now deal with the rest as if they were packages
    Tool.findCreatorsFromPackages(classes, opts, no_dependents);
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
           pkg.subSequence(0, pkg.lastIndexOf('.', pkg.lastIndexOf('.') - 1)).toString()
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
      final CommandLineParser parser = new GnuParser();
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

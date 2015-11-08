/*
 * Copyright 2015 Apache Software Foundation.
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
package org.ellitron.tinkerpop.gremlin.ramcloud.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.ellitron.tinkerpop.gremlin.ramcloud.measurement.MeasurementClient;
import org.ellitron.tinkerpop.gremlin.ramcloud.structure.RAMCloudGraph;
import org.ellitron.tinkerpop.gremlin.ramcloud.structure.util.RAMCloudHelper;

/**
 *
 * @author Jonathan Ellithorpe <jde@cs.stanford.edu>
 */
public class LDBCSNBBulkLoader {

    private static final Logger logger = Logger.getLogger(LDBCSNBBulkLoader.class.getName());

    public static void main(String[] args) throws IOException {
        Options options = new Options();
        options.addOption("C", "coordinator", true, "Service locator where the coordinator can be contacted.");
        options.addOption(null, "numMasters", true, "Total master servers.");
        options.addOption(null, "input", true, "Input file directory.");
        options.addOption("h", "help", false, "Print usage.");

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException ex) {
            logger.log(Level.SEVERE, null, ex);
            return;
        }

        if (cmd.hasOption("h")) {
            formatter.printHelp("LDBCSNBBulkLoader", options);
            return;
        }

        // Required parameters.
        String coordinatorLocator;
        if (cmd.hasOption("coordinator")) {
            coordinatorLocator = cmd.getOptionValue("coordinator");
        } else {
            logger.log(Level.SEVERE, "Missing required argument: coordinator");
            return;
        }

        int numMasters;
        if (cmd.hasOption("numMasters")) {
            numMasters = Integer.decode(cmd.getOptionValue("numMasters"));
        } else {
            logger.log(Level.SEVERE, "Missing required argument: numMasters");
            return;
        }

        String inputBaseDir;
        if (cmd.hasOption("input")) {
            inputBaseDir = cmd.getOptionValue("input");
        } else {
            logger.log(Level.SEVERE, "Missing required argument: input");
            return;
        }

        /**
         * TODO: Use environment variables for the source of configuration
         * information.
         */
        BaseConfiguration config = new BaseConfiguration();
        config.setDelimiterParsingDisabled(true);
        config.setProperty(RAMCloudGraph.CONFIG_COORD_LOC, coordinatorLocator);
        config.setProperty(RAMCloudGraph.CONFIG_NUM_MASTER_SERVERS, numMasters);

        RAMCloudGraph graph = RAMCloudGraph.open(config);

        try {
            System.out.print("Loading person_0_0.csv ");
            
            long count = 0;
            String[] colNames = null;
            boolean firstLine = true;
            for (String line : Files.readAllLines(Paths.get(inputBaseDir + "/person_0_0.csv"))) {
                if (firstLine) {
                    colNames = line.split("\\|");
                    firstLine = false;
                    continue;
                }

                String[] colVals = line.split("\\|");
                Map<Object, String> propertiesMap = new HashMap<>();

                for (int i = 0; i < colNames.length; ++i) {
                    if (colNames[i].equals("id")) {
                        propertiesMap.put(T.id, colVals[i]);
                    } else {
                        propertiesMap.put(colNames[i], colVals[i]);
                    }
                }

                propertiesMap.put(T.label, "person");

                List<Object> keyValues = new ArrayList<>();
                propertiesMap.forEach((key, val) -> {
                    keyValues.add(key);
                    keyValues.add(val);
                });

                graph.addVertex(keyValues.toArray());

                count++;
                if (count % 100 == 0) {
                    graph.tx().commit();
                    if (count % 100000 == 0)
                        System.out.print(". ");
                }
            }
            graph.tx().commit();

            System.out.println("Finished loading person_0_0.csv");

            System.out.print("Loading comment_0_0.csv ");
            
            count = 0;
            colNames = null;
            firstLine = true;
            for (String line : Files.readAllLines(Paths.get(inputBaseDir + "/comment_0_0.csv"))) {
                if (firstLine) {
                    colNames = line.split("\\|");
                    firstLine = false;
                    continue;
                }

                String[] colVals = line.split("\\|");
                Map<Object, String> propertiesMap = new HashMap<>();

                for (int i = 0; i < colNames.length; ++i) {
                    if (colNames[i].equals("id")) {
                        propertiesMap.put(T.id, colVals[i]);
                    } else {
                        propertiesMap.put(colNames[i], colVals[i]);
                    }
                }

                propertiesMap.put(T.label, "comment");

                List<Object> keyValues = new ArrayList<>();
                propertiesMap.forEach((key, val) -> {
                    keyValues.add(key);
                    keyValues.add(val);
                });

                graph.addVertex(keyValues.toArray());

                count++;
                if (count % 100 == 0) {
                    graph.tx().commit();
                    if (count % 100000 == 0)
                        System.out.print(". ");
                }
            }
            graph.tx().commit();

            System.out.println("Finished loading comment_0_0.csv");
            
            System.out.print("Loading forum_0_0.csv ");
            
            count = 0;
            colNames = null;
            firstLine = true;
            for (String line : Files.readAllLines(Paths.get(inputBaseDir + "/forum_0_0.csv"))) {
                if (firstLine) {
                    colNames = line.split("\\|");
                    firstLine = false;
                    continue;
                }

                String[] colVals = line.split("\\|");
                Map<Object, String> propertiesMap = new HashMap<>();

                for (int i = 0; i < colNames.length; ++i) {
                    if (colNames[i].equals("id")) {
                        propertiesMap.put(T.id, colVals[i]);
                    } else {
                        propertiesMap.put(colNames[i], colVals[i]);
                    }
                }

                propertiesMap.put(T.label, "comment");

                List<Object> keyValues = new ArrayList<>();
                propertiesMap.forEach((key, val) -> {
                    keyValues.add(key);
                    keyValues.add(val);
                });

                graph.addVertex(keyValues.toArray());

                count++;
                if (count % 100 == 0) {
                    graph.tx().commit();
                    if (count % 100000 == 0)
                        System.out.print(". ");
                }
            }
            graph.tx().commit();

            System.out.println("Finished loading forum_0_0.csv");
        } catch (Exception e) {
            System.out.println("Exception: " + e);
            e.printStackTrace();
        } finally {
            graph.deleteDatabaseAndCloseConnection();
        }
    }
}

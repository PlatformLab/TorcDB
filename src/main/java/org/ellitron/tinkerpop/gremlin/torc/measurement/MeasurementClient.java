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
package org.ellitron.tinkerpop.gremlin.torc.measurement;

import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import edu.stanford.ramcloud.*;
import edu.stanford.ramcloud.multiop.*;
import edu.stanford.ramcloud.transactions.*;
import static edu.stanford.ramcloud.ClientException.*;
import java.awt.BasicStroke;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import static java.lang.Thread.sleep;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FilenameUtils;
import org.apache.tinkerpop.gremlin.structure.T;
import org.ellitron.tinkerpop.gremlin.torc.structure.TorcGraph;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.LogAxis;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.data.statistics.HistogramDataset;
import org.jfree.data.statistics.HistogramType;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;


/**
 *
 * @author Jonathan Ellithorpe <jde@cs.stanford.edu>
 */
public class MeasurementClient {

    private static final Logger logger = Logger.getLogger(MeasurementClient.class.getName());
    
    private final String        coordinatorLocator;
    private final int           numClients;
    private final int           clientIndex;
    private final PrintWriter   logFile;
    private final String        logDir;
    private final int           numMasters;
    private final long          testDuration;
    private final int          numSamples;
    private final int           threads;
    
    private static final Map<String, Runnable> tests = new HashMap<>();
    
    RAMCloud cluster;
    
    private long controlTableId;
    
    public MeasurementClient(   String          coordinatorLocator,
                                int             numClients,
                                int             clientIndex,
                                PrintWriter     logFile,
                                String          logDir,
                                int             numMasters,
                                long            testDuration,
                                int            numSamples,
                                int             threads){
        this.coordinatorLocator = coordinatorLocator;
        this.numClients = numClients;
        this.clientIndex = clientIndex;
        this.logFile = logFile;
        this.logDir = logDir;
        this.numMasters = numMasters;
        this.testDuration = testDuration;
        this.numSamples = numSamples;
        this.threads = threads;
        
        tests.put("testAddVertexThroughput", () -> testAddVertexThroughput());
        tests.put("testAddVertexLatency", () -> testAddVertexLatency());
        
        cluster = new RAMCloud(coordinatorLocator);
        
        controlTableId = cluster.createTable("controlTable");
    }
    
    private enum ControlRegister {
        COMMAND,
        ARGUMENTS,
        RETURN_CODE,
        RETURN_VALUE,
    }
    
    private enum CommandCode {
        RUN,
        RUN_CONTINUOUSLY,
        STOP,
        DIE,
    }
    
    private enum ReturnStatus {
        STATUS_OK,
        UNRECOGNIZED_COMMAND,
        INVALID_COMMAND,
    }
    
    private String getRegisterKey(int clientIndex, ControlRegister reg) {
        return String.format("%d:%s", clientIndex, reg.name());
    }

    /** Helper methods used by masters. */

    private void issueCommandToSlaves(CommandCode cmd, String args, int firstSlave, int numSlaves) {
        // Clear status and return value registers.
        for (int i = 0; i < numSlaves; ++i) {
            String regKey = getRegisterKey(firstSlave + i, ControlRegister.RETURN_CODE);
            cluster.remove(controlTableId, regKey);
            regKey = getRegisterKey(firstSlave + i, ControlRegister.RETURN_VALUE);
            cluster.remove(controlTableId, regKey);
        }
        
        // If this command has args, first write the args.
        if(args != null) {
            for (int i = 0; i < numSlaves; ++i) {
                String regKey = getRegisterKey(firstSlave + i, ControlRegister.ARGUMENTS);
                cluster.write(controlTableId, regKey, args);
            }
        }
        
        // Finally issue the command. 
        for (int i = 0; i < numSlaves; ++i) {
            String regKey = getRegisterKey(firstSlave + i, ControlRegister.COMMAND);
            cluster.write(controlTableId, regKey, cmd.name());
        }
    }
    
    private void pollSlaves(int firstSlave, int numSlaves, double timeoutSeconds) throws Exception {
        for (int i = 0; i < numSlaves; ++i) {
            String regKey = getRegisterKey(firstSlave + i, ControlRegister.RETURN_CODE);
            long startTimeMillis = System.currentTimeMillis();
            while (true) {
                try {
                    RAMCloudObject obj = cluster.read(controlTableId, regKey);
                    if (obj.getValue().equals(ReturnStatus.STATUS_OK.name())) {
                        break;
                    } else {
                        throw new Exception("Slave " + (firstSlave + i) + 
                                " returned error status " + obj.getValue());
                    }
                } catch (TableDoesntExistException | ObjectDoesntExistException e) {
                }

                double elapsedTimeSeconds = (System.currentTimeMillis() - startTimeMillis) / 1000.0;
                if (elapsedTimeSeconds > timeoutSeconds) {
                    throw new Exception("Slave " + (firstSlave + i) + 
                            " did not return a status after "
                            + timeoutSeconds + "s.");
                }

                try {
                    sleep(10L);
                } catch (InterruptedException ex) {
                }
            }
        }
    }
    
    private String[] readSlavesReturnValues(int firstSlave, int numSlaves) {
        String[] values = new String[numSlaves];
        
        for (int i = 0; i < numSlaves; ++i) {
            String regKey = getRegisterKey(firstSlave + i, ControlRegister.RETURN_VALUE);
            try {
                RAMCloudObject obj = cluster.read(controlTableId, regKey);
                values[i] = obj.getValue();
            } catch (TableDoesntExistException | ObjectDoesntExistException e) {
                values[i] = null;
            }
        }
        
        return values;
    }
    
    /** Helper methods used by slaves. */
    
    private CommandCode pollForCommand() {
        String regKey = getRegisterKey(clientIndex, ControlRegister.COMMAND);
        while (true) {
            try {
                RAMCloudObject obj = cluster.read(controlTableId, regKey);
                return CommandCode.valueOf(obj.getValue());
            } catch (TableDoesntExistException | ObjectDoesntExistException e) {
            }

            try {
                sleep(10L);
            } catch (InterruptedException ex) {
            }
        }
    }
    
    private CommandCode checkForCommand() {
        String regKey = getRegisterKey(clientIndex, ControlRegister.COMMAND);
     
        try {
            RAMCloudObject obj = cluster.read(controlTableId, regKey);
            return CommandCode.valueOf(obj.getValue());
        } catch (TableDoesntExistException | ObjectDoesntExistException e) {
            return null;
        }
    }
    
    private String readCommandArguments() {
        String regKey = getRegisterKey(clientIndex, ControlRegister.ARGUMENTS);
        try {
            RAMCloudObject obj = cluster.read(controlTableId, regKey);
            return obj.getValue();
        } catch (TableDoesntExistException | ObjectDoesntExistException e) {
        }
        
        return null;
    }
    
    private void setStatusAndReturnValue(ReturnStatus status, String returnValue) {
        // Clear command register.
        String regKey = getRegisterKey(clientIndex, ControlRegister.COMMAND);
        cluster.remove(controlTableId, regKey);
        
        // Clear command arguments register.
        regKey = getRegisterKey(clientIndex, ControlRegister.ARGUMENTS);
        cluster.remove(controlTableId, regKey);

        // First write out the return value string.
        if (returnValue != null) {
            regKey = getRegisterKey(clientIndex, ControlRegister.RETURN_VALUE);
            cluster.write(controlTableId, regKey, returnValue);
        }
        
        // Finally write out the return status.
        regKey = getRegisterKey(clientIndex, ControlRegister.RETURN_CODE);
        cluster.write(controlTableId, regKey, status.name());
    }
    
    
    public void testAddVertexThroughput() {
        BaseConfiguration ramCloudGraphConfig = new BaseConfiguration();
        ramCloudGraphConfig.setDelimiterParsingDisabled(true);
        ramCloudGraphConfig.setProperty(TorcGraph.CONFIG_COORD_LOCATOR, coordinatorLocator);
        ramCloudGraphConfig.setProperty(TorcGraph.CONFIG_NUM_MASTER_SERVERS, numMasters);
        
        TorcGraph graph = TorcGraph.open(ramCloudGraphConfig);
        
        if(clientIndex == 0) {
            PrintWriter dataFile;
            try {
                dataFile = new PrintWriter(logDir + "/testAddVertexThroughput.data", "UTF-8");
            } catch (FileNotFoundException | UnsupportedEncodingException ex) {
                logger.log(Level.SEVERE, null, ex);
                return;
            }

            dataFile.println("## testAddVertexThroughput(): testDuration=" + testDuration + ", numMasters=" + numMasters);
            dataFile.println("## clients      operations per second");
            dataFile.println("## ----------------------------------");
            
            for (int slaves = 1; slaves < numClients; ++slaves) {
                System.out.println("testAddVertexThroughput(): testDuration=" + testDuration + ", numMasters=" + numMasters + ", clients=" + slaves);
                try {
                    pollSlaves(1, slaves, 10.0);
                } catch (Exception ex) {
                    logger.log(Level.SEVERE, null, ex);
                    issueCommandToSlaves(CommandCode.DIE, null, 1, numClients-1);
                    return;
                }
                
                long startTime = System.currentTimeMillis();
                issueCommandToSlaves(CommandCode.RUN, null, 1, slaves);
                
                try {
                    pollSlaves(1, slaves, testDuration*2.0);
                } catch (Exception ex) {
                    logger.log(Level.SEVERE, null, ex);
                    issueCommandToSlaves(CommandCode.DIE, null, 1, numClients-1);
                    return;
                }
                long elapsedTime = (System.currentTimeMillis() - startTime) / 1000;
                
                String retVals[] = readSlavesReturnValues(1, slaves);
                
                long totalOps = 0;
                for (int i = 0; i < slaves; ++i)
                    totalOps += Long.decode(retVals[i]);
                
                dataFile.println(String.format("%8d\t%d", slaves, totalOps/elapsedTime));
                dataFile.flush();
            }
            
            issueCommandToSlaves(CommandCode.DIE, null, 1, numClients-1);
            
            try {
                pollSlaves(1, numClients-1, 10.0);
            } catch (Exception ex) {
                logger.log(Level.SEVERE, null, ex);
            }
            
            dataFile.flush();
            dataFile.close();
            graph.deleteDatabaseAndCloseConnection();
            return;
        } else {
            setStatusAndReturnValue(ReturnStatus.STATUS_OK, null);
            
            while(true) {
                CommandCode cmd = pollForCommand();

                if (cmd == CommandCode.RUN) {
                    long opCount = 0;

                    long startTime = System.nanoTime();
                    while (true) {
                        graph.addVertex(T.label, "Person", "name", "Raggles");
                        opCount++;
                        if (opCount % 1000 == 0) {
                            if ((System.nanoTime() - startTime) / 1000000000L > testDuration) {
                                break;
                            }
                        }
                    }
                    
                    setStatusAndReturnValue(ReturnStatus.STATUS_OK, Long.toString(opCount));
                } else if (cmd == CommandCode.DIE) { 
                    setStatusAndReturnValue(ReturnStatus.STATUS_OK, null);
                    graph.close();
                    return;
                } else {
                    // Don't recognize this command."
                    setStatusAndReturnValue(ReturnStatus.UNRECOGNIZED_COMMAND, null);
                    graph.close();
                    return;
                }
            }
        }
    }
    
    public void testAddVertexLatency() {
        BaseConfiguration ramCloudGraphConfig = new BaseConfiguration();
        ramCloudGraphConfig.setDelimiterParsingDisabled(true);
        ramCloudGraphConfig.setProperty(TorcGraph.CONFIG_COORD_LOCATOR, coordinatorLocator);
        ramCloudGraphConfig.setProperty(TorcGraph.CONFIG_NUM_MASTER_SERVERS, numMasters);
        
        TorcGraph graph = TorcGraph.open(ramCloudGraphConfig);
        
        if (clientIndex == 0) {
            for (int slaves = 0; slaves < numClients; ++slaves) {
                int clients = slaves + 1;
                System.out.println("testAddVertexLatency(): numSamples=" + numSamples + ", numMasters=" + numMasters + ", clients=" + clients);
                PrintWriter dataFile;
                try {
                    dataFile = new PrintWriter(logDir + "/testAddVertexLatency" + clients + ".data", "UTF-8");
                } catch (FileNotFoundException | UnsupportedEncodingException ex) {
                    logger.log(Level.SEVERE, null, ex);
                    return;
                }

                dataFile.println("## testAddVertexLatency(): numSamples=" + numSamples + ", numMasters=" + numMasters + ", clients=" + clients);
                dataFile.println("## latency (us)      % Samples > X");
                dataFile.println("## ----------------------------------");

                // Wait for slaves to be ready.
                try {
                    pollSlaves(1, slaves, 10.0);
                } catch (Exception ex) {
                    logger.log(Level.SEVERE, null, ex);
                    issueCommandToSlaves(CommandCode.DIE, null, 1, numClients - 1);
                    return;
                }

                issueCommandToSlaves(CommandCode.RUN_CONTINUOUSLY, null, 1, slaves);

                // Wait for slaves to be running.
                try {
                    pollSlaves(1, slaves, 10.0);
                } catch (Exception ex) {
                    logger.log(Level.SEVERE, null, ex);
                    issueCommandToSlaves(CommandCode.DIE, null, 1, numClients - 1);
                    return;
                }
                
                // Take measurements.
                List<Double> latencyTimesUs = new ArrayList<Double>(numSamples);
                for (int i = 0; i < numSamples; ++i) {
                    long opStartTime = System.nanoTime();
                    graph.addVertex(T.label, "Person", "name", "Raggles");
                    long latency = System.nanoTime() - opStartTime;

                    latencyTimesUs.add(((double) latency)/1000.0);
                }

                issueCommandToSlaves(CommandCode.STOP, null, 1, slaves);
                
                Collections.sort(latencyTimesUs);

                for (int i = 0; i < numSamples; ++i) {
                    double percentile = (double) i / (double) numSamples;
                    dataFile.println(String.format("%8.1f\t%.6f", latencyTimesUs.get(i), 1.0 - percentile));
                }

                dataFile.flush();
                dataFile.close();
            }

            // Wait for slaves to be stopped.
            try {
                pollSlaves(1, numClients - 1, 10.0);
            } catch (Exception ex) {
                logger.log(Level.SEVERE, null, ex);
            }
            
            // Command slaves to die.
            issueCommandToSlaves(CommandCode.DIE, null, 1, numClients - 1);

            try {
                pollSlaves(1, numClients - 1, 10.0);
            } catch (Exception ex) {
                logger.log(Level.SEVERE, null, ex);
            }

            graph.deleteDatabaseAndCloseConnection();
            return;
        } else {
            setStatusAndReturnValue(ReturnStatus.STATUS_OK, null);
            
            while(true) {
                CommandCode cmd = pollForCommand();

                if (cmd == CommandCode.RUN_CONTINUOUSLY) {
                    setStatusAndReturnValue(ReturnStatus.STATUS_OK, null);
                    
                    long startTime = System.currentTimeMillis();
                    while (true) {
                        graph.addVertex(T.label, "Person", "name", "Raggles");
                        if (System.currentTimeMillis() - startTime > 100l) {
                            cmd = checkForCommand();
                            if (cmd == null) {
                                startTime = System.currentTimeMillis();
                            } else if (cmd == CommandCode.DIE) {
                                setStatusAndReturnValue(ReturnStatus.STATUS_OK, null);
                                graph.close();
                                return;
                            } else if (cmd == CommandCode.STOP) {
                                setStatusAndReturnValue(ReturnStatus.STATUS_OK, null);
                                break;
                            } else {
                                setStatusAndReturnValue(ReturnStatus.INVALID_COMMAND, null);
                                startTime = System.currentTimeMillis();
                            }
                        }
                    }
                } else if (cmd == CommandCode.DIE) { 
                    setStatusAndReturnValue(ReturnStatus.STATUS_OK, null);
                    graph.close();
                    return;
                } else {
                    // Don't recognize this command."
                    setStatusAndReturnValue(ReturnStatus.UNRECOGNIZED_COMMAND, null);
                    graph.close();
                    return;
                }
            }
        }
    }
    
    public void executeTest(String testName) {
        tests.get(testName).run();
    }
    
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        Options options = new Options();
        String coordinatorLocator;
        int numClients;
        int clientIndex;
        String logFileName;
        int numMasters;
        long testDuration;
        int numSamples;
        int threads;
        String testName;
        
        options.addOption("C", "coordinator", true, "Service locator where the coordinator can be contacted.");
        options.addOption(null, "numClients", true, "Total number of clients.");
        options.addOption(null, "clientIndex", true, "This client's index number in total clients.");
        options.addOption(null, "logFile", true, "File to use as the log.");
        options.addOption(null, "numMasters", true, "Total master servers.");
        options.addOption(null, "testDuration", true, "Duration of each test.");
        options.addOption(null, "numSamples", true, "Number of samples to take.");
        options.addOption(null, "threads", true, "Number of threads to use.");
        options.addOption(null, "testName", true, "Name of the test to run.");
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
        
        if(cmd.hasOption("h")) {
            formatter.printHelp( "MeasurementClient", options );
            return;
        }
        
        // Required parameters.
        if(cmd.hasOption("coordinator")) {
            coordinatorLocator = cmd.getOptionValue("coordinator");
        } else {
            logger.log(Level.SEVERE, "Missing required argument: coordinator");
            return;
        }
        
        if(cmd.hasOption("numMasters")) {
            numMasters = Integer.decode(cmd.getOptionValue("numMasters"));
        } else {
            logger.log(Level.SEVERE, "Missing required argument: numMasters");
            return;
        }
        
        // Optional parameters.
        testDuration = Long.decode(cmd.getOptionValue("testDuration", "5"));
        numSamples = Integer.decode(cmd.getOptionValue("numSamples", "1000000"));
        threads = Integer.decode(cmd.getOptionValue("threads", "1"));
        numClients = Integer.decode(cmd.getOptionValue("numClients", "1"));
        clientIndex = Integer.decode(cmd.getOptionValue("clientIndex", "0"));
        logFileName = cmd.getOptionValue("logFile");
        testName = cmd.getOptionValue("testName");
        
        PrintWriter logFile;
        try {
            logFile = new PrintWriter(logFileName, "UTF-8");
        } catch (FileNotFoundException | UnsupportedEncodingException ex) {
            logger.log(Level.SEVERE, null, ex);
            return;
        }
        
        String logDir = FilenameUtils.getFullPathNoEndSeparator(logFileName);
        
        MeasurementClient mc = new MeasurementClient(   coordinatorLocator,
                                                        numClients,
                                                        clientIndex,
                                                        logFile,
                                                        logDir,
                                                        numMasters,
                                                        testDuration,
                                                        numSamples,
                                                        threads);
        
        mc.executeTest(testName);
    }
    
}

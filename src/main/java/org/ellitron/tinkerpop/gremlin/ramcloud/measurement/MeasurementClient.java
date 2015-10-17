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
package org.ellitron.tinkerpop.gremlin.ramcloud.measurement;

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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.T;
import org.ellitron.tinkerpop.gremlin.ramcloud.structure.RAMCloudGraph;
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
 * @author ellitron
 */
public class MeasurementClient {

    private static final Logger logger = Logger.getLogger(MeasurementClient.class.getName());
    
    private static final String CONFIG_TEST_DURATION = "testDuration";
    private static final String CONFIG_RCGRAPH_CONFIG = "ramCloudGraphConfiguration";
    private static final String CONFIG_THREADS = "threads";
    private static final String CONFIG_NUM_CLIENTS = "numClients";
    private static final String CONFIG_CLIENT_INDEX = "clientIndex";
    private static final String CONFIG_LOG_FILE = "logFile";
    
    public static void testAddVertexThroughput(final Configuration testConfig) {
        // Extract test configuration parameters.
        Configuration ramCloudGraphConfig = (Configuration)testConfig.getProperty(CONFIG_RCGRAPH_CONFIG);
        long testDuration = testConfig.getLong(CONFIG_TEST_DURATION);
        int threads = testConfig.getInt(CONFIG_THREADS);
        
        Thread t[] = new Thread[threads];
        long threadOps[] = new long[threads];
        long totalOps = 0;
        
        for (int i = 0; i < threads; ++i) {
            int threadNum = i;
            t[i] = new Thread(() -> {
                threadOps[threadNum] = 0;
                long startTime, endTime, elapsedTime;

                RAMCloudGraph graph = RAMCloudGraph.open(ramCloudGraphConfig);

                startTime = System.nanoTime();
                while (true) {
                    graph.addVertex(T.label, "Person", "name", "Raggles");
                    threadOps[threadNum]++;
                    if (threadOps[threadNum] % 1000 == 0) {
                        if ((System.nanoTime() - startTime) / 1000000000L > testDuration) {
                            break;
                        }
                    }
                }
                endTime = System.nanoTime();
                elapsedTime = endTime - startTime;

                System.out.println(threadNum + ": Test Finished. Stats:");
                System.out.println("\tTest Duration: " + elapsedTime / 1000000000L + "s");
                System.out.println("\tVertices added: " + threadOps[threadNum]);
                System.out.println("\tThroughput: " + threadOps[threadNum] / (elapsedTime / 1000000000L) + " operations per second");
                System.out.println("\tAvg. Latency: " + (elapsedTime / 1000L) / threadOps[threadNum] + "us");

                graph.close();
            });
        }
        
        long startTime, endTime, elapsedTime;
        
        startTime = System.nanoTime();
        for(int i = 0; i < threads; ++i)
            t[i].start();
        
        try {
            for(int i = 0; i < threads; ++i)
                t[i].join();
        } catch (InterruptedException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
        endTime = System.nanoTime();
        elapsedTime = endTime - startTime;
        
        for(int i = 0; i < threads; ++i)
            totalOps += threadOps[i];
        
        System.out.println(Thread.currentThread().getName() + ": Test Finished. Stats:");
        System.out.println("\tTest Duration: " + elapsedTime / 1000000000L + "s");
        System.out.println("\tVertices added: " + totalOps);
        System.out.println("\tThroughput: " + totalOps / (elapsedTime / 1000000000L) + " operations per second");
        
        // Erase the graph.
        RAMCloudGraph graph = RAMCloudGraph.open(ramCloudGraphConfig);
        graph.eraseAll();
        graph.close();
    }
    
    public static void testAddVertexLatency(final Configuration testConfig) {
        // Extract test configuration parameters.
        Configuration ramCloudGraphConfig = (Configuration)testConfig.getProperty(CONFIG_RCGRAPH_CONFIG);
        long testDuration = testConfig.getLong(CONFIG_TEST_DURATION);
        
        RAMCloudGraph graph = RAMCloudGraph.open(ramCloudGraphConfig);
        
        int opCount = 0;
        long startTime, endTime, elapsedTime;

        List<Double> latencyTimes = new ArrayList<>();
        
        startTime = System.nanoTime();
        while (true) {
            long opStartTime = System.nanoTime();
            graph.addVertex(T.label, "Person", "name", "Raggles");
            long latency = System.nanoTime() - opStartTime;
            
            latencyTimes.add((double)latency);
            
            opCount++;
            if (opCount % 1000 == 0) {
                if ((System.nanoTime() - startTime) / 1000000000L > testDuration) {
                    break;
                }
            }
        }
        endTime = System.nanoTime();
        elapsedTime = endTime - startTime;

        System.out.println("Test Finished. Stats:");
        System.out.println("\tTest Duration: " + elapsedTime / 1000000000L + "s");
        System.out.println("\tVertices added: " + opCount);
        System.out.println("\tThroughput: " + opCount / (elapsedTime / 1000000000L) + " operations per second");
        System.out.println("\tAvg. Latency: " + (elapsedTime / 1000L) / opCount + "us");
        
        Collections.sort(latencyTimes);
        int samples = latencyTimes.size();
        
        // Full plot.
        final XYSeries ccdf_full = new XYSeries("First");
        for(int i = 0; i < samples; ++i) {
            double percentile = (double)i/(double)samples;
            ccdf_full.add((double)latencyTimes.get(i)/1000.0, 1.0 - percentile);
        }
        final XYSeriesCollection dataset_full = new XYSeriesCollection();
        dataset_full.addSeries(ccdf_full);
        final JFreeChart chart_full = ChartFactory.createXYLineChart(
            "Latency CCDF",      // chart title
            "Latency (us)",                      // x axis label
            "Percent > X",                      // y axis label
            dataset_full,                  // data
            PlotOrientation.VERTICAL,
            true,                     // include legend
            true,                     // tooltips
            false                     // urls
        );
        XYPlot plot_full = (XYPlot) chart_full.getPlot();
        plot_full.setDomainPannable(true);
        plot_full.setRangePannable(true);
        plot_full.setDomainGridlineStroke(new BasicStroke(1.0f));
        plot_full.setRangeGridlineStroke(new BasicStroke(1.0f));
        plot_full.setDomainMinorGridlinesVisible(true);
        plot_full.setRangeMinorGridlinesVisible(true);
        plot_full.setDomainMinorGridlineStroke(new BasicStroke(0.1f));
        plot_full.setRangeMinorGridlineStroke(new BasicStroke(0.1f));
        LogAxis yAxis = new LogAxis("Percent > X");
        plot_full.setRangeAxis(yAxis);

        ChartUtilities.applyCurrentTheme(chart_full);
        
        try {
            ChartUtilities.saveChartAsJPEG(new File("chart_full.jpg"), chart_full, 640, 480);
        } catch (IOException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
        
        // Plot to 99%
        final XYSeries ccdf_99 = new XYSeries("First");
        for(int i = 0; i < samples; ++i) {
            double percentile = (double)i/(double)samples;
            ccdf_99.add((double)latencyTimes.get(i)/1000.0, 1.0 - percentile);
            if(percentile > 0.99)
                break;
        }
        final XYSeriesCollection dataset_99 = new XYSeriesCollection();
        dataset_99.addSeries(ccdf_99);
        final JFreeChart chart_99 = ChartFactory.createXYLineChart(
            "Latency CCDF",      // chart title
            "Latency (us)",                      // x axis label
            "Percent > X",                      // y axis label
            dataset_99,                  // data
            PlotOrientation.VERTICAL,
            true,                     // include legend
            true,                     // tooltips
            false                     // urls
        );
        XYPlot plot_99 = (XYPlot) chart_99.getPlot();
        plot_99.setDomainPannable(true);
        plot_99.setRangePannable(true);
        plot_99.setDomainGridlineStroke(new BasicStroke(1.0f));
        plot_99.setRangeGridlineStroke(new BasicStroke(1.0f));
        plot_99.setDomainMinorGridlinesVisible(true);
        plot_99.setRangeMinorGridlinesVisible(true);
        plot_99.setDomainMinorGridlineStroke(new BasicStroke(0.1f));
        plot_99.setRangeMinorGridlineStroke(new BasicStroke(0.1f));
        plot_99.setRangeAxis(yAxis);

        ChartUtilities.applyCurrentTheme(chart_99);
        
        try {
            ChartUtilities.saveChartAsJPEG(new File("chart_99.jpg"), chart_99, 640, 480);
        } catch (IOException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
        
        graph.close();
    }
    
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        Options options = new Options();
        AbstractConfiguration ramCloudGraphConfig = new BaseConfiguration();
        AbstractConfiguration testConfig = new BaseConfiguration();
        String coordinatorLocator;
        int numClients;
        int clientIndex;
        String logFile;
        int masterServers;
        long testDuration;
        int threads;
        
        options.addOption("C", "coordinator", true, "Service locator where the coordinator can be contacted.");
        options.addOption(null, "numClients", true, "Total number of clients.");
        options.addOption(null, "clientIndex", true, "This client's index number in total clients.");
        options.addOption(null, "logFile", true, "File to use as the log.");
        options.addOption(null, "masterServers", true, "Total master servers.");
        options.addOption(null, "testDuration", true, "Duration of each test.");
        options.addOption(null, "threads", true, "Number of threads to use.");
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
        
        if(cmd.hasOption("masterServers")) {
            masterServers = Integer.decode(cmd.getOptionValue("masterServers"));
        } else {
            logger.log(Level.SEVERE, "Missing required argument: masterServers");
            return;
        }
        
        // Optional parameters.
        testDuration = Long.decode(cmd.getOptionValue("testDuration", "5"));
        threads = Integer.decode(cmd.getOptionValue(CONFIG_THREADS, "1"));
        numClients = Integer.decode(cmd.getOptionValue("numClients", "1"));
        clientIndex = Integer.decode(cmd.getOptionValue("clientIndex", "0"));
        logFile = cmd.getOptionValue("logFile", null);
        
        ramCloudGraphConfig.setDelimiterParsingDisabled(true);
        testConfig.setDelimiterParsingDisabled(true);
        ramCloudGraphConfig.setProperty(RAMCloudGraph.CONFIG_COORD_LOC, coordinatorLocator);
        ramCloudGraphConfig.setProperty(RAMCloudGraph.CONFIG_NUM_MASTER_SERVERS, masterServers);
        
        testConfig.addProperty(CONFIG_RCGRAPH_CONFIG, ramCloudGraphConfig);
        testConfig.addProperty(CONFIG_TEST_DURATION, testDuration);
        testConfig.addProperty(CONFIG_THREADS, threads);
        testConfig.addProperty(CONFIG_NUM_CLIENTS, numClients);
        testConfig.addProperty(CONFIG_CLIENT_INDEX, clientIndex);
        testConfig.addProperty(CONFIG_LOG_FILE, logFile);
        
        // Run through the tests.
        //testAddVertexThroughput(testConfig);
        testAddVertexLatency(testConfig);
    }
    
}

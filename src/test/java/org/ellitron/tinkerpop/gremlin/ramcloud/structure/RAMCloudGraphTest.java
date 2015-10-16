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
package org.ellitron.tinkerpop.gremlin.ramcloud.structure;

import java.util.Iterator;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Ignore;

/**
 *
 * @author ellitron
 */
public class RAMCloudGraphTest {
    
    private String coordinatorLocator;
    private int totalMasterServers;
    
    public RAMCloudGraphTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
        coordinatorLocator = System.getProperty("coordLoc");
        totalMasterServers = Integer.decode(System.getProperty("servers"));
        System.out.println("RAMCloudGraphTest: {coordLoc: " + coordinatorLocator + ", servers: " + totalMasterServers + "}");
    }
    
    @After
    public void tearDown() {
    }

    /**
     * Test of open method, of class RAMCloudGraph.
     */
    @Test
    public void testOpen() {
        System.out.println("open");
        
        Configuration configuration = new BaseConfiguration();
        configuration.setProperty(RAMCloudGraph.CONFIG_COORD_LOC, coordinatorLocator);
        configuration.setProperty(RAMCloudGraph.CONFIG_NUM_MASTER_SERVERS, totalMasterServers);
        
        RAMCloudGraph graph = RAMCloudGraph.open(configuration);
        
        graph.close();
    }

    /**
     * Test of addVertex method, of class RAMCloudGraph.
     */
    @Test
    public void testAddVertex() {
        System.out.println("addVertex");
        
        Configuration configuration = new BaseConfiguration();
        configuration.setProperty(RAMCloudGraph.CONFIG_COORD_LOC, coordinatorLocator);
        configuration.setProperty(RAMCloudGraph.CONFIG_NUM_MASTER_SERVERS, totalMasterServers);
        
        RAMCloudGraph graph = RAMCloudGraph.open(configuration);
        
        RAMCloudVertex v1 = graph.addVertex(T.label, "Person", "name", "bob");
        RAMCloudVertex v2 = graph.addVertex(T.label, "Person", "name", "alice");
        RAMCloudVertex v3 = graph.addVertex(T.label, "Animal", "name", "boots", "species", "british shorthair");
        RAMCloudVertex v4 = graph.addVertex(T.label, "Animal", "name", "puff", "species", "foldex cat");
        RAMCloudVertex v5 = graph.addVertex(T.label, "Animal", "name", "sassy", "species", "havana brown");
        
        assertEquals(1, (long)v1.id());
        assertEquals(2, (long)v2.id());
        assertEquals(3, (long)v3.id());
        assertEquals(4, (long)v4.id());
        assertEquals(5, (long)v5.id());
        
        assertEquals("Person", v1.label());
        assertEquals("Person", v2.label());
        assertEquals("Animal", v3.label());
        assertEquals("Animal", v4.label());
        assertEquals("Animal", v5.label());
        
        graph.eraseAll();
        graph.close();
    }

    /**
     * Test of compute method, of class RAMCloudGraph.
     */
    @Test
    @Ignore
    public void testCompute_Class() {
        System.out.println("compute");
        RAMCloudGraph instance = null;
        Object expResult = null;
        Object result = instance.compute(null);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of compute method, of class RAMCloudGraph.
     */
    @Test
    @Ignore
    public void testCompute_0args() {
        System.out.println("compute");
        RAMCloudGraph instance = null;
        GraphComputer expResult = null;
        GraphComputer result = instance.compute();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of vertices method, of class RAMCloudGraph.
     */
    @Test
    @Ignore
    public void testVertices() {
        System.out.println("vertices");
        Object[] os = null;
        RAMCloudGraph instance = null;
        Iterator<Vertex> expResult = null;
        Iterator<Vertex> result = instance.vertices(os);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of edges method, of class RAMCloudGraph.
     */
    @Test
    @Ignore
    public void testEdges() {
        System.out.println("edges");
        Object[] os = null;
        RAMCloudGraph instance = null;
        Iterator<Edge> expResult = null;
        Iterator<Edge> result = instance.edges(os);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of tx method, of class RAMCloudGraph.
     */
    @Test
    @Ignore
    public void testTx() {
        System.out.println("tx");
        RAMCloudGraph instance = null;
        Transaction expResult = null;
        Transaction result = instance.tx();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of variables method, of class RAMCloudGraph.
     */
    @Test
    @Ignore
    public void testVariables() {
        System.out.println("variables");
        RAMCloudGraph instance = null;
        Graph.Variables expResult = null;
        Graph.Variables result = instance.variables();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of configuration method, of class RAMCloudGraph.
     */
    @Test
    @Ignore
    public void testConfiguration() {
        System.out.println("configuration");
        RAMCloudGraph instance = null;
        Configuration expResult = null;
        Configuration result = instance.configuration();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of close method, of class RAMCloudGraph.
     */
    @Test
    @Ignore
    public void testClose() throws Exception {
        System.out.println("close");
        RAMCloudGraph instance = null;
        instance.close();
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }
    
}

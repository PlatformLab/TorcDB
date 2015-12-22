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
package org.ellitron.tinkerpop.gremlin.torc.structure.util;

import edu.stanford.ramcloud.RAMCloud;
import edu.stanford.ramcloud.transactions.RAMCloudTransaction;
import java.util.List;
import org.ellitron.tinkerpop.gremlin.torc.structure.TorcEdgeDirection;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author Jonathan Ellithorpe <jde@cs.stanford.edu>
 */
public class TorcVertexEdgeListTest {
    
    private RAMCloud client;
    private RAMCloudTransaction rctx;
    private static String TEST_TABLE_NAME = "TorcVertexEdgeListTestTable";
    private long testTableId;
    
    public TorcVertexEdgeListTest() {
    }

    @Before
    public void setUp() {
        String ramcloudCoordinatorLocator = System.getenv("RAMCLOUD_COORDINATOR_LOCATOR");
        if (ramcloudCoordinatorLocator == null) 
            throw new RuntimeException("RAMCLOUD_COORDINATOR_LOCATOR environment variable not set. Please set this to your RAMCloud cluster's coordinator locator string (e.g. infrc:host=192.168.1.1,port=12246).");
        
        client = new RAMCloud(ramcloudCoordinatorLocator);
        rctx = new RAMCloudTransaction(client);
        testTableId = client.createTable(TEST_TABLE_NAME);
    }
    
    @After
    public void tearDown() {
        client.dropTable(TEST_TABLE_NAME);
        rctx.close();
        client.disconnect();
    }
    
    @Test
    public void prependEdge_singleEdgeNoProperties_prepended() {
        byte[] keyPrefix = new byte[0];
        int segmentSizeLimit = 1 << 20;
        int segmentTargetSplitPoint = segmentSizeLimit/4;
        
        TorcVertexEdgeList dut = TorcVertexEdgeList.open(rctx, testTableId, keyPrefix, segmentSizeLimit, segmentTargetSplitPoint);
        
        UInt128 neighborId = new UInt128(1);
        
        dut.prependEdge(neighborId, keyPrefix);
        
        List<UInt128> neighborList = dut.readNeighborIds();
        
        assertEquals(1, neighborList.size());
        assertEquals(neighborId, neighborList.get(0));
    }
    
    @Test
    public void prependEdge_manyEdgesNoProperties_prepended() {
        byte[] keyPrefix = new byte[0];
        int segmentSizeLimit = 1 << 20;
        int segmentTargetSplitPoint = segmentSizeLimit/4;
        
        TorcVertexEdgeList dut = TorcVertexEdgeList.open(rctx, testTableId, keyPrefix, segmentSizeLimit, segmentTargetSplitPoint);
        
        for (int i = 0; i < (1<<14); i++) {
            dut.prependEdge(new UInt128(i), keyPrefix);
        }
        
        List<UInt128> neighborList = dut.readNeighborIds();
        
        assertEquals(1 << 14, neighborList.size());
        
        for (int i = 0; i < (1<<14); i++) {
            assertEquals(new UInt128(i), neighborList.get((1<<14) - i - 1));
        }
    }
}

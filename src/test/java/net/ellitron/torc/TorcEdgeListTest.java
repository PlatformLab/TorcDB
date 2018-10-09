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
package net.ellitron.torc;

import net.ellitron.torc.util.*;

import edu.stanford.ramcloud.*;
import edu.stanford.ramcloud.ClientException.*;

import java.util.List;

import static org.junit.Assert.*;

import org.apache.tinkerpop.gremlin.structure.Direction;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author Jonathan Ellithorpe (jde@cs.stanford.edu)
 */
public class TorcEdgeListTest {

  RAMCloud client;
  long tableId;

  public TorcEdgeListTest() {
    this.client = null;
    this.tableId = -1;
  }

  @Before
  public void before() throws Exception {
    String coordLoc = System.getProperty("ramcloudCoordinatorLocator");
    if (coordLoc == null)
      throw new Exception("No RAMCloud coordinator specified. Please specify with -DramcloudCoordinatorLocator=<locator_string>");

    this.client = new RAMCloud(coordLoc);
    this.tableId = client.createTable("test");
  }

  @Test
  public void prependAndRead_withoutProperties0to1k() {
    RAMCloudTransaction rctx = new RAMCloudTransaction(client);

    UInt128 baseVertexId = new UInt128(42);

    byte[] keyPrefix = TorcHelper.getEdgeListKeyPrefix(
        baseVertexId, 
        "hasCreator", 
        Direction.IN,
        "Comment");
   
    for (int i = 0; i < (1<<10); i++) {
      UInt128 neighborId = new UInt128(i);

      boolean newList = TorcEdgeList.prepend(
          rctx,
          tableId,
          keyPrefix,
          neighborId, 
          new byte[] {});

      if (i == 0) {
        assertEquals(newList, true);
      } else {
        assertEquals(newList, false);
      }

      List<TorcEdge> list = TorcEdgeList.read(
          rctx,
          tableId,
          keyPrefix,
          null, 
          baseVertexId,
          "hasCreator", 
          Direction.IN);

      int j = i;
      for (TorcEdge edge : list) {
        UInt128 expectedId = new UInt128(j);
        assertEquals(expectedId, edge.getV1Id());
        j--;
      }
    }

    rctx.close();
  }

  @Test
  public void prependAndRead_smallSegmentSizeManyElements() {
    UInt128 baseVertexId = new UInt128(42);

    byte[] keyPrefix = TorcHelper.getEdgeListKeyPrefix(
        baseVertexId, 
        "hasCreator", 
        Direction.IN,
        "Comment");
  
    int segSize = 25;

    RAMCloudTransaction rctx = new RAMCloudTransaction(client);

    for (int i = 0; i < (1<<16); i++) {
      UInt128 neighborId = new UInt128(i);

      boolean newList = TorcEdgeList.prepend(
          rctx,
          tableId,
          keyPrefix,
          neighborId, 
          new byte[] {},
          segSize,
          0);

      if (i == 0) {
        assertEquals(newList, true);
      } else {
        assertEquals(newList, false);
      }
    }

    List<TorcEdge> list = TorcEdgeList.read(
        rctx,
        tableId,
        keyPrefix,
        null, 
        baseVertexId,
        "hasCreator", 
        Direction.IN);

    int j = (1<<16) - 1;
    for (TorcEdge edge : list) {
      UInt128 expectedId = new UInt128(j);
      assertEquals(expectedId, edge.getV1Id());
      j--;
    }

    rctx.close();
  }

  @Test
  public void prependAndRead_segmentSizes() {
    UInt128 baseVertexId = new UInt128(42);

    byte[] keyPrefix = TorcHelper.getEdgeListKeyPrefix(
        baseVertexId, 
        "hasCreator", 
        Direction.IN,
        "Comment");
  
    for (int segSize = (1<<4); segSize <= (1<<12); segSize *= 2) { 
      RAMCloudTransaction rctx = new RAMCloudTransaction(client);

      for (int i = 0; i < (1<<12); i++) {
        UInt128 neighborId = new UInt128(i);

        boolean newList = TorcEdgeList.prepend(
            rctx,
            tableId,
            keyPrefix,
            neighborId, 
            new byte[] {},
            segSize,
            0);

        if (i == 0) {
          assertEquals(newList, true);
        } else {
          assertEquals(newList, false);
        }

      }

      List<TorcEdge> list = TorcEdgeList.read(
          rctx,
          tableId,
          keyPrefix,
          null, 
          baseVertexId,
          "hasCreator", 
          Direction.IN);

      int j = (1<<12) - 1;
      for (TorcEdge edge : list) {
        UInt128 expectedId = new UInt128(j);
        assertEquals(expectedId, edge.getV1Id());
        j--;
      }

      rctx.close();
    }
  }

  @Test
  public void prependAndRead_withProperties0to1k() {
    RAMCloudTransaction rctx = new RAMCloudTransaction(client);

    UInt128 baseVertexId = new UInt128(42);

    byte[] keyPrefix = TorcHelper.getEdgeListKeyPrefix(
        baseVertexId, 
        "hasCreator", 
        Direction.IN,
        "Comment");
   
    for (int i = 0; i < (1<<10); i++) {
      UInt128 neighborId = new UInt128(i);

      boolean newList = TorcEdgeList.prepend(
          rctx,
          tableId,
          keyPrefix,
          neighborId, 
          neighborId.toByteArray());

      if (i == 0) {
        assertEquals(newList, true);
      } else {
        assertEquals(newList, false);
      }

      List<TorcEdge> list = TorcEdgeList.read(
          rctx,
          tableId,
          keyPrefix,
          null, 
          baseVertexId,
          "hasCreator", 
          Direction.IN);

      int j = i;
      for (TorcEdge edge : list) {
        UInt128 expectedId = new UInt128(j);
        assertEquals(expectedId, edge.getV1Id());
        assertTrue(java.util.Arrays.equals(expectedId.toByteArray(), 
            edge.getSerializedProperties().array()));
        j--;
      }
    }

    rctx.close();
  }

  @Test
  public void prependAndRead_withoutProperties1M() {
    RAMCloudTransaction rctx = new RAMCloudTransaction(client);

    UInt128 baseVertexId = new UInt128(42);

    byte[] keyPrefix = TorcHelper.getEdgeListKeyPrefix(
        baseVertexId, 
        "hasCreator", 
        Direction.IN,
        "Comment");
   
    for (int i = 0; i < (1<<20); i++) {
      UInt128 neighborId = new UInt128(i);

      boolean newList = TorcEdgeList.prepend(
          rctx,
          tableId,
          keyPrefix,
          neighborId, 
          new byte[] {});

      if (i == 0) {
        assertEquals(newList, true);
      } else {
        assertEquals(newList, false);
      }
    }

    List<TorcEdge> list = TorcEdgeList.read(
        rctx,
        tableId,
        keyPrefix,
        null, 
        baseVertexId,
        "hasCreator", 
        Direction.IN);

    int j = (1<<20) - 1;
    for (TorcEdge edge : list) {
      UInt128 expectedId = new UInt128(j);
      assertEquals(expectedId, edge.getV1Id());
      j--;
    }

    rctx.close();
  }

  @Test
  public void prependAndRead_withProperties1M() {
    RAMCloudTransaction rctx = new RAMCloudTransaction(client);

    UInt128 baseVertexId = new UInt128(42);

    byte[] keyPrefix = TorcHelper.getEdgeListKeyPrefix(
        baseVertexId, 
        "hasCreator", 
        Direction.IN,
        "Comment");
   
    for (int i = 0; i < (1<<20); i++) {
      UInt128 neighborId = new UInt128(i);

      boolean newList = TorcEdgeList.prepend(
          rctx,
          tableId,
          keyPrefix,
          neighborId, 
          neighborId.toByteArray());

      if (i == 0) {
        assertEquals(newList, true);
      } else {
        assertEquals(newList, false);
      }
    }

    List<TorcEdge> list = TorcEdgeList.read(
        rctx,
        tableId,
        keyPrefix,
        null, 
        baseVertexId,
        "hasCreator", 
        Direction.IN);

    int j = (1<<20) - 1;
    for (TorcEdge edge : list) {
      UInt128 expectedId = new UInt128(j);
      assertEquals(expectedId, edge.getV1Id());
      assertTrue(java.util.Arrays.equals(expectedId.toByteArray(), 
          edge.getSerializedProperties().array()));
      j--;
    }

    rctx.close();
  }

  @After
  public void after() throws Exception {
    client.dropTable("test");
    client.disconnect();
  }
}

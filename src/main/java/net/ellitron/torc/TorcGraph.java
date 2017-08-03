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

import net.ellitron.torc.util.TorcHelper;
import net.ellitron.torc.util.UInt128;

import edu.stanford.ramcloud.*;
import edu.stanford.ramcloud.ClientException.*;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import org.apache.log4j.Logger;

import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.util.AbstractTransaction;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.EnumSet;
import java.util.function.Consumer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * TorcGraph is an ACID compliant implementation of the TinkerPop Graph
 * interface, and represents a graph stored in RAMCloud.
 * <p>
 * TorcGraph differs from the standard TinkerPop API in two important ways. 1)
 * While TinkerPop supports the creation of multiple edges with the same label
 * between two vertices, TorcGraph supports only one. In other words, vertices
 * A and B can only have one edge labeled "knows" between them. 2) TorcGraph
 * supports bidirectional edges, whereas standard TinkerPop specifies only the
 * existence of directed edges.
 *
 * @author Jonathan Ellithorpe (jde@cs.stanford.edu)
 */
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
public final class TorcGraph implements Graph {

  private static final Logger logger = Logger.getLogger(TorcGraph.class);

  // Configuration keys.
  public static final String CONFIG_GRAPH_NAME =
      "gremlin.torc.graphName";
  public static final String CONFIG_COORD_LOCATOR =
      "gremlin.torc.coordinatorLocator";
  public static final String CONFIG_NUM_MASTER_SERVERS =
      "gremlin.torc.numMasterServers";
  public static final String CONFIG_LOG_LEVEL =
      "gremlin.torc.logLevel";
  public static final String CONFIG_THREADLOCALCLIENTMAP =
      "gremlin.torc.threadLocalClientMap";

  // Constants.
  private static final String ID_TABLE_NAME = "idTable";
  private static final String VERTEX_TABLE_NAME = "vertexTable";
  private static final String EDGELIST_TABLE_NAME = "edgeListTable";
  private static final int MAX_TX_RETRY_COUNT = 100;
  private static final int NUM_ID_COUNTERS = 16;
  private static final int RAMCLOUD_OBJECT_SIZE_LIMIT = 1 << 20;

  // Normal private members.
  private final Configuration configuration;
  private final String coordinatorLocator;
  private final int totalMasterServers;
  private final ConcurrentHashMap<Thread, RAMCloud> threadLocalClientMap;
  private long idTableId, vertexTableId, edgeListTableId;
  private final String graphName;
  private final TorcGraphTransaction torcGraphTx;

  boolean initialized = false;

  private TorcGraph(final Configuration configuration) {
    this.configuration = configuration;

    if (configuration.containsKey(CONFIG_THREADLOCALCLIENTMAP)) {
      this.threadLocalClientMap =
          (ConcurrentHashMap<Thread, RAMCloud>) configuration
          .getProperty(CONFIG_THREADLOCALCLIENTMAP);
    } else {
      this.threadLocalClientMap = new ConcurrentHashMap<>();
    }

    this.torcGraphTx = new TorcGraphTransaction();

    graphName = configuration.getString(CONFIG_GRAPH_NAME);
    coordinatorLocator = configuration.getString(CONFIG_COORD_LOCATOR);

    if (configuration.containsKey(CONFIG_NUM_MASTER_SERVERS)) {
      totalMasterServers = configuration.getInt(CONFIG_NUM_MASTER_SERVERS);
    } else {
      totalMasterServers = 1;
    }

    logger.debug(String.format("Constructing TorcGraph (%s,%s)",
        graphName, coordinatorLocator));
  }

  public static TorcGraph open(Map<String, String> configuration) {
    BaseConfiguration config = new BaseConfiguration();
    config.setDelimiterParsingDisabled(true);

    for (String key : configuration.keySet()) {
      config.setProperty(key, configuration.get(key));
    }

    return new TorcGraph(config);
  }

  public static TorcGraph open(final Configuration configuration) {
    return new TorcGraph(configuration);
  }

  public static TorcGraph open(String graphName) {
    Map<String, String> env = System.getenv();

    BaseConfiguration config = new BaseConfiguration();
    config.setDelimiterParsingDisabled(true);
    config.setProperty(TorcGraph.CONFIG_GRAPH_NAME, graphName);
    config.setProperty(TorcGraph.CONFIG_COORD_LOCATOR,
        env.get("RAMCLOUD_COORDINATOR_LOCATOR"));

    if (env.containsKey("RAMCLOUD_SERVERS")) {
      config.setProperty(TorcGraph.CONFIG_NUM_MASTER_SERVERS,
          env.get("RAMCLOUD_SERVERS"));
    }

    return open(config);
  }

  public static TorcGraph open() {
    return open("default");
  }

  /* **************************************************************************
   *
   * Standard TinkerPop Graph Interface Methods
   *
   * *************************************************************************/

  @Override
  public Vertex addVertex(final Object... keyValues) {
    long startTimeNs = 0;
    if (logger.isDebugEnabled()) {
      startTimeNs = System.nanoTime();
    }

    initialize();

    torcGraphTx.readWrite();
    RAMCloudTransaction rctx = torcGraphTx.getThreadLocalRAMCloudTx();

    long startTime = 0;
    if (logger.isTraceEnabled()) {
      startTime = System.nanoTime();
    }

    TorcHelper.legalPropertyKeyValueArray(Vertex.class, keyValues);

    Object idValue = ElementHelper.getIdValue(keyValues).orElse(null);
    final String label =
        ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);

    if (logger.isTraceEnabled()) {
      long endTime = System.nanoTime();
      logger.trace(String.format("addVertex(label=%s):setup, took %dus",
          label, (endTime - startTime) / 1000l));
    }

    UInt128 vertexId;
    if (idValue != null) {
      vertexId = UInt128.decode(idValue);

      startTime = 0;
      if (logger.isTraceEnabled()) {
        startTime = System.nanoTime();
      }

      // Check if a vertex with this ID already exists.
      try {
        rctx.read(vertexTableId, TorcHelper.getVertexLabelKey(vertexId));
        throw Graph.Exceptions.vertexWithIdAlreadyExists(vertexId.toString());
      } catch (ObjectDoesntExistException e) {
        // Good!
      }

      if (logger.isTraceEnabled()) {
        long endTime = System.nanoTime();
        logger.trace(String.format("addVertex(id=%s,label=%s):idCheck, took "
            + "%dus", vertexId.toString(), label,
            (endTime - startTime) / 1000l));
      }

    } else {
      startTime = 0;
      if (logger.isTraceEnabled()) {
        startTime = System.nanoTime();
      }

      long id_counter = (long) (Math.random() * NUM_ID_COUNTERS);
      RAMCloud client = threadLocalClientMap.get(Thread.currentThread());

      long id =
          client.incrementInt64(idTableId,
              TorcHelper.serializeString(Long.toString(id_counter)), 1, null);

      vertexId = new UInt128((1L << 63) + id_counter, id);

      if (logger.isTraceEnabled()) {
        long endTime = System.nanoTime();
        logger.trace(String.format("addVertex(id=%s,label=%s):idGen, took "
            + "%dus", vertexId.toString(), label,
            (endTime - startTime) / 1000l));
      }
    }

    startTime = 0;
    if (logger.isTraceEnabled()) {
      startTime = System.nanoTime();
    }

    // Create property map.
    Map<String, List<String>> properties = new HashMap<>();
    for (int i = 0; i < keyValues.length; i = i + 2) {
      if (keyValues[i] instanceof String) {
        String key = (String) keyValues[i];
        String val = (String) keyValues[i + 1];
        if (properties.containsKey(key)) {
          properties.get(key).add(val);
        } else {
          properties.put(key, new ArrayList<>(Arrays.asList(val)));
        }
      }
    }

    if (logger.isTraceEnabled()) {
      long endTime = System.nanoTime();
      logger.trace(String.format("addVertex(id=%s,label=%s):propMap, took "
          + "%dus", vertexId.toString(), label,
          (endTime - startTime) / 1000l));
    }

    startTime = 0;
    if (logger.isTraceEnabled()) {
      startTime = System.nanoTime();
    }

    /*
     * Perform size checks on objects to be written to RAMCloud.
     */
    byte[] labelByteArray = TorcHelper.serializeString(label);
    if (labelByteArray.length > RAMCLOUD_OBJECT_SIZE_LIMIT) {
      throw new IllegalArgumentException(String.format("Size of vertex label "
          + "exceeds maximum allowable (size=%dB, max=%dB)",
          labelByteArray.length, RAMCLOUD_OBJECT_SIZE_LIMIT));
    }

    byte[] serializedProps =
        TorcHelper.serializeProperties(properties).array();
    if (serializedProps.length > RAMCLOUD_OBJECT_SIZE_LIMIT) {
      throw new IllegalArgumentException(String.format("Total size of "
          + "properties exceeds maximum allowable (size=%dB, max=%dB)",
          serializedProps.length, RAMCLOUD_OBJECT_SIZE_LIMIT));
    }

    if (logger.isTraceEnabled()) {
      long endTime = System.nanoTime();
      logger.trace(String.format("addVertex(id=%s,label=%s):sizeCheck, took "
          + "%dus", vertexId.toString(), label,
          (endTime - startTime) / 1000l));
    }

    startTime = 0;
    if (logger.isTraceEnabled()) {
      startTime = System.nanoTime();
    }

    rctx.write(vertexTableId, TorcHelper.getVertexLabelKey(vertexId),
        labelByteArray);

    if (logger.isTraceEnabled()) {
      long endTime = System.nanoTime();
      logger.trace(String.format("addVertex(id=%s,label=%s):writeLabel, "
          + "took %dus", vertexId.toString(), label,
          (endTime - startTime) / 1000l));
    }

    startTime = 0;
    if (logger.isTraceEnabled()) {
      startTime = System.nanoTime();
    }

    rctx.write(vertexTableId, TorcHelper.getVertexPropertiesKey(vertexId),
        serializedProps);

    if (logger.isTraceEnabled()) {
      long endTime = System.nanoTime();
      logger.trace(String.format("addVertex(id=%s,label=%s):writeProps, "
          + "took %dus", vertexId.toString(), label,
          (endTime - startTime) / 1000l));
    }

    if (logger.isDebugEnabled()) {
      long endTimeNs = System.nanoTime();
      int propLength = serializedProps.length;
      logger.debug(String.format("addVertex(id=%s,label=%s,propLen=%d), "
          + "took %dus", vertexId.toString(), label, propLength,
          (endTimeNs - startTimeNs) / 1000l));
    }

    return new TorcVertex(this, vertexId, label);
  }

  @Override
  public <C extends GraphComputer> C compute(final Class<C> type)
      throws IllegalArgumentException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public GraphComputer compute() throws IllegalArgumentException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Iterator<Vertex> vertices(final Object... vertexIds) {
    long startTimeNs = 0;
    if (logger.isDebugEnabled()) {
      startTimeNs = System.nanoTime();
    }

    initialize();

    torcGraphTx.readWrite();
    RAMCloudTransaction rctx = torcGraphTx.getThreadLocalRAMCloudTx();

    ElementHelper.validateMixedElementIds(TorcVertex.class, vertexIds);

    List<Vertex> list = new ArrayList<>();
    if (vertexIds.length > 0) {
      if (vertexIds[0] instanceof TorcVertex) {
        Arrays.asList(vertexIds).forEach((id) -> {
          list.add((Vertex) id);
        });
      } else {
        for (int i = 0; i < vertexIds.length; ++i) {
          UInt128 vertexId = UInt128.decode(vertexIds[i]);

          RAMCloudObject obj;
          try {
            obj = rctx.read(vertexTableId,
                TorcHelper.getVertexLabelKey(vertexId));
          } catch (ObjectDoesntExistException e) {
            throw Graph.Exceptions.elementNotFound(TorcVertex.class,
                vertexIds[i]);
          }

          list.add(new TorcVertex(this, vertexId, obj.getValue()));
        }
      }
    } else {
      long max_id[] = new long[NUM_ID_COUNTERS];

      for (int i = 0; i < NUM_ID_COUNTERS; ++i) {
        try {
          ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
          buffer.order(ByteOrder.LITTLE_ENDIAN);
          buffer.put(rctx.read(idTableId, Long.toString(i)).getValueBytes());
          buffer.flip();
          max_id[i] = buffer.getLong();
        } catch (ObjectDoesntExistException e) {
          max_id[i] = 0;
        }
      }

      for (int i = 0; i < NUM_ID_COUNTERS; ++i) {
        for (long j = 1; j <= max_id[i]; ++j) {
          UInt128 vertexId = new UInt128((1L << 63) + i, j);
          try {
            RAMCloudObject obj =
                rctx.read(vertexTableId,
                    TorcHelper.getVertexLabelKey(vertexId));
            list.add(new TorcVertex(this, vertexId, obj.getValue()));
          } catch (ObjectDoesntExistException e) {
            // Continue...
          }
        }
      }
    }

    if (logger.isDebugEnabled()) {
      long endTimeNs = System.nanoTime();
      logger.debug(String.format("vertices(n=%d), took %dus", vertexIds.length,
          (endTimeNs - startTimeNs) / 1000l));
    }

    return list.iterator();
  }

  @Override
  public Iterator<Edge> edges(final Object... edgeIds) {
    long startTimeNs = 0;
    if (logger.isDebugEnabled()) {
      startTimeNs = System.nanoTime();
    }

    initialize();

    torcGraphTx.readWrite();
    RAMCloudTransaction rctx = torcGraphTx.getThreadLocalRAMCloudTx();

    ElementHelper.validateMixedElementIds(TorcEdge.class, edgeIds);

    List<Edge> list = new ArrayList<>();
    if (edgeIds.length > 0) {
      if (edgeIds[0] instanceof TorcEdge) {
        for (int i = 0; i < edgeIds.length; ++i) {
          list.add((Edge) edgeIds[i]);
        }
      } else if (edgeIds[0] instanceof TorcEdge.Id) {
        for (int i = 0; i < edgeIds.length; ++i) {
          list.add(((TorcEdge.Id) edgeIds[i]).getEdge());
        }
      } else {
        throw Graph.Exceptions.elementNotFound(TorcEdge.class, edgeIds[0]);
      }
    } else {
//      long max_id[] = new long[NUM_ID_COUNTERS];
//
//      for (int i = 0; i < NUM_ID_COUNTERS; ++i) {
//        try {
//          ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
//          buffer.order(ByteOrder.LITTLE_ENDIAN);
//          buffer.put(rctx.read(idTableId,
//              Integer.toString(i)).getValueBytes());
//          buffer.flip();
//          max_id[i] = buffer.getLong();
//        } catch (ObjectDoesntExistException e) {
//          max_id[i] = 0;
//        }
//      }
//
//      for (int i = 0; i < NUM_ID_COUNTERS; ++i) {
//        for (long j = 1; j <= max_id[i]; ++j) {
//          UInt128 baseVertexId = new UInt128((1L << 63) + i, j);
//          try {
//            byte[] edgeLabelListKey =
//                TorcHelper.getIncidentEdgeLabelListKey(baseVertexId);
//            RAMCloudObject obj = rctx.read(vertexTableId, edgeLabelListKey);
//            List<String> edgeLabels = TorcHelper.deserializeStringList(obj);
//
//            for (String label : edgeLabels) {
//              /*
//               * Add all the directed edges.
//               */
//              byte[] keyPrefix =
//                  TorcHelper.getEdgeListKeyPrefix(baseVertexId, label,
//                      TorcEdgeDirection.DIRECTED_OUT);
//              TorcVertexEdgeList edgeList =
//                  TorcVertexEdgeList.open(rctx, edgeListTableId, keyPrefix);
//              list.addAll(edgeList.readEdges(this, baseVertexId, label,
//                  TorcEdgeDirection.DIRECTED_OUT));
//
//              /*
//               * Add all the undirected edges.
//               */
//              keyPrefix =
//                  TorcHelper.getEdgeListKeyPrefix(baseVertexId, label,
//                      TorcEdgeDirection.UNDIRECTED);
//              edgeList =
//                  TorcVertexEdgeList.open(rctx, edgeListTableId, keyPrefix);
//              edgeList.readEdges(this, baseVertexId, label,
//                  TorcEdgeDirection.UNDIRECTED).forEach((edge) -> {
//                    if (edge.getV1Id().compareTo(edge.getV2Id()) < 0) {
//                      list.add(edge);
//                    }
//                  });
//            }
//          } catch (ObjectDoesntExistException e) {
//            /*
//             * The edge label list object for this vertex doesn't exist
//             */
//          }
//        }
//      }
    }

    if (logger.isDebugEnabled()) {
      long endTimeNs = System.nanoTime();
      logger.debug(String.format("edges(n=%d), took %dus", edgeIds.length,
          (endTimeNs - startTimeNs) / 1000l));
    }

    return list.iterator();
  }

  @Override
  public Transaction tx() {
    initialize();

    return torcGraphTx;
  }

  /**
   * Closes the thread-local transaction (if it is open), and closes the
   * thread-local connection to RAMCloud (if one has been made). This may
   * affect the state of the graph in RAMCloud depending on the close behavior
   * set for the transaction (e.g. in the case that there is an open
   * transaction which is set to automatically commit when closed).
   *
   * Important: Every thread that performs any operation on this graph instance
   * has the responsibility of calling this close method before exiting.
   * Otherwise it is possible that state that has been created via the RAMCloud
   * JNI library will not be cleaned up properly (for instance, although
   * {@link RAMCloud} and {@link RAMCloudTransaction} objects have implemented
   * finalize() methods to clean up their mirrored C++ objects, it is still
   * possible that the garbage collector will clean up the RAMCloud object
   * before the RAMCloudTransaction object that uses it. This *may* lead to
   * unexpected behavior).
   */
  @Override
  public void close() {
    long startTimeNs = 0;
    if (logger.isDebugEnabled()) {
      startTimeNs = System.nanoTime();
    }

    if (threadLocalClientMap.containsKey(Thread.currentThread())) {
      torcGraphTx.close();
      RAMCloud client = threadLocalClientMap.get(Thread.currentThread());
      client.disconnect();
      threadLocalClientMap.remove(Thread.currentThread());
    }

    if (logger.isDebugEnabled()) {
      long endTimeNs = System.nanoTime();
      logger.debug(String.format("close(), took %dus",
          (endTimeNs - startTimeNs) / 1000l));
    }
  }

  @Override
  public Variables variables() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Configuration configuration() {
    return configuration;
  }

  @Override
  public Features features() {
    return new TorcGraphFeatures();
  }


  /* **************************************************************************
   *
   * TorcGraph Specific Public Facing Methods
   *
   * *************************************************************************/

  public boolean isInitialized() {
    return initialized;
  }

  /** 
   * Fetches the neighbors of a whole set of vertices in bulk, given a set of
   * edge labels and a direction. Takes advantage of TorcGraph's ability to
   * fetch data efficiently in parallel from RAMCloud.  
   *
   * @param vertices Set of vertices to use as base vertices.
   * @param direction Edge direction to traverse.
   * @param edgeLabels Set of edge labels to traverse.
   *
   * @return Map of base vertex to array of neighbor vertices.
   */
  public Map<Vertex, Iterator<Vertex>> vertexNeighbors(
      final List<Vertex> vertices,
      final Direction direction,
      final String[] edgeLabels) {
//    System.out.println(String.format("TorcGraph.vertexNeighbors(): vertices.size = %d, direction = %s, labels = %s",
//          vertices.size(),
//          direction,
//          Arrays.toString(edgeLabels)));
    EnumSet<TorcEdgeDirection> dirs;
    switch (direction) {
      case OUT:
            dirs = EnumSet.of(TorcEdgeDirection.DIRECTED_OUT);
            break;
      case IN:
            dirs = EnumSet.of(TorcEdgeDirection.DIRECTED_IN);
            break;
      case BOTH:
            dirs = EnumSet.of(TorcEdgeDirection.DIRECTED_OUT,
                TorcEdgeDirection.DIRECTED_IN);
            break;

      default:
        throw new UnsupportedOperationException("Unrecognized direction "
            + "value: " + direction);
    }

    Map<Vertex, Iterator<Vertex>> map = new HashMap<>();
    for (Vertex v : vertices) {
      Iterator<Vertex> neighbors = vertexNeighbors((TorcVertex)v, dirs, 
          edgeLabels);
      map.put(v, neighbors);
    }

    return map;
  }

  /** 
   * Fetches the incident edges of a whole set of vertices in bulk, given a set
   * of edge labels and a direction. Takes advantage of TorcGraph's ability to
   * fetch data efficiently in parallel from RAMCloud.  
   *
   * @param vertices Set of vertices to use as base vertices.
   * @param direction Edge direction to traverse.
   * @param edgeLabels Set of edge labels to traverse.
   *
   * @return Map of base vertex to array of incident edges.
   */
  public Map<Vertex, Iterator<Edge>> vertexEdges(
      final List<Vertex> vertices,
      final Direction direction,
      final String[] edgeLabels) {
//    System.out.println(String.format("vertexEdges: vertices.size = %d, direction = %s, labels = %s",
//          vertices.size(),
//          direction,
//          Arrays.toString(edgeLabels)));
    EnumSet<TorcEdgeDirection> dirs;
    switch (direction) {
      case OUT:
            dirs = EnumSet.of(TorcEdgeDirection.DIRECTED_OUT);
            break;
      case IN:
            dirs = EnumSet.of(TorcEdgeDirection.DIRECTED_IN);
            break;
      case BOTH:
            dirs = EnumSet.of(TorcEdgeDirection.DIRECTED_OUT,
                TorcEdgeDirection.DIRECTED_IN);
            break;

      default:
        throw new UnsupportedOperationException("Unrecognized direction "
            + "value: " + direction);
    }

    Map<Vertex, Iterator<Edge>> map = new HashMap<>();
    for (Vertex v : vertices) {
      Iterator<Edge> edges = vertexEdges((TorcVertex)v, dirs, 
          edgeLabels);
      map.put(v, edges);
    }

    return map;
  }

  /**
   * This method closes all open transactions on all threads (using rollback),
   * and closes all open client connections to RAMCloud on all threads. Since
   * this method uses rollback as the close mechanism for open transactions,
   * and RAMCloud transactions keep no server-side state until commit, it is
   * safe to execute this method even after the graph has been deleted with
   * {@link #deleteAll()}. Its intended use is primarily for unit tests to
   * ensure the freeing of all client-side state remaining across JNI (i.e. C++
   * RAMCloud client objects, C++ RAMCloud Transaction objects) before
   * finishing the current test and moving on to the next.
   */
  public void closeAllThreads() {
    long startTimeNs = 0;
    if (logger.isDebugEnabled()) {
      startTimeNs = System.nanoTime();
    }

    torcGraphTx.doRollbackAllThreads();

    threadLocalClientMap.forEach((thread, client) -> {
      try {
        client.disconnect();
      } catch (Exception e) {
        logger.error("closeAllThreads(): could not close transaction of "
            + "thread " + thread.getId());
      }

      logger.debug(String.format("closeAllThreads(): closed client connection "
          + "of %d", thread.getId()));
    });

    threadLocalClientMap.clear();

    if (logger.isDebugEnabled()) {
      long endTimeNs = System.nanoTime();
      logger.debug(String.format("closeAllThreads(), took %dus",
          (endTimeNs - startTimeNs) / 1000l));
    }
  }

  /**
   * This method closes all open client connections to RAMCloud on all threads.
   * Since this method uses rollback as the close mechanism for open
   * transactions, and RAMCloud transactions keep no server-side state until
   * commit, it is safe to execute this method even after the graph has been
   * deleted with {@link #deleteAll()}. Its intended use is primarily for unit
   * tests to reset all transaction state before executing the next test .
   */
  public void rollbackAllThreads() {
    long startTimeNs = 0;
    if (logger.isDebugEnabled()) {
      startTimeNs = System.nanoTime();
    }

    torcGraphTx.doRollbackAllThreads();

    if (logger.isDebugEnabled()) {
      long endTimeNs = System.nanoTime();
      logger.debug(String.format("rollbackAllThreads(), took %dus",
          (endTimeNs - startTimeNs) / 1000l));
    }
  }

  /**
   * Deletes all graph data for the graph represented by this TorcGraph
   * instance in RAMCloud.
   *
   * This method's intended use is for the reset phase of unit tests (see also
   * {@link #closeAllThreads()}). To delete all RAMCloud state representing
   * this graph as well as clear up all client-side state, one would execute
   * the following in sequence:
   *
   * graph.deleteGraph();
   *
   * graph.closeAllThreads();
   */
  public void deleteGraph() {
    long startTimeNs = 0;
    if (logger.isDebugEnabled()) {
      startTimeNs = System.nanoTime();
    }

    initialize();

    RAMCloud client = threadLocalClientMap.get(Thread.currentThread());
    client.dropTable(graphName + "_" + ID_TABLE_NAME);
    client.dropTable(graphName + "_" + VERTEX_TABLE_NAME);
    client.dropTable(graphName + "_" + EDGELIST_TABLE_NAME);
    idTableId = client.createTable(graphName + "_" + ID_TABLE_NAME,
        totalMasterServers);
    vertexTableId = client.createTable(graphName + "_" + VERTEX_TABLE_NAME,
        totalMasterServers);
    edgeListTableId = client.createTable(graphName + "_" + EDGELIST_TABLE_NAME,
        totalMasterServers);

    if (logger.isDebugEnabled()) {
      long endTimeNs = System.nanoTime();
      logger.debug(String.format("deleteGraph(), took %dus",
          (endTimeNs - startTimeNs) / 1000l));
    }
  }

  /* **************************************************************************
   *
   * TorcGraph Specific Internal Methods
   *
   * *************************************************************************/

  /**
   * This method ensures three things are true before it returns to the caller.
   * <ol>
   * <li>This thread has an initialized thread-local RAMCloud client (its own
   * connection to RAMCloud)</li>
   * <li>RAMCloud tables have been created for this graph.</li>
   * <li>RAMCloud table IDs have been fetched.</li>
   * </ol>
   * <p>
   * This method is intended to be used as a way of deferring costly
   * initialization until absolutely needed. This method should be called at
   * the top of any public method of TorcGraph that performs operations against
   * RAMCloud.
   */
  private void initialize() {
    if (!threadLocalClientMap.containsKey(Thread.currentThread())) {
      threadLocalClientMap.put(Thread.currentThread(),
          new RAMCloud(coordinatorLocator));

      logger.debug(String.format("initialize(): Thread %d made connection to "
          + "RAMCloud cluster.", Thread.currentThread().getId()));
    }

    if (!initialized) {
      RAMCloud client = threadLocalClientMap.get(Thread.currentThread());
      idTableId =
          client.createTable(graphName + "_" + ID_TABLE_NAME,
              totalMasterServers);
      vertexTableId =
          client.createTable(graphName + "_" + VERTEX_TABLE_NAME,
              totalMasterServers);
      edgeListTableId =
          client.createTable(graphName + "_" + EDGELIST_TABLE_NAME,
              totalMasterServers);

      initialized = true;

      logger.debug(String.format("initialize(): Fetched table Ids "
          + "(%s=%d,%s=%d,%s=%d)", graphName + "_" + ID_TABLE_NAME,
          idTableId, graphName + "_" + VERTEX_TABLE_NAME,
          vertexTableId, graphName + "_" + EDGELIST_TABLE_NAME,
          edgeListTableId));
    }
  }

  String getLabel(TorcVertex v) {
    RAMCloudTransaction rctx = torcGraphTx.getThreadLocalRAMCloudTx();
    RAMCloudObject neighborLabelRCObj =
        rctx.read(vertexTableId, TorcHelper.getVertexLabelKey(v.id()));
    return neighborLabelRCObj.getValue();
  }

  void removeVertex(final TorcVertex vertex) {
    throw Vertex.Exceptions.vertexRemovalNotSupported();
  }

  Edge addEdge(final TorcVertex vertex1, final TorcVertex vertex2,
      final String edgeLabel, final TorcEdge.Type type, 
      final Object[] keyValues) {
    long startTimeNs = 0;
    if (logger.isDebugEnabled()) {
      startTimeNs = System.nanoTime();
    }

    torcGraphTx.readWrite();
    RAMCloudTransaction rctx = torcGraphTx.getThreadLocalRAMCloudTx();

    if (vertex1 == null || vertex2 == null) {
      throw Graph.Exceptions.argumentCanNotBeNull("vertex");
    }

    ElementHelper.validateLabel(edgeLabel);

    TorcHelper.legalPropertyKeyValueArray(Edge.class, keyValues);

    // Create property map.
    Map<String, List<String>> properties = new HashMap<>();
    for (int i = 0; i < keyValues.length; i = i + 2) {
      String key = (String) keyValues[i];
      String val = (String) keyValues[i + 1];
      if (properties.containsKey(key)) {
        properties.get(key).add(val);
      } else {
        properties.put(key, new ArrayList<>(Arrays.asList(val)));
      }
    }

    ByteBuffer serializedProperties =
        TorcHelper.serializeProperties(properties);

    /*
     * Add one vertex to the other's edge list, and vice versa.
     */
    for (int i = 0; i < 2; ++i) {
      TorcVertex baseVertex;
      TorcVertex neighborVertex;
      TorcEdgeDirection direction;

      /*
       * Choose which vertex acts as the base and which acts as the neighbor in
       * this half of the edge addition operation.
       */
      if (i == 0) {
        baseVertex = vertex1;
        neighborVertex = vertex2;
        if (type == TorcEdge.Type.DIRECTED) {
          direction = TorcEdgeDirection.DIRECTED_OUT;
        } else {
          direction = TorcEdgeDirection.UNDIRECTED;
        }
      } else {
        baseVertex = vertex2;
        neighborVertex = vertex1;
        if (type == TorcEdge.Type.DIRECTED) {
          direction = TorcEdgeDirection.DIRECTED_IN;
        } else {
          direction = TorcEdgeDirection.UNDIRECTED;
        }
      }

      String neighborLabel = neighborVertex.label();

      byte[] keyPrefix =
          TorcHelper.getEdgeListKeyPrefix(baseVertex.id(), edgeLabel, direction,
              neighborLabel);
      TorcVertexEdgeList edgeList =
          TorcVertexEdgeList.open(rctx, edgeListTableId, keyPrefix);
      boolean newListCreated =
          edgeList.prependEdge(neighborVertex.id(),
              serializedProperties.array());

      if (newListCreated) {
        /*
         * It's possible that this is the first edge that has this particular
         * edge label, or the first edge to a vertex with this vertex label, so
         * we must check the edge label list and add it if necessary.
         */
        byte[] incidentEdgeLabelListKey =
            TorcHelper.getIncidentEdgeLabelListKey(baseVertex.id());
        try {
          RAMCloudObject incidentEdgeLabelListRCObj =
              rctx.read(edgeListTableId, incidentEdgeLabelListKey);
          List<String> incidentEdgeLabelList =
              TorcHelper.deserializeStringList(incidentEdgeLabelListRCObj);
          if (!incidentEdgeLabelList.contains(edgeLabel)) {
            incidentEdgeLabelList.add(edgeLabel);
            rctx.write(edgeListTableId, incidentEdgeLabelListKey,
                TorcHelper.serializeStringList(incidentEdgeLabelList).array());
          }
        } catch (ClientException.ObjectDoesntExistException e) {
          List<String> edgeLabelList = new ArrayList<>();
          edgeLabelList.add(edgeLabel);
          rctx.write(vertexTableId, incidentEdgeLabelListKey,
              TorcHelper.serializeStringList(edgeLabelList).array());
        }

        byte[] neighborLabelListKey =
            TorcHelper.getNeighborLabelListKey(baseVertex.id(), edgeLabel,
                direction);
        try {
          RAMCloudObject neighborLabelListRCObj =
              rctx.read(edgeListTableId, neighborLabelListKey);
          List<String> neighborLabelList =
              TorcHelper.deserializeStringList(neighborLabelListRCObj);
          if (!neighborLabelList.contains(neighborLabel)) {
            neighborLabelList.add(neighborLabel);
            rctx.write(edgeListTableId, neighborLabelListKey,
                TorcHelper.serializeStringList(neighborLabelList).array());
          }
        } catch (ClientException.ObjectDoesntExistException e) {
          List<String> neighborLabelList = new ArrayList<>();
          neighborLabelList.add(neighborLabel);
          rctx.write(edgeListTableId, neighborLabelListKey,
              TorcHelper.serializeStringList(neighborLabelList).array());
        }
      }
    }

    if (logger.isDebugEnabled()) {
      long endTimeNs = System.nanoTime();
      int propLength =
          TorcHelper.serializeProperties(properties).array().length;
      logger.debug(String.format("addEdge(from=%s,to=%s,edgeLabel=%s,propLen=%d), "
          + "took %dus", vertex1.id().toString(), vertex2.id().toString(),
          edgeLabel, propLength, (endTimeNs - startTimeNs) / 1000l));
    }

    return new TorcEdge(this, vertex1.id(), vertex2.id(), type, edgeLabel);
  }

  Iterator<Edge> vertexEdges(final TorcVertex vertex,
      final EnumSet<TorcEdgeDirection> edgeDirections,
      final String[] edgeLabels) {
    long startTimeNs = 0;
    if (logger.isDebugEnabled()) {
      startTimeNs = System.nanoTime();
    }

    torcGraphTx.readWrite();
    RAMCloudTransaction rctx = torcGraphTx.getThreadLocalRAMCloudTx();

    List<Edge> edges = new ArrayList<>();
    List<String> labels = Arrays.asList(edgeLabels);

    if (labels.isEmpty()) {
      try {
        byte[] incidentEdgeLabelListKey 
            = TorcHelper.getIncidentEdgeLabelListKey(vertex.id());
        RAMCloudObject incidentEdgeLabelListRCObj =
            rctx.read(edgeListTableId, incidentEdgeLabelListKey);
        labels = TorcHelper.deserializeStringList(incidentEdgeLabelListRCObj);
      } catch (ObjectDoesntExistException e) {

      }
    }

    for (String edgeLabel : labels) {
      for (TorcEdgeDirection dir : edgeDirections) {
        /* Get all the neighbor vertex labels for this edge label. */
        List<String> neighborLabels = new ArrayList<>();
        byte[] neighborLabelListKey =
            TorcHelper.getNeighborLabelListKey(vertex.id(), edgeLabel, dir);

        try {
          RAMCloudObject neighborLabelListRCObj =
              rctx.read(edgeListTableId, neighborLabelListKey);
          neighborLabels =
              TorcHelper.deserializeStringList(neighborLabelListRCObj);
        } catch (ClientException.ObjectDoesntExistException e) {

        }

        for (String neighborLabel : neighborLabels) {
          byte[] keyPrefix =
              TorcHelper.getEdgeListKeyPrefix(vertex.id(), edgeLabel, dir,
                  neighborLabel);
          TorcVertexEdgeList edgeList =
              TorcVertexEdgeList.open(rctx, edgeListTableId, keyPrefix);
          edges.addAll(edgeList.readEdges(this, vertex.id(), edgeLabel, dir));
        }
      }
    }

    if (logger.isDebugEnabled()) {
      long endTimeNs = System.nanoTime();
      logger.debug(String.format("vertexEdges(vertex=%s,directions=%s,"
          + "labels=%s):(n=%d), took %dus", vertex.id().toString(),
          edgeDirections.toString(), labels.toString(), edges.size(),
          (endTimeNs - startTimeNs) / 1000l));
    }

    return edges.iterator();
  }

  Iterator<Vertex> vertexNeighbors(final TorcVertex vertex,
      final EnumSet<TorcEdgeDirection> edgeDirections,
      final String[] edgeLabels) {
    long startTimeNs = 0;
    if (logger.isDebugEnabled()) {
      startTimeNs = System.nanoTime();
    }

    torcGraphTx.readWrite();
    RAMCloudTransaction rctx = torcGraphTx.getThreadLocalRAMCloudTx();

    List<Vertex> vertices = new ArrayList<>();
    List<String> labels = Arrays.asList(edgeLabels);

    if (labels.isEmpty()) {
      try {
        byte[] incidentEdgeLabelListKey 
            = TorcHelper.getIncidentEdgeLabelListKey(vertex.id());
        RAMCloudObject incidentEdgeLabelListRCObj =
            rctx.read(edgeListTableId, incidentEdgeLabelListKey);
        labels = TorcHelper.deserializeStringList(incidentEdgeLabelListRCObj);
      } catch (ObjectDoesntExistException e) {

      }
    }

    for (String edgeLabel : labels) {
      for (TorcEdgeDirection dir : edgeDirections) {
        /* Get all the neighbor vertex labels for this edge label. */
        List<String> neighborLabels = new ArrayList<>();
        byte[] neighborLabelListKey =
            TorcHelper.getNeighborLabelListKey(vertex.id(), edgeLabel, dir);

        try {
          RAMCloudObject neighborLabelListRCObj =
              rctx.read(edgeListTableId, neighborLabelListKey);
          neighborLabels =
              TorcHelper.deserializeStringList(neighborLabelListRCObj);
        } catch (ClientException.ObjectDoesntExistException e) {

        }

        for (String neighborLabel : neighborLabels) {
          byte[] keyPrefix =
              TorcHelper.getEdgeListKeyPrefix(vertex.id(), edgeLabel, dir,
                  neighborLabel);
          TorcVertexEdgeList edgeList =
              TorcVertexEdgeList.open(rctx, edgeListTableId, keyPrefix);
          List<UInt128> neighborIdList = edgeList.readNeighborIds();
          for (UInt128 neighborId : neighborIdList) {
            vertices.add(new TorcVertex(this, neighborId, neighborLabel));
          }
        }
      }
    }

    if (logger.isDebugEnabled()) {
      long endTimeNs = System.nanoTime();
      logger.debug(String.format("vertexNeighbors(vertex=%s,directions=%s,"
          + "labels=%s):(n=%d), took %dus", vertex.id().toString(),
          edgeDirections.toString(), labels.toString(), vertices.size(),
          (endTimeNs - startTimeNs) / 1000l));
    }

    return vertices.iterator();
  }

  <V> Iterator<VertexProperty<V>> getVertexProperties(final TorcVertex vertex,
      final String[] propertyKeys) {
    long startTimeNs = 0;
    if (logger.isDebugEnabled()) {
      startTimeNs = System.nanoTime();
    }

    torcGraphTx.readWrite();
    RAMCloudTransaction rctx = torcGraphTx.getThreadLocalRAMCloudTx();

    RAMCloudObject obj = rctx.read(vertexTableId,
        TorcHelper.getVertexPropertiesKey(vertex.id()));

    Map<String, List<String>> properties =
        TorcHelper.deserializeProperties(obj);

    List<VertexProperty<V>> propList = new ArrayList<>();

    if (propertyKeys.length > 0) {
      for (String key : propertyKeys) {
        if (properties.containsKey(key)) {
          for (String value : properties.get(key)) {
            propList.add(new TorcVertexProperty(vertex, key, value));
          }
        } else {
          throw Property.Exceptions.propertyDoesNotExist(vertex, key);
        }
      }
    } else {
      for (Map.Entry<String, List<String>> property : properties.entrySet()) {
        // TODO: Here I am implicitly assuming that V is of type String, 
        // since property.getValue() returns a string, making the new 
        // elemennt to propList TorcVertexProperty<String>
        String key = property.getKey();
        for (String value : property.getValue()) {
          propList.add(new TorcVertexProperty(vertex, key, value));
        }
      }
    }

    if (logger.isDebugEnabled()) {
      long endTimeNs = System.nanoTime();
      logger.debug(String.format("getVertexProperties(vertex=%s,propKeys=%s), "
          + "took %dus", vertex.id().toString(),
          Arrays.asList(propertyKeys).toString(),
          (endTimeNs - startTimeNs) / 1000l));
    }

    return propList.iterator();
  }

  <V> VertexProperty<V> setVertexProperty(final TorcVertex vertex,
      final VertexProperty.Cardinality cardinality, final String key,
      final V value, final Object[] keyValues) {
    long startTimeNs = 0;
    if (logger.isDebugEnabled()) {
      startTimeNs = System.nanoTime();
    }

    torcGraphTx.readWrite();
    RAMCloudTransaction rctx = torcGraphTx.getThreadLocalRAMCloudTx();

    if (!(keyValues == null || keyValues.length == 0)) {
      throw VertexProperty.Exceptions.metaPropertiesNotSupported();
    }

    if (!(value instanceof String)) {
      throw Property.Exceptions.dataTypeOfPropertyValueNotSupported(value);
    }

    RAMCloudObject obj = rctx.read(vertexTableId,
        TorcHelper.getVertexPropertiesKey(vertex.id()));

    Map<String, List<String>> properties =
        TorcHelper.deserializeProperties(obj);

    if (properties.containsKey(key)) {
      if (cardinality == VertexProperty.Cardinality.single) {
        properties.put(key, new ArrayList<>(Arrays.asList((String) value)));
      } else if (cardinality == VertexProperty.Cardinality.list) {
        properties.get(key).add((String) value);
      } else if (cardinality == VertexProperty.Cardinality.set) {
        if (!properties.get(key).contains((String) value)) {
          properties.get(key).add((String) value);
        }
      } else {
        throw new UnsupportedOperationException("Do not recognize Cardinality "
            + "of this type: " + cardinality.toString());
      }
    } else {
      properties.put(key, new ArrayList<>(Arrays.asList((String) value)));
    }

    rctx.write(vertexTableId, TorcHelper.getVertexPropertiesKey(vertex.id()),
        TorcHelper.serializeProperties(properties).array());

    if (logger.isDebugEnabled()) {
      long endTimeNs = System.nanoTime();
      logger.debug(String.format("setVertexProperty(vertex=%s,card=%s,key=%s,"
          + "value=%s), took %dus", vertex.id().toString(),
          cardinality.toString(), key, value,
          (endTimeNs - startTimeNs) / 1000l));
    }

    return new TorcVertexProperty(vertex, key, value);
  }

  void removeEdge(final TorcEdge edge) {
    throw Edge.Exceptions.edgeRemovalNotSupported();
  }

  Iterator<Vertex> edgeVertices(final TorcEdge edge,
      final Direction direction) {
    long startTimeNs = 0;
    if (logger.isDebugEnabled()) {
      startTimeNs = System.nanoTime();
    }

    if (edge.getType() == TorcEdge.Type.UNDIRECTED
        && direction != Direction.BOTH) {
      throw new RuntimeException(String.format("Tried get source/destination "
          + "vertex of an undirected edge: [edge:%s, direction:%s]",
          edge.toString(), direction.toString()));
    }

    torcGraphTx.readWrite();
    RAMCloudTransaction rctx = torcGraphTx.getThreadLocalRAMCloudTx();

    List<Vertex> list = new ArrayList<>();

    if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH)) {
      RAMCloudObject obj = rctx.read(vertexTableId,
          TorcHelper.getVertexLabelKey(edge.getV1Id()));
      list.add(new TorcVertex(this, edge.getV1Id(), obj.getValue()));
    }

    if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH)) {
      RAMCloudObject obj = rctx.read(vertexTableId,
          TorcHelper.getVertexLabelKey(edge.getV2Id()));
      list.add(new TorcVertex(this, edge.getV2Id(), obj.getValue()));
    }

    if (logger.isDebugEnabled()) {
      long endTimeNs = System.nanoTime();
      // TODO: Update this when we have TorcEdgeId class to stringify
      logger.debug(String.format("edgeVertices(edgeIdGoesHere,dir=%s):(n=%d), "
          + "took %dus", direction.toString(), list.size(),
          (endTimeNs - startTimeNs) / 1000l));
    }

    return list.iterator();
  }

  <V> Iterator<Property<V>> getEdgeProperties(final TorcEdge edge,
      final String[] propertyKeys) {
    long startTimeNs = 0;
    if (logger.isDebugEnabled()) {
      startTimeNs = System.nanoTime();
    }

    torcGraphTx.readWrite();
    RAMCloudTransaction rctx = torcGraphTx.getThreadLocalRAMCloudTx();

    TorcEdgeDirection dir;
    if (edge.getType() == TorcEdge.Type.DIRECTED) {
      dir = TorcEdgeDirection.DIRECTED_OUT;
    } else {
      dir = TorcEdgeDirection.UNDIRECTED;
    }

    /* Get all the neighbor vertex labels for this edge label. */
    List<String> neighborLabels = new ArrayList<>();
    byte[] neighborLabelListKey =
        TorcHelper.getNeighborLabelListKey(edge.getV1Id(), edge.label(), dir);

    try {
      RAMCloudObject neighborLabelListRCObj =
          rctx.read(edgeListTableId, neighborLabelListKey);
      neighborLabels =
          TorcHelper.deserializeStringList(neighborLabelListRCObj);
    } catch (ClientException.ObjectDoesntExistException e) {
      throw Graph.Exceptions.elementNotFound(TorcEdge.class, edge.id());
    }

    byte[] serializedProperties = null;
    for (String neighborLabel : neighborLabels) {
      byte[] keyPrefix =
          TorcHelper.getEdgeListKeyPrefix(edge.getV1Id(), edge.label(), dir,
              neighborLabel);
      TorcVertexEdgeList edgeList =
          TorcVertexEdgeList.open(rctx, edgeListTableId, keyPrefix);

      serializedProperties = edgeList.getEdgeProperties(edge.getV2Id());

      if (serializedProperties != null) {
        break;
      }
    }

    List<Property<V>> propList = new ArrayList<>();

    if (serializedProperties == null) {
      if (propertyKeys == null || propertyKeys.length == 0) {
        return propList.iterator();
      } else {
        logger.debug("Throwing exception with propkey " + propertyKeys[0]);
        throw Property.Exceptions.propertyDoesNotExist((Edge) edge,
            propertyKeys[0]);
      }
    }

    Map<String, List<String>> propMap =
        TorcHelper.deserializeProperties(serializedProperties);

    for (String key : propertyKeys) {
      if (key == null) {
        throw Property.Exceptions.propertyKeyCanNotBeNull();
      } else if (key.length() == 0) {
        throw Property.Exceptions.propertyKeyCanNotBeEmpty();
      }

      if (propMap.containsKey(key)) {
        List<String> values = propMap.get(key);
        for (String value : values) {
          propList.add(new TorcProperty(edge, key, value));
        }
      }
    }

    if (logger.isDebugEnabled()) {
      long endTimeNs = System.nanoTime();
      logger.debug(String.format("getEdgeProperties(v1Id=%s,v2Id=%s,"
          + "propKeys=%s), took %dus", edge.getV1Id().toString(),
          edge.getV2Id().toString(), Arrays.asList(propertyKeys).toString(),
          (endTimeNs - startTimeNs) / 1000l));
    }

    return propList.iterator();
  }

  <V> Property<V> setEdgeProperty(final TorcEdge edge, final String key,
      final V value) {
    throw Element.Exceptions.propertyAdditionNotSupported();
  }

  @Override
  public String toString() {
    return StringFactory.graphString(this, "coordLoc:"
        + this.coordinatorLocator + " graphName:" + this.graphName);
  }

  @Override
  public boolean equals(Object that) {
    if (!(that instanceof TorcGraph)) {
      return false;
    }

    TorcGraph thatGraph = (TorcGraph) that;

    return this.coordinatorLocator.equals(thatGraph.coordinatorLocator)
        && this.graphName.equals(thatGraph.graphName);
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 29 * hash + Objects.hashCode(this.coordinatorLocator);
    hash = 29 * hash + Objects.hashCode(this.graphName);
    return hash;
  }

  // TODO: Move this into its own file.
  public static class Exceptions {

    public static IllegalArgumentException userSuppliedIdNotValid(
        String message) {
      throw new IllegalArgumentException("Invalid vertex ID: " + message);
    }
  }

  class TorcGraphTransaction extends AbstractTransaction {

    private final ConcurrentHashMap<Thread, RAMCloudTransaction> threadLocalRCTXMap =
        new ConcurrentHashMap<>();

    public TorcGraphTransaction() {
      super(TorcGraph.this);
    }

    /**
     * This method returns the underlying RAMCloudTransaction object for this
     * thread that contains all of the transaction state.
     *
     * @return RAMCloudTransaction for current thread.
     */
    protected RAMCloudTransaction getThreadLocalRAMCloudTx() {
      return threadLocalRCTXMap.get(Thread.currentThread());
    }

    /**
     * This method rolls back the transactions of all threads that have not
     * closed their transactions themselves. It is meant to be used as a final
     * cleanup method to free all transaction state before exiting, in the case
     * that threads had executed without performing final cleanup themselves
     * before exiting. This currently happens in TinkerPop unit tests
     * (3.1.0-incubating). See
     * {@link org.apache.tinkerpop.gremlin.structure.TransactionTest#shouldExecuteCompetingThreadsOnMultipleDbInstances}.
     * This method is *not* meant to be called while other threads and still
     * executing.
     */
    private void doRollbackAllThreads() {
      threadLocalRCTXMap.forEach((thread, rctx) -> {
        try {
          rctx.close();
        } catch (Exception e) {
          logger.error("TorcGraphTransaction.doRollbackAllThreads(): could not"
              + " close transaction of thread " + thread.getId());
        }

        logger.debug(
            String.format("TorcGraphTransaction.doRollbackAllThreads(): "
                + "rolling back oustanding transaction of thread %d",
                thread.getId()));
      });

      threadLocalRCTXMap.clear();
    }

    @Override
    public void doOpen() {
      long startTimeNs = 0;
      if (logger.isDebugEnabled()) {
        startTimeNs = System.nanoTime();
      }

      Thread us = Thread.currentThread();
      if (threadLocalRCTXMap.get(us) == null) {
        RAMCloud client = threadLocalClientMap.get(us);
        threadLocalRCTXMap.put(us, new RAMCloudTransaction(client));
      } else {
        throw Transaction.Exceptions.transactionAlreadyOpen();
      }

      if (logger.isDebugEnabled()) {
        long endTimeNs = System.nanoTime();
        logger.debug(String.format("TorcGraphTransaction.doOpen(thread=%d), "
            + "took %dus", us.getId(), (endTimeNs - startTimeNs) / 1000l));
      }
    }

    @Override
    public boolean isOpen() {
      boolean isOpen =
          (threadLocalRCTXMap.get(Thread.currentThread()) != null);

      return isOpen;
    }

    @Override
    public void doCommit() throws AbstractTransaction.TransactionException {
      long startTimeNs = 0;
      if (logger.isDebugEnabled()) {
        startTimeNs = System.nanoTime();
      }

      RAMCloudTransaction rctx =
          threadLocalRCTXMap.get(Thread.currentThread());

      try {
        if (!rctx.commitAndSync()) {
          throw new AbstractTransaction.TransactionException("RAMCloud "
              + "commitAndSync failed.");
        }
      } catch (ClientException ex) {
        throw new AbstractTransaction.TransactionException(ex);
      } finally {
        rctx.close();
        threadLocalRCTXMap.remove(Thread.currentThread());
      }

      if (logger.isDebugEnabled()) {
        long endTimeNs = System.nanoTime();
        logger.debug(String.format("TorcGraphTransaction.doCommit(thread=%d), "
            + "took %dus", Thread.currentThread().getId(),
            (endTimeNs - startTimeNs) / 1000l));
      }
    }

    @Override
    public void doRollback() throws AbstractTransaction.TransactionException {
      long startTimeNs = 0;
      if (logger.isDebugEnabled()) {
        startTimeNs = System.nanoTime();
      }

      RAMCloudTransaction rctx =
          threadLocalRCTXMap.get(Thread.currentThread());

      try {
        rctx.close();
      } catch (Exception e) {
        throw new AbstractTransaction.TransactionException(e);
      } finally {
        threadLocalRCTXMap.remove(Thread.currentThread());
      }

      if (logger.isDebugEnabled()) {
        long endTimeNs = System.nanoTime();
        logger.debug(
            String.format("TorcGraphTransaction.doRollback(thread=%d), "
                + "took %dus", Thread.currentThread().getId(),
                (endTimeNs - startTimeNs) / 1000l));
      }
    }

    @Override
    protected void fireOnCommit() {
      // Not implemented.
    }

    @Override
    protected void fireOnRollback() {
      // Not implemented.
    }

    @Override
    protected void doReadWrite() {
      if (!isOpen()) {
        doOpen();
      }
    }

    @Override
    protected void doClose() {
      if (isOpen()) {
        try {
          doRollback();
        } catch (TransactionException ex) {
          throw new RuntimeException(ex);
        }
      }
    }

    @Override
    public Transaction onReadWrite(Consumer<Transaction> consumer) {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Transaction onClose(Consumer<Transaction> consumer) {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void addTransactionListener(Consumer<Status> listener) {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void removeTransactionListener(Consumer<Status> listener) {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void clearTransactionListeners() {
      throw new UnsupportedOperationException("Not supported yet.");
    }
  }

  // TODO: Move this to its own file.
  public class TorcGraphFeatures implements Features {

    private TorcGraphFeatures() {
    }

    @Override
    public Features.GraphFeatures graph() {
      return new TorcGraphGraphFeatures();
    }

    @Override
    public Features.VertexFeatures vertex() {
      return new TorcGraphVertexFeatures();
    }

    @Override
    public Features.EdgeFeatures edge() {
      return new TorcGraphEdgeFeatures();
    }

    @Override
    public String toString() {
      return StringFactory.featureString(this);
    }
  }

  public class TorcGraphGraphFeatures implements Features.GraphFeatures {

    private TorcGraphGraphFeatures() {
    }

    @Override
    public boolean supportsComputer() {
      return false;
    }

    @Override
    public boolean supportsPersistence() {
      return true;
    }

    @Override
    public boolean supportsConcurrentAccess() {
      return false;
    }

    @Override
    public boolean supportsTransactions() {
      return true;
    }

    @Override
    public boolean supportsThreadedTransactions() {
      return false;
    }

    @Override
    public Features.VariableFeatures variables() {
      return new TorcGraphVariableFeatures() {
      };
    }

  }

  public class TorcGraphVertexFeatures implements Features.VertexFeatures {

    private TorcGraphVertexFeatures() {
    }

    @Override
    public VertexProperty.Cardinality getCardinality(final String key) {
      return VertexProperty.Cardinality.single;
    }

    @Override
    public boolean supportsAddVertices() {
      return true;
    }

    @Override
    public boolean supportsRemoveVertices() {
      return false;
    }

    @Override
    public boolean supportsMultiProperties() {
      return true;
    }

    @Override
    public boolean supportsMetaProperties() {
      return false;
    }

    @Override
    public Features.VertexPropertyFeatures properties() {
      return new TorcGraphVertexPropertyFeatures();
    }

    @Override
    public boolean supportsAddProperty() {
      return true;
    }

    @Override
    public boolean supportsRemoveProperty() {
      return true;
    }

    @Override
    public boolean supportsUserSuppliedIds() {
      return false;
    }

    @Override
    public boolean supportsNumericIds() {
      return false;
    }

    @Override
    public boolean supportsStringIds() {
      return false;
    }

    @Override
    public boolean supportsUuidIds() {
      return false;
    }

    @Override
    public boolean supportsCustomIds() {
      return false;
    }

    @Override
    public boolean supportsAnyIds() {
      return false;
    }
  }

  public class TorcGraphEdgeFeatures implements Features.EdgeFeatures {

    private TorcGraphEdgeFeatures() {
    }

    @Override
    public boolean supportsAddEdges() {
      return true;
    }

    @Override
    public boolean supportsRemoveEdges() {
      return false;
    }

    @Override
    public Features.EdgePropertyFeatures properties() {
      return new TorcGraphEdgePropertyFeatures() {
      };
    }

    @Override
    public boolean supportsAddProperty() {
      return false;
    }

    @Override
    public boolean supportsRemoveProperty() {
      return false;
    }

    @Override
    public boolean supportsUserSuppliedIds() {
      return false;
    }

    @Override
    public boolean supportsNumericIds() {
      return false;
    }

    @Override
    public boolean supportsStringIds() {
      return false;
    }

    @Override
    public boolean supportsUuidIds() {
      return false;
    }

    @Override
    public boolean supportsCustomIds() {
      return false;
    }

    @Override
    public boolean supportsAnyIds() {
      return false;
    }
  }

  public class TorcGraphVertexPropertyFeatures
      implements Features.VertexPropertyFeatures {

    private TorcGraphVertexPropertyFeatures() {
    }

    @Override
    public boolean supportsAddProperty() {
      return false;
    }

    @Override
    public boolean supportsRemoveProperty() {
      return false;
    }

    @Override
    public boolean supportsUserSuppliedIds() {
      return false;
    }

    @Override
    public boolean supportsNumericIds() {
      return false;
    }

    @Override
    public boolean supportsStringIds() {
      return false;
    }

    @Override
    public boolean supportsUuidIds() {
      return false;
    }

    @Override
    public boolean supportsCustomIds() {
      return false;
    }

    @Override
    public boolean supportsAnyIds() {
      return false;
    }

    @Override
    public boolean supportsBooleanValues() {
      return false;
    }

    @Override
    public boolean supportsByteValues() {
      return false;
    }

    @Override
    public boolean supportsDoubleValues() {
      return false;
    }

    @Override
    public boolean supportsFloatValues() {
      return false;
    }

    @Override
    public boolean supportsIntegerValues() {
      return false;
    }

    @Override
    public boolean supportsLongValues() {
      return false;
    }

    @Override
    public boolean supportsMapValues() {
      return false;
    }

    @Override
    public boolean supportsMixedListValues() {
      return false;
    }

    @Override
    public boolean supportsBooleanArrayValues() {
      return false;
    }

    @Override
    public boolean supportsByteArrayValues() {
      return false;
    }

    @Override
    public boolean supportsDoubleArrayValues() {
      return false;
    }

    @Override
    public boolean supportsFloatArrayValues() {
      return false;
    }

    @Override
    public boolean supportsIntegerArrayValues() {
      return false;
    }

    @Override
    public boolean supportsStringArrayValues() {
      return false;
    }

    @Override
    public boolean supportsLongArrayValues() {
      return false;
    }

    @Override
    public boolean supportsSerializableValues() {
      return false;
    }

    @Override
    public boolean supportsStringValues() {
      return true;
    }

    @Override
    public boolean supportsUniformListValues() {
      return false;
    }
  }

  public class TorcGraphEdgePropertyFeatures
      implements Features.EdgePropertyFeatures {

    private TorcGraphEdgePropertyFeatures() {
    }

    @Override
    public boolean supportsBooleanValues() {
      return false;
    }

    @Override
    public boolean supportsByteValues() {
      return false;
    }

    @Override
    public boolean supportsDoubleValues() {
      return false;
    }

    @Override
    public boolean supportsFloatValues() {
      return false;
    }

    @Override
    public boolean supportsIntegerValues() {
      return false;
    }

    @Override
    public boolean supportsLongValues() {
      return false;
    }

    @Override
    public boolean supportsMapValues() {
      return false;
    }

    @Override
    public boolean supportsMixedListValues() {
      return false;
    }

    @Override
    public boolean supportsBooleanArrayValues() {
      return false;
    }

    @Override
    public boolean supportsByteArrayValues() {
      return false;
    }

    @Override
    public boolean supportsDoubleArrayValues() {
      return false;
    }

    @Override
    public boolean supportsFloatArrayValues() {
      return false;
    }

    @Override
    public boolean supportsIntegerArrayValues() {
      return false;
    }

    @Override
    public boolean supportsStringArrayValues() {
      return false;
    }

    @Override
    public boolean supportsLongArrayValues() {
      return false;
    }

    @Override
    public boolean supportsSerializableValues() {
      return false;
    }

    @Override
    public boolean supportsStringValues() {
      return true;
    }

    @Override
    public boolean supportsUniformListValues() {
      return false;
    }
  }

  public class TorcGraphVariableFeatures implements Features.VariableFeatures {

    private TorcGraphVariableFeatures() {
    }

    @Override
    public boolean supportsBooleanValues() {
      return false;
    }

    @Override
    public boolean supportsByteValues() {
      return false;
    }

    @Override
    public boolean supportsDoubleValues() {
      return false;
    }

    @Override
    public boolean supportsFloatValues() {
      return false;
    }

    @Override
    public boolean supportsIntegerValues() {
      return false;
    }

    @Override
    public boolean supportsLongValues() {
      return false;
    }

    @Override
    public boolean supportsMapValues() {
      return false;
    }

    @Override
    public boolean supportsMixedListValues() {
      return false;
    }

    @Override
    public boolean supportsBooleanArrayValues() {
      return false;
    }

    @Override
    public boolean supportsByteArrayValues() {
      return false;
    }

    @Override
    public boolean supportsDoubleArrayValues() {
      return false;
    }

    @Override
    public boolean supportsFloatArrayValues() {
      return false;
    }

    @Override
    public boolean supportsIntegerArrayValues() {
      return false;
    }

    @Override
    public boolean supportsStringArrayValues() {
      return false;
    }

    @Override
    public boolean supportsLongArrayValues() {
      return false;
    }

    @Override
    public boolean supportsSerializableValues() {
      return false;
    }

    @Override
    public boolean supportsStringValues() {
      return false;
    }

    @Override
    public boolean supportsUniformListValues() {
      return false;
    }
  }
}

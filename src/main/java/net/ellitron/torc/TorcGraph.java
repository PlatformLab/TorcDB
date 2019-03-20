/* Copyright (c) 2015-2019 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */
package net.ellitron.torc;

import net.ellitron.torc.util.TorcHelper;
import net.ellitron.torc.util.UInt128;

import edu.stanford.ramcloud.*;
import edu.stanford.ramcloud.ClientException.*;
import edu.stanford.ramcloud.multiop.MultiReadObject;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import org.apache.log4j.Logger;

import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
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

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Collections;
import java.util.Collection;
import java.util.EnumSet;
import java.util.function.Consumer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.*;
import java.util.Set;

import javax.xml.bind.DatatypeConverter;

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
  public static final String CONFIG_DPDK_PORT =
      "gremlin.torc.dpdkPort";
  public static final String CONFIG_NUM_MASTER_SERVERS =
      "gremlin.torc.numMasterServers";
  public static final String CONFIG_LOG_LEVEL =
      "gremlin.torc.logLevel";
  public static final String CONFIG_THREADLOCALCLIENTMAP =
      "gremlin.torc.threadLocalClientMap";
  /*
   * A special operating mode for directly creating RAMCloud image files while
   * loading nodes and edges into the graph. In this mode only the loadVertex
   * and loadEdgeList methods (coming soon) are available. When this mode is
   * enabled, these methods write directly to RAMCloud images files on local
   * disk, in a directory configured by the CONFIG_RC_IMAGE_DIRECTORY parameter.
   */
  public static final String CONFIG_RC_IMAGE_CREATION_MODE =
      "gremlin.torc.rcImageCreationMode";
  public static final String CONFIG_RC_IMAGE_DIRECTORY =
      "gremlin.torc.rcImageDirectory";

  // Constants.
  private static final String ID_TABLE_NAME = "idTable";
  private static final String VERTEX_TABLE_NAME = "vertexTable";
  private static final String EDGELIST_TABLE_NAME = "edgeListTable";
  private static final int MAX_TX_RETRY_COUNT = 100;
  private static final int RAMCLOUD_OBJECT_SIZE_LIMIT = 1 << 20;

  // Normal private members.
  private Configuration configuration;
  private String coordinatorLocator;
  private boolean rcImageCreationMode = false;
  private String rcImageDir;
  private OutputStream vertexTableOS, edgeListTableOS;
  private int totalMasterServers;
  private int dpdkPort;
  private ConcurrentHashMap<Thread, RAMCloud> threadLocalClientMap;
  private long idTableId, vertexTableId, edgeListTableId;
  private String graphName;
  private TorcGraphTransaction torcGraphTx;

  /* Set by enableTx() and disableTx(). Controls whether or not reads and writes
   * are performed in a transaction context. TorcDB's default behavior is to
   * automatically open a transaction upon first read/write and continue in that
   * transaction context until commit or rollback. Sometimes it may be desired,
   * for performance reasons or otherwise, to execute outside of a transaction
   * context, and in that case the user may call disableTx(), after which reads
   * and writes will execute outside any transaction context. 
   */
  private boolean txMode = true; // We're in transactional mode by default.

  boolean initialized = false;

  private TorcGraph(final Configuration configuration) {
    this.configuration = configuration;

    graphName = configuration.getString(CONFIG_GRAPH_NAME);

    if (configuration.containsKey(CONFIG_THREADLOCALCLIENTMAP)) {
      this.threadLocalClientMap =
          (ConcurrentHashMap<Thread, RAMCloud>) configuration
          .getProperty(CONFIG_THREADLOCALCLIENTMAP);
    } else {
      this.threadLocalClientMap = new ConcurrentHashMap<>();
    }

    if (configuration.containsKey(CONFIG_RC_IMAGE_CREATION_MODE)) {
      rcImageCreationMode = true;

      rcImageDir = configuration.getString(CONFIG_RC_IMAGE_DIRECTORY);

      try {
        vertexTableOS = new BufferedOutputStream(new FileOutputStream(
              rcImageDir + "/" + graphName + "_" + VERTEX_TABLE_NAME + 
              ".img"));

        edgeListTableOS = new BufferedOutputStream(new FileOutputStream(
              rcImageDir + "/" + graphName + "_" + EDGELIST_TABLE_NAME + 
              ".img"));
      } catch (FileNotFoundException e) {
        throw new RuntimeException(e);
      }

      logger.debug(String.format("Constructing TorcGraph (%s,%s)",
          graphName, rcImageDir));
    } else {
      coordinatorLocator = configuration.getString(CONFIG_COORD_LOCATOR);

      if (configuration.containsKey(CONFIG_NUM_MASTER_SERVERS)) {
        totalMasterServers = configuration.getInt(CONFIG_NUM_MASTER_SERVERS);
      } else {
        totalMasterServers = 1;
      }

      if (configuration.containsKey(CONFIG_DPDK_PORT)) {
        dpdkPort = configuration.getInt(CONFIG_DPDK_PORT);
      } else {
        dpdkPort = -1;
      }

      this.torcGraphTx = new TorcGraphTransaction();

      logger.debug(String.format("Constructing TorcGraph (%s,%s)",
          graphName, coordinatorLocator));
    }
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
    initialize();

    torcGraphTx.readWrite();
    RAMCloudTransaction rctx = torcGraphTx.getThreadLocalRAMCloudTx();
    RAMCloud client = threadLocalClientMap.get(Thread.currentThread());

    TorcHelper.legalPropertyKeyValueArray(Vertex.class, keyValues);

    Object idValue = ElementHelper.getIdValue(keyValues).orElse(null);
    final String label =
        ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);

    UInt128 vertexId;
    if (idValue != null) {
      vertexId = UInt128.decode(idValue);
    } else {
      throw new UnsupportedOperationException("Automatic ID generation not supported.");
    }

    // Create property map.
    Map<Object, Object> properties = new HashMap<>();
    for (int i = 0; i < keyValues.length; i = i + 2) {
      if (keyValues[i] instanceof String) {
        Object key = keyValues[i];
        Object val = keyValues[i + 1];
        if (properties.containsKey(key)) {
          if (properties.get(key) instanceof List) {
            ((List)properties.get(key)).add(val);
          } else {
            // Convert this to a list.
            List<Object> list = new ArrayList<>();
            list.add(properties.get(key));
            list.add(val);
            properties.put(key, list);
          }
        } else {
          properties.put(key, val);
        }
      }
    }

    /* Perform size checks on objects to be written to RAMCloud. */
    byte[] labelByteArray = TorcHelper.serializeObject(label);
    if (labelByteArray.length > RAMCLOUD_OBJECT_SIZE_LIMIT) {
      throw new IllegalArgumentException(String.format("Size of vertex label "
          + "exceeds maximum allowable (size=%dB, max=%dB)",
          labelByteArray.length, RAMCLOUD_OBJECT_SIZE_LIMIT));
    }

    byte[] serializedProps = TorcHelper.serializeObject(properties);
    if (serializedProps.length > RAMCLOUD_OBJECT_SIZE_LIMIT) {
      throw new IllegalArgumentException(String.format("Total size of "
          + "properties exceeds maximum allowable (size=%dB, max=%dB)",
          serializedProps.length, RAMCLOUD_OBJECT_SIZE_LIMIT));
    }

    if (txMode) {
      rctx.write(vertexTableId, TorcHelper.getVertexLabelKey(vertexId),
          labelByteArray);
      rctx.write(vertexTableId, TorcHelper.getVertexPropertiesKey(vertexId),
          serializedProps);
    } else {
      client.write(vertexTableId, TorcHelper.getVertexLabelKey(vertexId),
          labelByteArray, null);
      client.write(vertexTableId, TorcHelper.getVertexPropertiesKey(vertexId),
          serializedProps, null);
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
    initialize();

    torcGraphTx.readWrite();
    RAMCloudTransaction rctx = torcGraphTx.getThreadLocalRAMCloudTx();
    RAMCloud client = threadLocalClientMap.get(Thread.currentThread());

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
            if (txMode) {
              obj = rctx.read(vertexTableId,
                  TorcHelper.getVertexLabelKey(vertexId));
            } else {
              obj = client.read(vertexTableId,
                  TorcHelper.getVertexLabelKey(vertexId));
            }
          } catch (ClientException e) {
            throw new RuntimeException(e);
          }

          if (obj == null) {
            throw Graph.Exceptions.elementNotFound(TorcVertex.class,
                vertexIds[i]);
          }
          
          list.add(new TorcVertex(this, vertexId, 
                (String)TorcHelper.deserializeObject(obj.getValueBytes())));
        }
      }
    } else {
      throw new UnsupportedOperationException("Reading all graph vertices not supported.");
    }

    return list.iterator();
  }

  @Override
  public Iterator<Edge> edges(final Object... edgeIds) {
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
      throw new UnsupportedOperationException("Reading all graph edges not supported.");
    }

    return list.iterator();
  }

  @Override
  public Transaction tx() {
    initialize();

    return torcGraphTx;
  }

  /**
   * Expose RamCloud client to application for testing, probing, debugging, and
   * statistics gathering purposes.
   */
  public RAMCloud ramcloud() {
    return threadLocalClientMap.get(Thread.currentThread());
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
    if (rcImageCreationMode) {
      try {
        vertexTableOS.flush();
        vertexTableOS.close();
        edgeListTableOS.flush();
        edgeListTableOS.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      if (threadLocalClientMap.containsKey(Thread.currentThread())) {
        torcGraphTx.close();
        RAMCloud client = threadLocalClientMap.get(Thread.currentThread());
        client.disconnect();
        threadLocalClientMap.remove(Thread.currentThread());
      }
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

  public TraversalResult traverse(
      TorcVertex v, 
      String eLabel, 
      Direction dir, 
      boolean fillEdge,
      String ... neighborLabels) {
    return traverse(Collections.singleton(v), eLabel, dir, 
        fillEdge, neighborLabels);
  }

  public TraversalResult traverse(
      TraversalResult r, 
      String eLabel, 
      Direction dir, 
      boolean fillEdge,
      String ... neighborLabels) {
    return traverse(r.vSet, eLabel, dir, fillEdge, neighborLabels);
  }

  /** 
   * Traverses an edge type for a set of vertices.
   *
   * @param vCol Collection of vertices to start from.
   * @param eLabel Label of edge to traverse.
   * @param dir Direction of edge.
   * @param fillEdge Whether or not to fill in edge properties in the return
   * result
   * @param nLabels Labels of neighbor vertices.
   *
   * @return TraversalResult describing the result of the traversal.
   */
  public TraversalResult traverse(
      Collection<TorcVertex> vCol,
      String eLabel, 
      Direction dir, 
      boolean fillEdge,
      String ... nLabels) {
    initialize();

    torcGraphTx.readWrite();
    RAMCloudTransaction rctx = torcGraphTx.getThreadLocalRAMCloudTx();
    RAMCloud client = threadLocalClientMap.get(Thread.currentThread());

    List<byte[]> keyPrefixes = 
        TorcHelper.getEdgeListKeyPrefixes(vCol, eLabel, dir, nLabels);

    Map<byte[], List<TorcSerializedEdge>> serEdgeLists;
    if (txMode) {
      serEdgeLists = TorcEdgeList.batchRead(rctx, edgeListTableId, keyPrefixes);
    } else {
      serEdgeLists = TorcEdgeList.batchRead(client, edgeListTableId, keyPrefixes);
    }

    Map<TorcVertex, List<TorcVertex>> nbrListMap = new HashMap<>();

    Map<TorcVertex, List<Map<Object, Object>>> ePropListMap = null;
    if (fillEdge)
      ePropListMap = new HashMap<>();

    Map<UInt128, TorcVertex> nbrDedupMap = new HashMap<>();
    Set<TorcVertex> uniqNbrSet = new HashSet<>();

    int i = 0;
    for (String nLabel : nLabels) {
      for (TorcVertex vertex : vCol) {
        byte[] keyPrefix = keyPrefixes.get(i);

        if (serEdgeLists.containsKey(keyPrefix)) {
          List<TorcSerializedEdge> serEdgeList = serEdgeLists.get(keyPrefix);

          List<TorcVertex> nList;
          List<Map<Object, Object>> ePropList = null;
          if (nbrListMap.containsKey(vertex)) {
            nList = nbrListMap.get(vertex);
            if (fillEdge)
              ePropList = ePropListMap.get(vertex);
          } else {
            nList = new ArrayList<>(serEdgeList.size());
            nbrListMap.put(vertex, nList);
            if (fillEdge) {
              ePropList = new ArrayList<>(serEdgeList.size());
              ePropListMap.put(vertex, ePropList);
            }
          }

          for (TorcSerializedEdge serEdge : serEdgeList) {
            if (nbrDedupMap.containsKey(serEdge.vertexId)) {
              nList.add(nbrDedupMap.get(serEdge.vertexId));
            } else {
              TorcVertex v = new TorcVertex(this, serEdge.vertexId, nLabel);
              nList.add(v);
              nbrDedupMap.put(serEdge.vertexId, v);
              uniqNbrSet.add(v);
            }

            if (fillEdge)
              ePropList.add((Map<Object, Object>)TorcHelper.deserializeObject(
                    serEdge.serializedProperties));
          }
        }

        i++;
      }
    }    

    return new TraversalResult(nbrListMap, ePropListMap, uniqNbrSet);
  }

  public void fillProperties(TorcVertex v) {
    fillProperties(Collections.singletonList(v));
  }

  public void fillProperties(TraversalResult ... rs) {
    if (rs.length == 1) {
      fillProperties(rs[0].vSet);
    } else {
      int totalSize = 0;
      for (TraversalResult r : rs)
        totalSize += r.vSet.size();

      List<TorcVertex> vList = new ArrayList<>(totalSize);
      for (TraversalResult r : rs)
        vList.addAll(r.vSet);

      fillProperties(vList);
    }
  }

  public void fillProperties(Iterable<TorcVertex> vertices) {
    initialize();

    torcGraphTx.readWrite();

    // Max number of reads to issue in a multiread / batch
    int DEFAULT_MAX_MULTIREAD_SIZE = 1 << 11; 

    if (txMode) {
//      RAMCloudTransaction rctx = torcGraphTx.getThreadLocalRAMCloudTx();
//
//      // Keeps track of where we are in the vList.
//      int vListMarker = 0;
//
//      while (vListMarker < vList.size()) {
//        // Queue up a batchSize of asynchronous ReadOps.
//        int batchSize = Math.min(vList.size() - vListMarker, 
//            DEFAULT_MAX_MULTIREAD_SIZE);
//
//        RAMCloudTransactionReadOp[] readOps = 
//          new RAMCloudTransactionReadOp[batchSize];
//        for (int i = 0; i < batchSize; i++) {
//          readOps[i] = new RAMCloudTransactionReadOp(rctx, vertexTableId,
//              TorcHelper.getVertexPropertiesKey(vList.get(vListMarker + i).id()), 
//              true);
//        }
//
//        for (int i = 0; i < batchSize; i++) {
//          TorcVertex v = vList.get(vListMarker + i);
//
//          RAMCloudObject obj;
//          try {
//            obj = readOps[i].getValue();
//            if (obj == null) {
//              // This vertex has no properties set.
//              v.setProperties(new HashMap<>());
//              continue;
//            }
//          } catch (ClientException e) {
//            throw new RuntimeException(e);
//          } finally {
//            readOps[i].close();
//          }
//
//          Map<Object, Object> properties = 
//            (Map<Object, Object>)TorcHelper.deserializeObject(obj.getValueBytes());
//          v.setProperties(properties);
//        }
//
//        vListMarker += batchSize;
//      }
    } else {
      RAMCloud client = threadLocalClientMap.get(Thread.currentThread());

      Iterator<TorcVertex> it = vertices.iterator();
      LinkedList<MultiReadObject> requestQ = new LinkedList<>();
      LinkedList<TorcVertex> vertexQ = new LinkedList<>();
      while (it.hasNext()) {
        TorcVertex v = it.next();
        requestQ.addLast(new MultiReadObject(vertexTableId, 
              TorcHelper.getVertexPropertiesKey(v.id())));
        vertexQ.addLast(v);
        if (requestQ.size() == DEFAULT_MAX_MULTIREAD_SIZE) {
          MultiReadObject[] requests = new MultiReadObject[requestQ.size()];
          for (int i = 0; i < requests.length; i++) {
            requests[i] = requestQ.removeFirst();
          }

          client.read(requests);
        
          for (int i = 0; i < requests.length; i++) {
            v = vertexQ.removeFirst();

            if (requests[i].getStatus() != Status.STATUS_OK) {
              if (requests[i].getStatus() == Status.STATUS_OBJECT_DOESNT_EXIST) {
                // This vertex has no properties set.
                v.setProperties(new HashMap<>());
                continue;
              } else {
                throw new RuntimeException(
                    "Vertex properties RAMCloud object had status " + 
                    requests[i].getStatus());
              }
            }

            Map<Object, Object> properties = (Map<Object, Object>)
              TorcHelper.deserializeObject(requests[i].getValueBytes());
            v.setProperties(properties);
          } 
        } 
      }

      if (requestQ.size() > 0) {
        MultiReadObject[] requests = new MultiReadObject[requestQ.size()];
        for (int i = 0; i < requests.length; i++) {
          requests[i] = requestQ.removeFirst();
        }

        client.read(requests);
      
        for (int i = 0; i < requests.length; i++) {
          TorcVertex v = vertexQ.removeFirst();

          if (requests[i].getStatus() != Status.STATUS_OK) {
            if (requests[i].getStatus() == Status.STATUS_OBJECT_DOESNT_EXIST) {
              // This vertex has no properties set.
              v.setProperties(new HashMap<>());
              continue;
            } else {
              throw new RuntimeException(
                  "Vertex properties RAMCloud object had status " + 
                  requests[i].getStatus());
            }
          }

          Map<Object, Object> properties = (Map<Object, Object>)
            TorcHelper.deserializeObject(requests[i].getValueBytes());
          v.setProperties(properties);
        }
      }
    }
  }

  public void enableTx() {
    txMode = true;
  }

  public void disableTx() {
    txMode = false;
  }

  public boolean getTxMode() {
    return txMode;
  }

  public RAMCloud getClient() {
    return threadLocalClientMap.get(Thread.currentThread());
  }

  public boolean isInitialized() {
    return initialized;
  }

  /*
   * Specialized method for quickly loading a vertex. Method works by executing
   * essentially a blind write of a vertex into the graph, without checking
   * whether or not the vertex exists. Assumes that the vertex has an ID already
   * set. Does not perform normal checks on the properties. If outputFile is
   * non-null, then instead of writing the writing the vertex into RAMCloud, the
   * vertex's RAMCloud key-value serialization is appended to the given file.
   */
  public void loadVertex(UInt128 vertexId, String label, 
      Map<Object, Object> properties) {
    List<byte[]> keys = new ArrayList<>();
    List<byte[]> values = new ArrayList<>();

    // Label
    keys.add(TorcHelper.getVertexLabelKey(vertexId));
    values.add(TorcHelper.serializeObject(label));

    // Properties
    keys.add(TorcHelper.getVertexPropertiesKey(vertexId));
    values.add(TorcHelper.serializeObject(properties));

    for (int i = 0; i < keys.size(); i++) {
      byte[] key = keys.get(i);
      byte[] value = values.get(i);

      ByteBuffer buffer = ByteBuffer.allocate(
          Integer.BYTES +
          key.length +
          Integer.BYTES +
          value.length)
          .order(ByteOrder.LITTLE_ENDIAN);

      buffer.putInt(key.length);
      buffer.put(key);
      buffer.putInt(value.length);
      buffer.put(value);

      try {
        vertexTableOS.write(buffer.array());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /*
   * Specialized method for quickly loading an edge list for a vertex in one
   * direction. Edge lists for the neighbor vertices must be added separately.
   */
  public void loadEdges(final UInt128 baseVertexId, final String edgeLabel,
      final Direction direction, final String neighborLabel, 
      final List<UInt128> neighborIds, 
      final List<Map<Object, Object>> propMaps) {
    byte[] keyPrefix =
        TorcHelper.getEdgeListKeyPrefix(baseVertexId, edgeLabel, direction,
            neighborLabel);

    List<byte[]> serializedPropList = new ArrayList<>(propMaps.size());
    for (int i = 0; i < propMaps.size(); i++) {
      serializedPropList.add(TorcHelper.serializeObject(propMaps.get(i)));
    }

    TorcEdgeList.writeListToFile(edgeListTableOS, keyPrefix, neighborIds,
        serializedPropList);
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
      final List<TorcVertex> vertices,
      final Direction direction,
      final String[] edgeLabels,
      final List<String> neighborLabels) {
    torcGraphTx.readWrite();
    RAMCloudTransaction rctx = torcGraphTx.getThreadLocalRAMCloudTx();
    RAMCloud client = threadLocalClientMap.get(Thread.currentThread());

    /* Build arguments to TorcEdgeList.batchRead(). */
    List<byte[]> brKeyPrefixes = new ArrayList<>();
    List<Vertex> brVertexList = new ArrayList<>();
    List<UInt128> brBaseVertexIds = new ArrayList<>();
    List<String> brEdgeLabels = new ArrayList<>();
    List<Direction> brDirections = new ArrayList<>();
    List<String> brNeighborLabels = new ArrayList<>();

    List<String> eLabels;
    if (edgeLabels != null) {
      eLabels = Arrays.asList(edgeLabels);
    } else {
      throw new UnsupportedOperationException("Must specify the edge labels when fetching vertex neighbors.");
    }

    if (neighborLabels == null) {
      throw new UnsupportedOperationException("Must specify the neighbor vertex labels when fetching vertex neighbors.");
    }

    List<Direction> eDirs = new ArrayList<>();
    switch (direction) {
      case OUT:
        eDirs.add(Direction.OUT);
        break;
      case IN:
        eDirs.add(Direction.IN);
        break;
      case BOTH:
        eDirs.add(Direction.OUT);
        eDirs.add(Direction.IN);
        break;
      default:
        throw new UnsupportedOperationException("Unknown direction type: " + direction);
    }

    for (TorcVertex vertex : vertices) {
      for (String edgeLabel : eLabels) {
        for (Direction edgeDir : eDirs) {
          for (String neighborLabel : neighborLabels) {
            brKeyPrefixes.add(TorcHelper.getEdgeListKeyPrefix(vertex.id(), 
                  edgeLabel, direction, neighborLabel));
            brVertexList.add(vertex);
            brBaseVertexIds.add(vertex.id());
            brEdgeLabels.add(edgeLabel);
            brDirections.add(edgeDir);
            brNeighborLabels.add(neighborLabel);
          }
        }
      }
    }

    Map<byte[], List<TorcEdge>> edgeListMap;
    if (txMode) {
      edgeListMap = TorcEdgeList.batchRead(rctx, 
          edgeListTableId, brKeyPrefixes, this, brBaseVertexIds, brEdgeLabels, 
          brDirections);
    } else {
      edgeListMap = TorcEdgeList.batchRead(client, 
          edgeListTableId, brKeyPrefixes, this, brBaseVertexIds, brEdgeLabels, 
          brDirections);
    }
    
    Map<Vertex, List<Vertex>> map = new HashMap<>();

    for (int i = 0; i < brKeyPrefixes.size(); i++) {
      byte[] keyPrefix = brKeyPrefixes.get(i);
      Vertex v = brVertexList.get(i);
      UInt128 baseVertexId = brBaseVertexIds.get(i);
      String edgeLabel = brEdgeLabels.get(i);
      Direction dir = brDirections.get(i);
      String neighborLabel = brNeighborLabels.get(i);

      List<TorcEdge> edgeList = edgeListMap.get(keyPrefix);

      List<Vertex> neighborList;
      if (map.containsKey(v)) {
        neighborList = map.get(v);
      } else {
        neighborList = new ArrayList<>();
        map.put(v, neighborList);
      }

      for (TorcEdge edge : edgeList) {
        if (dir == Direction.OUT) {
          neighborList.add(new TorcVertex(this, edge.getV2Id(), neighborLabel));
        } else {
          neighborList.add(new TorcVertex(this, edge.getV1Id(), neighborLabel));
        }
      }
    }    

    Map<Vertex, Iterator<Vertex>> retMap = new HashMap<>();
    for (Map.Entry<Vertex, List<Vertex>> entry : map.entrySet()) {
      retMap.put(entry.getKey(), entry.getValue().iterator());
    }
    
    return retMap;
  }

  /** 
   * Fetches the incident edges of a whole set of vertices in bulk, given a set
   * of edge labels and a direction. Takes advantage of TorcGraph's ability to
   * fetch data efficiently in parallel from RAMCloud.  
   *
   * @param vertices Set of vertices to use as base vertices.
   * @param direction Edge direction to traverse.
   * @param edgeLabels Set of edge labels to traverse.
   * @param neighborLabels List of neighbor vertex labels.
   *
   * @return Map of base vertex to array of incident edges.
   */
  public Map<Vertex, Iterator<Edge>> vertexEdges(
      final List<TorcVertex> vertices,
      final Direction direction,
      final String[] edgeLabels,
      final List<String> neighborLabels) {
    torcGraphTx.readWrite();
    RAMCloudTransaction rctx = torcGraphTx.getThreadLocalRAMCloudTx();
    RAMCloud client = threadLocalClientMap.get(Thread.currentThread());

    /* Build arguments to TorcEdgeList.batchRead(). */
    List<byte[]> brKeyPrefixes = new ArrayList<>();
    List<Vertex> brVertexList = new ArrayList<>();
    List<UInt128> brBaseVertexIds = new ArrayList<>();
    List<String> brEdgeLabels = new ArrayList<>();
    List<Direction> brDirections = new ArrayList<>();
    List<String> brNeighborLabels = new ArrayList<>();

    List<String> eLabels;
    if (edgeLabels != null) {
      eLabels = Arrays.asList(edgeLabels);
    } else {
      throw new UnsupportedOperationException("Must specify the edge labels when fetching vertex edges.");
    }

    if (neighborLabels == null) {
      throw new UnsupportedOperationException("Must specify the neighbor vertex labels when fetching vertex edges.");
    }

    List<Direction> eDirs = new ArrayList<>();
    switch (direction) {
      case OUT:
        eDirs.add(Direction.OUT);
        break;
      case IN:
        eDirs.add(Direction.IN);
        break;
      case BOTH:
        eDirs.add(Direction.OUT);
        eDirs.add(Direction.IN);
        break;
      default:
        throw new UnsupportedOperationException("Unknown direction type: " + direction);
    }

    for (TorcVertex vertex : vertices) {
      for (String edgeLabel : eLabels) {
        for (Direction edgeDir : eDirs) {
          for (String neighborLabel : neighborLabels) {
            brKeyPrefixes.add(TorcHelper.getEdgeListKeyPrefix(vertex.id(), 
                  edgeLabel, direction, neighborLabel));
            brVertexList.add(vertex);
            brBaseVertexIds.add(vertex.id());
            brEdgeLabels.add(edgeLabel);
            brDirections.add(edgeDir);
            brNeighborLabels.add(neighborLabel);
          }
        }
      }
    }

    Map<byte[], List<TorcEdge>> edgeListMap;
    if (txMode) {
      edgeListMap = TorcEdgeList.batchRead(rctx, 
          edgeListTableId, brKeyPrefixes, this, brBaseVertexIds, brEdgeLabels, 
          brDirections); 
    } else {
      edgeListMap = TorcEdgeList.batchRead(client, 
          edgeListTableId, brKeyPrefixes, this, brBaseVertexIds, brEdgeLabels, 
          brDirections);
    }
    
    Map<Vertex, List<Edge>> map = new HashMap<>();

    for (int i = 0; i < brKeyPrefixes.size(); i++) {
      byte[] keyPrefix = brKeyPrefixes.get(i);
      Vertex v = brVertexList.get(i);
      UInt128 baseVertexId = brBaseVertexIds.get(i);
      String edgeLabel = brEdgeLabels.get(i);
      Direction dir = brDirections.get(i);
      String neighborLabel = brNeighborLabels.get(i);

      List<TorcEdge> edgeList = edgeListMap.get(keyPrefix);

      List<Edge> incidentEdgeList;
      if (map.containsKey(v)) {
        incidentEdgeList = map.get(v);
      } else {
        incidentEdgeList = new ArrayList<>();
        map.put(v, incidentEdgeList);
      }

      incidentEdgeList.addAll(edgeList);
    }    

    Map<Vertex, Iterator<Edge>> retMap = new HashMap<>();
    for (Map.Entry<Vertex, List<Edge>> entry : map.entrySet()) {
      retMap.put(entry.getKey(), entry.getValue().iterator());
    }
    
    return retMap;
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
    torcGraphTx.doRollbackAllThreads();
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
          new RAMCloud(coordinatorLocator, "main", dpdkPort));

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
    RAMCloud client = threadLocalClientMap.get(Thread.currentThread());

    RAMCloudObject neighborLabelRCObj;

    if (txMode) {
        neighborLabelRCObj = rctx.read(vertexTableId, 
            TorcHelper.getVertexLabelKey(v.id()));
    } else {
        neighborLabelRCObj = client.read(vertexTableId, 
            TorcHelper.getVertexLabelKey(v.id()));
    }

    if (neighborLabelRCObj == null) {
      throw new RuntimeException("Tried to read label for vertex but " +
          "RAMCloud object does not exist");
    }

    return (String)
      TorcHelper.deserializeObject(neighborLabelRCObj.getValueBytes());
  }

  void removeVertex(final TorcVertex vertex) {
    throw Vertex.Exceptions.vertexRemovalNotSupported();
  }

  Edge addEdge(final TorcVertex vertex1, final TorcVertex vertex2,
      final String edgeLabel, final Object[] keyValues) {
    torcGraphTx.readWrite();
    RAMCloudTransaction rctx = torcGraphTx.getThreadLocalRAMCloudTx();
    RAMCloud client = threadLocalClientMap.get(Thread.currentThread());

    if (vertex1 == null || vertex2 == null) {
      throw Graph.Exceptions.argumentCanNotBeNull("vertex");
    }

    ElementHelper.validateLabel(edgeLabel);

    TorcHelper.legalPropertyKeyValueArray(Edge.class, keyValues);

    // Create property map.
    Map<Object, Object> properties = new HashMap<>();
    for (int i = 0; i < keyValues.length; i = i + 2) {
      Object key = keyValues[i];
      Object val = keyValues[i + 1];
      if (properties.containsKey(key)) {
        if (properties.get(key) instanceof List) {
          ((List)properties.get(key)).add(val);
        } else {
          // Convert this to a list.
          List<Object> list = new ArrayList<>();
          list.add(properties.get(key));
          list.add(val);
          properties.put(key, list);
        }
      } else {
        properties.put(key, val);
      }
    }

    byte[] serializedProperties =
        TorcHelper.serializeObject(properties);

    /*
     * Add one vertex to the other's edge list, and vice versa.
     */
    for (int i = 0; i < 2; ++i) {
      TorcVertex baseVertex;
      TorcVertex neighborVertex;
      Direction direction;

      /*
       * Choose which vertex acts as the base and which acts as the neighbor in
       * this half of the edge addition operation.
       */
      if (i == 0) {
        baseVertex = vertex1;
        neighborVertex = vertex2;
        direction = Direction.OUT;
      } else {
        baseVertex = vertex2;
        neighborVertex = vertex1;
        direction = Direction.IN;
      }

      String neighborLabel = neighborVertex.label();

      byte[] keyPrefix =
          TorcHelper.getEdgeListKeyPrefix(baseVertex.id(), edgeLabel, direction,
              neighborLabel);

      boolean newListCreated;
      if (txMode) {
        newListCreated = TorcEdgeList.prepend(rctx, edgeListTableId, keyPrefix, 
            neighborVertex.id(), serializedProperties);
      } else {
        newListCreated = TorcEdgeList.prepend(client, edgeListTableId, keyPrefix, 
            neighborVertex.id(), serializedProperties);
      }
    }

    return new TorcEdge(this, vertex1.id(), vertex2.id(), edgeLabel,
        properties, serializedProperties);
  }

  Iterator<Edge> vertexEdges(final TorcVertex vertex, final Direction direction,
      final String[] edgeLabels, final String[] neighborLabels) {
    initialize();

    torcGraphTx.readWrite();
    RAMCloudTransaction rctx = torcGraphTx.getThreadLocalRAMCloudTx();
    RAMCloud client = threadLocalClientMap.get(Thread.currentThread());

    List<Edge> edges = new ArrayList<>();
    List<String> eLabels = Arrays.asList(edgeLabels);
    List<String> nLabels = Arrays.asList(neighborLabels);

    if (eLabels.isEmpty() || nLabels.isEmpty()) {
      throw new UnsupportedOperationException("Must specify the edge labels " + 
          "and neighbor vertex labels when fetching vertex edges.");
    }

    List<Direction> edgeDirections = new ArrayList<>();
    switch (direction) {
      case OUT:
        edgeDirections.add(Direction.OUT);
        break;
      case IN:
        edgeDirections.add(Direction.IN);
        break;
      case BOTH:
        edgeDirections.add(Direction.OUT);
        edgeDirections.add(Direction.IN);
        break;
      default:
        throw new UnsupportedOperationException("Unknown direction type: " + 
            direction);
    }

    for (String edgeLabel : eLabels) {
      for (Direction dir : edgeDirections) {
        for (String neighborLabel : nLabels) {
          byte[] keyPrefix = TorcHelper.getEdgeListKeyPrefix(vertex.id(), 
              edgeLabel, dir, neighborLabel);

          List<TorcEdge> edgeList;
          if (txMode) {
            edgeList = TorcEdgeList.read(rctx, edgeListTableId, 
                keyPrefix, this, vertex.id(), edgeLabel, dir);
          } else {
            edgeList = TorcEdgeList.read(client, edgeListTableId, 
                keyPrefix, this, vertex.id(), edgeLabel, dir);
          }

          edges.addAll(edgeList);
        }
      }
    }

    return edges.iterator();
  }

  Iterator<Vertex> vertexNeighbors(final TorcVertex vertex, 
      final Direction direction, final String[] edgeLabels, 
      final String[] neighborLabels) {
    torcGraphTx.readWrite();
    RAMCloudTransaction rctx = torcGraphTx.getThreadLocalRAMCloudTx();
    RAMCloud client = threadLocalClientMap.get(Thread.currentThread());

    List<Vertex> vertices = new ArrayList<>();
    List<String> eLabels = Arrays.asList(edgeLabels);
    List<String> nLabels = Arrays.asList(neighborLabels);

    if (eLabels.isEmpty() || nLabels.isEmpty()) {
      throw new UnsupportedOperationException("Must specify the edge labels and neighbor vertex labels when fetching vertex neighbors.");
    }

    List<Direction> edgeDirections = new ArrayList<>();
    switch (direction) {
      case OUT:
        edgeDirections.add(Direction.OUT);
        break;
      case IN:
        edgeDirections.add(Direction.IN);
        break;
      case BOTH:
        edgeDirections.add(Direction.OUT);
        edgeDirections.add(Direction.IN);
        break;
      default:
        throw new UnsupportedOperationException("Unknown direction type: " + direction);
    }

    for (String edgeLabel : eLabels) {
      for (Direction dir : edgeDirections) {
        for (String neighborLabel : nLabels) {
          byte[] keyPrefix = TorcHelper.getEdgeListKeyPrefix(vertex.id(), 
              edgeLabel, dir, neighborLabel);

          List<TorcEdge> edgeList;
          if (txMode) {
            edgeList = TorcEdgeList.read(rctx, edgeListTableId, 
                keyPrefix, this, vertex.id(), edgeLabel, dir);
          } else {
            edgeList = TorcEdgeList.read(client, edgeListTableId, 
                keyPrefix, this, vertex.id(), edgeLabel, dir);
          }

          for (TorcEdge edge : edgeList) {
            if (dir == Direction.OUT) {
              vertices.add(new TorcVertex(this, edge.getV2Id(), 
                    neighborLabel));
            } else {
              vertices.add(new TorcVertex(this, edge.getV1Id(), 
                    neighborLabel));
            } 
          }
        }
      }
    }

    return vertices.iterator();
  }

  <V> Iterator<VertexProperty<V>> getVertexProperties(final TorcVertex vertex,
      final String[] propertyKeys) {
    torcGraphTx.readWrite();
    RAMCloudTransaction rctx = torcGraphTx.getThreadLocalRAMCloudTx();
    RAMCloud client = threadLocalClientMap.get(Thread.currentThread());

    RAMCloudObject obj;
    if (txMode) {
      obj  = rctx.read(vertexTableId,
        TorcHelper.getVertexPropertiesKey(vertex.id()));
    } else {
      obj  = client.read(vertexTableId,
        TorcHelper.getVertexPropertiesKey(vertex.id()));
    }

    Map<Object, Object> properties;
    if (obj != null) {
      properties = 
        (Map<Object, Object>)TorcHelper.deserializeObject(obj.getValueBytes());
    } else {
      properties = new HashMap<>();
    }

    List<VertexProperty<V>> propList = new ArrayList<>();

    if (propertyKeys.length > 0) {
      for (String key : propertyKeys) {
        if (properties.containsKey(key)) {
          propList.add(new TorcVertexProperty(vertex, key, properties.get(key)));
        } else {
          throw Property.Exceptions.propertyDoesNotExist(vertex, key);
        }
      }
    } else {
      for (Map.Entry<Object, Object> property : properties.entrySet()) {
        String key = (String)property.getKey();
        propList.add(new TorcVertexProperty(vertex, key, property.getValue()));
      }
    }

    return propList.iterator();
  }

  <V> VertexProperty<V> setVertexProperty(final TorcVertex vertex,
      final VertexProperty.Cardinality cardinality, final String key,
      final V value, final Object[] keyValues) {
    torcGraphTx.readWrite();
    RAMCloudTransaction rctx = torcGraphTx.getThreadLocalRAMCloudTx();
    RAMCloud client = threadLocalClientMap.get(Thread.currentThread());

    if (!(keyValues == null || keyValues.length == 0)) {
      throw VertexProperty.Exceptions.metaPropertiesNotSupported();
    }

    if (!(value instanceof String)) {
      throw Property.Exceptions.dataTypeOfPropertyValueNotSupported(value);
    }

    RAMCloudObject obj; 
    if (txMode) {
      obj  = rctx.read(vertexTableId,
          TorcHelper.getVertexPropertiesKey(vertex.id()));
    } else {
      obj  = client.read(vertexTableId,
          TorcHelper.getVertexPropertiesKey(vertex.id()));
    }

    Map<Object, Object> properties;
    if (obj != null) {
      properties = 
        (Map<Object, Object>)TorcHelper.deserializeObject(obj.getValueBytes());
    } else {
      properties = new HashMap<>();
    }

    if (properties.containsKey(key)) {
      if (cardinality == VertexProperty.Cardinality.single) {
        properties.put(key, value);
      } else if (cardinality == VertexProperty.Cardinality.list) {
        ((List)properties.get(key)).add(value);
      } else if (cardinality == VertexProperty.Cardinality.set) {
        if (!((Set)properties.get(key)).contains(value)) {
          ((Set)properties.get(key)).add(value);
        }
      } else {
        throw new UnsupportedOperationException("Do not recognize Cardinality "
            + "of this type: " + cardinality.toString());
      }
    } else {
      properties.put(key, value);
    }

    if (txMode) {
      rctx.write(vertexTableId, TorcHelper.getVertexPropertiesKey(vertex.id()),
          TorcHelper.serializeObject(properties));
    } else {
      client.write(vertexTableId, TorcHelper.getVertexPropertiesKey(vertex.id()),
          TorcHelper.serializeObject(properties), null);
    }

    return new TorcVertexProperty(vertex, key, value);
  }

  void removeEdge(final TorcEdge edge) {
    throw Edge.Exceptions.edgeRemovalNotSupported();
  }

  Iterator<Vertex> edgeVertices(final TorcEdge edge,
      final Direction direction) {
    torcGraphTx.readWrite();
    RAMCloudTransaction rctx = torcGraphTx.getThreadLocalRAMCloudTx();
    RAMCloud client = threadLocalClientMap.get(Thread.currentThread());

    List<Vertex> list = new ArrayList<>();

    if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH)) {
      RAMCloudObject obj; 
      if (txMode) {
        obj  = rctx.read(vertexTableId,
            TorcHelper.getVertexLabelKey(edge.getV1Id()));
      } else {
        obj  = client.read(vertexTableId,
            TorcHelper.getVertexLabelKey(edge.getV1Id()));
      }

      if (obj == null) {
        throw Graph.Exceptions.elementNotFound(TorcVertex.class,
            edge.getV1Id());
      }

      list.add(new TorcVertex(this, edge.getV1Id(), 
            (String)TorcHelper.deserializeObject(obj.getValueBytes())));
    }

    if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH)) {
      RAMCloudObject obj; 
      if (txMode) {
        obj  = rctx.read(vertexTableId,
            TorcHelper.getVertexLabelKey(edge.getV2Id()));
      } else {
        obj  = client.read(vertexTableId,
            TorcHelper.getVertexLabelKey(edge.getV2Id()));
      }

      if (obj == null) {
        throw Graph.Exceptions.elementNotFound(TorcVertex.class,
            edge.getV2Id());
      }

      list.add(new TorcVertex(this, edge.getV2Id(), 
            (String)TorcHelper.deserializeObject(obj.getValueBytes())));
    }

    return list.iterator();
  }

  <V> Iterator<Property<V>> getEdgeProperties(final TorcEdge edge,
      final String[] propertyKeys) {
    List<Property<V>> propList = new ArrayList<>();

    Map<Object, Object> propMap = edge.getProperties();

    for (String key : propertyKeys) {
      if (key == null) {
        throw Property.Exceptions.propertyKeyCanNotBeNull();
      } else if (key.length() == 0) {
        throw Property.Exceptions.propertyKeyCanNotBeEmpty();
      }

      if (propMap.containsKey(key)) {
        Object value = propMap.get(key);
        propList.add(new TorcProperty(edge, key, value));
      }
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
      Thread us = Thread.currentThread();
      if (threadLocalRCTXMap.get(us) == null) {
        RAMCloud client = threadLocalClientMap.get(us);
        threadLocalRCTXMap.put(us, new RAMCloudTransaction(client));
      } else {
        throw Transaction.Exceptions.transactionAlreadyOpen();
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
    }

    @Override
    public void doRollback() throws AbstractTransaction.TransactionException {
      RAMCloudTransaction rctx =
          threadLocalRCTXMap.get(Thread.currentThread());

      try {
        rctx.close();
      } catch (Exception e) {
        throw new AbstractTransaction.TransactionException(e);
      } finally {
        threadLocalRCTXMap.remove(Thread.currentThread());
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

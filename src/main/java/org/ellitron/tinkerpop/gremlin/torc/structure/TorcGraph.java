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
// TODO: Change license to match RAMCloud
package org.ellitron.tinkerpop.gremlin.torc.structure;

import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import edu.stanford.ramcloud.*;
import edu.stanford.ramcloud.transactions.*;
import edu.stanford.ramcloud.ClientException.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;

import org.apache.commons.configuration.Configuration;

import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.util.AbstractTransaction;
import org.ellitron.tinkerpop.gremlin.torc.structure.util.TorcHelper;

/**
 * TODO: Write documentation - Bidirectional edges
 *
 *
 * @author Jonathan Ellithorpe <jde@cs.stanford.edu>
 *
 * TODO: Make TorcGraph thread safe. Currently every TorcGraph method that
 * performs reads or writes to the database uses a ThreadLocal
 * RAMCloudTransaction to isolate transactions from different threads. Each
 * ThreadLocal RAMCloudTransaction object, however, is constructed with a
 * reference to the same RAMCloud client object, which itself is not thread
 * safe. Therefore, TorcGraph, although using separate RAMCloudTransaction
 * objects for each thread, it not actually thread safe. To fix this problem,
 * each ThreadLocal RAMCloudTransaction object needs to be constructed with its
 * own RAMCloud client object.
 *
 * TODO: Implement way of handling objects that are larger than 1MB.
 */
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
public final class TorcGraph implements Graph {

    private static final Logger logger = Logger.getLogger(TorcGraph.class);

    // Configuration keys.
    public static final String CONFIG_GRAPH_NAME = "gremlin.torc.graphName";
    public static final String CONFIG_COORD_LOCATOR = "gremlin.torc.coordinatorLocator";
    public static final String CONFIG_NUM_MASTER_SERVERS = "gremlin.torc.numMasterServers";
    public static final String CONFIG_LOG_LEVEL = "gremlin.torc.logLevel";

    // Constants.
    private static final String ID_TABLE_NAME = "idTable";
    private static final String VERTEX_TABLE_NAME = "vertexTable";
    private static final String EDGE_TABLE_NAME = "edgeTable";
    private static final int MAX_TX_RETRY_COUNT = 100;
    private static final int NUM_ID_COUNTERS = 16;

    // Normal private members.
    private final Configuration configuration;
    private final String coordinatorLocator;
    private final int totalMasterServers;
    private RAMCloud ramcloud;
    private long idTableId, vertexTableId, edgeTableId;
    private final String graphName;
    private final TorcGraphTransaction torcGraphTx = new TorcGraphTransaction();

    boolean initialized = false;

    private TorcGraph(final Configuration configuration) {
        this.configuration = configuration;

        graphName = configuration.getString(CONFIG_GRAPH_NAME);
        coordinatorLocator = configuration.getString(CONFIG_COORD_LOCATOR);
        totalMasterServers = configuration.getInt(CONFIG_NUM_MASTER_SERVERS);

        logger.debug(String.format("Constructing TorcGraph (%s,%s,%d)", graphName, coordinatorLocator, totalMasterServers));
    }

    public boolean isInitialized() {
        return initialized;
    }

    private synchronized void initialize() {
        if (!initialized) {
            try {
                ramcloud = new RAMCloud(coordinatorLocator);
            } catch (ClientException e) {
                System.out.println(e.toString());
                throw e;
            }

            idTableId = ramcloud.createTable(graphName + "_" + ID_TABLE_NAME, totalMasterServers);
            vertexTableId = ramcloud.createTable(graphName + "_" + VERTEX_TABLE_NAME, totalMasterServers);
            edgeTableId = ramcloud.createTable(graphName + "_" + EDGE_TABLE_NAME, totalMasterServers);

            initialized = true;
            
            logger.debug("Initialized");
        }
    }

    public static TorcGraph open(final Configuration configuration) {
        return new TorcGraph(configuration);
    }

    @Override
    public Vertex addVertex(final Object... keyValues) {
        long startTimeNs = 0;
        if (logger.isDebugEnabled()) {
            startTimeNs = System.nanoTime();
        }

        if (!initialized) {
            initialize();
        }

        torcGraphTx.readWrite();
        RAMCloudTransaction rctx = torcGraphTx.getThreadLocalRAMCloudTx();

        ElementHelper.legalPropertyKeyValueArray(keyValues);

        // Only values of type String supported, currently.
        for (int i = 0; i < keyValues.length; i = i + 2) {
            if (!(keyValues[i] instanceof T) && !(keyValues[i + 1] instanceof String)) {
                throw Property.Exceptions.dataTypeOfPropertyValueNotSupported(keyValues[i + 1]);
            }
        }

        Object idValue = ElementHelper.getIdValue(keyValues).orElse(null);
        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);

        UInt128 vertexId;
        if (idValue != null) {
            vertexId = UInt128.decode(idValue);

            // Check if a vertex with this ID already exists.
            try {
                rctx.read(vertexTableId, TorcHelper.getVertexLabelKey(vertexId));
                throw Graph.Exceptions.vertexWithIdAlreadyExists(vertexId.toString());
            } catch (ObjectDoesntExistException e) {
                // Good!
            }
        } else {
            long id_counter = (long) (Math.random() * NUM_ID_COUNTERS);
            long id = ramcloud.incrementInt64(idTableId, Long.toString(id_counter).getBytes(), 1, null);

            vertexId = new UInt128((1L << 63) + id_counter, id);
        }

        // Create property map.
        Map<String, String> properties = new HashMap<>();
        for (int i = 0; i < keyValues.length; i = i + 2) {
            if (keyValues[i] instanceof String) {
                properties.put((String) keyValues[i], (String) keyValues[i + 1]);
            }
        }

        rctx.write(vertexTableId, TorcHelper.getVertexLabelKey(vertexId), label.getBytes());
        rctx.write(vertexTableId, TorcHelper.getVertexPropertiesKey(vertexId), TorcHelper.serializeProperties(properties).array());

        if (logger.isDebugEnabled()) {
            long endTimeNs = System.nanoTime();
            int propLength = TorcHelper.serializeProperties(properties).array().length;
            logger.debug(String.format("addVertex(id=%s,label=%s,propLen=%d), took %dus", vertexId.toString(), label, propLength, (endTimeNs - startTimeNs) / 1000l));
        }

        return new TorcVertex(this, vertexId, label);
    }

    @Override
    public <C extends GraphComputer> C compute(final Class<C> type) throws IllegalArgumentException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Iterator<Vertex> vertices(final Object... vertexIds) {
        long startTimeNs = 0;
        if (logger.isDebugEnabled()) {
            startTimeNs = System.nanoTime();
        }

        if (!initialized) {
            initialize();
        }

        torcGraphTx.readWrite();
        RAMCloudTransaction rctx = torcGraphTx.getThreadLocalRAMCloudTx();

        ElementHelper.validateMixedElementIds(TorcVertex.class, vertexIds);

        List<Vertex> list = new ArrayList<>();
        if (vertexIds.length > 0) {
            if (vertexIds[0] instanceof TorcVertex) {
                Arrays.asList(vertexIds).forEach((id) -> {list.add((Vertex) id);});
            } else {
                for (int i = 0; i < vertexIds.length; ++i) {
                    UInt128 vertexId = UInt128.decode(vertexIds[i]);

                    RAMCloudObject obj;
                    try {
                        obj = rctx.read(vertexTableId, TorcHelper.getVertexLabelKey(vertexId));
                    } catch (ObjectDoesntExistException e) {
                        throw Graph.Exceptions.elementNotFound(TorcVertex.class, vertexIds[i]);
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
                        RAMCloudObject obj = rctx.read(vertexTableId, TorcHelper.getVertexLabelKey(vertexId));
                        list.add(new TorcVertex(this, vertexId, obj.getValue()));
                    } catch (ObjectDoesntExistException e) {
                        // Continue...
                    }
                }
            }
        }

        if (logger.isDebugEnabled()) {
            long endTimeNs = System.nanoTime();
            logger.debug(String.format("vertices(n=%d), took %dus", vertexIds.length, (endTimeNs - startTimeNs) / 1000l));
        }

        return list.iterator();
    }

    @Override
    public Iterator<Edge> edges(final Object... edgeIds) {
        long startTimeNs = 0;
        if (logger.isDebugEnabled()) {
            startTimeNs = System.nanoTime();
        }

        if (!initialized) {
            initialize();
        }

        torcGraphTx.readWrite();
        RAMCloudTransaction rctx = torcGraphTx.getThreadLocalRAMCloudTx();

        ElementHelper.validateMixedElementIds(TorcEdge.class, edgeIds);

        List<Edge> list = new ArrayList<>();
        if (edgeIds.length > 0) {
            if (edgeIds[0] instanceof TorcEdge) {
                list = Arrays.asList((Edge[]) edgeIds);
            } else if (edgeIds[0] instanceof TorcEdge.Id) {
                for (int i = 0; i < edgeIds.length; ++i) {
                    list.add(((TorcEdge.Id) edgeIds[i]).getEdge());
                }
            } else {
                throw Graph.Exceptions.elementNotFound(TorcEdge.class, edgeIds[0]);
            }
        } else {
            long max_id[] = new long[NUM_ID_COUNTERS];

            for (int i = 0; i < NUM_ID_COUNTERS; ++i) {
                try {
                    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
                    buffer.order(ByteOrder.LITTLE_ENDIAN);
                    buffer.put(rctx.read(idTableId, Integer.toString(i)).getValueBytes());
                    buffer.flip();
                    max_id[i] = buffer.getLong();
                } catch (ObjectDoesntExistException e) {
                    max_id[i] = 0;
                }
            }

            for (int i = 0; i < NUM_ID_COUNTERS; ++i) {
                for (long j = 1; j <= max_id[i]; ++j) {
                    UInt128 baseVertexId = new UInt128((1L << 63) + i, j);
                    try {
                        RAMCloudObject obj = rctx.read(vertexTableId, TorcHelper.getVertexEdgeLabelListKey(baseVertexId));
                        List<String> edgeLabels = TorcHelper.deserializeEdgeLabelList(obj);

                        for (String label : edgeLabels) {
                            try {
                                obj = rctx.read(vertexTableId, TorcHelper.getVertexEdgeListKey(baseVertexId, label, TorcEdgeDirection.DIRECTED_OUT));
                                List<UInt128> neighborVertexIds = TorcHelper.parseNeighborIdsFromEdgeList(obj);
                                for (UInt128 neighborVertexId : neighborVertexIds) {
                                    list.add(new TorcEdge(this, baseVertexId, neighborVertexId, TorcEdge.Type.DIRECTED, label));
                                }
                            } catch (ObjectDoesntExistException e) {
                                /*
                                 * The DIRECTED_OUT edge list for this label
                                 * doesn't exist. Continue...
                                 */
                            }

                            /**
                             * TODO: Decide whether or not to include the new
                             * bi-directional edges in this list. Not sure
                             * whether or not including these RAMCloudGraph
                             * specific bi-directional edges is considered a
                             * violation of the API.
                             */
                            try {
                                obj = rctx.read(vertexTableId, TorcHelper.getVertexEdgeListKey(baseVertexId, label, TorcEdgeDirection.UNDIRECTED));
                                List<UInt128> neighborVertexIds = TorcHelper.parseNeighborIdsFromEdgeList(obj);
                                for (UInt128 neighborVertexId : neighborVertexIds) {
                                    /*
                                     * Prevent double counting of undirected
                                     * edges. Only count the edge at the vertex
                                     * with the min ID.
                                     */
                                    if (baseVertexId.compareTo(neighborVertexId) < 0) {
                                        list.add(new TorcEdge(this, baseVertexId, neighborVertexId, TorcEdge.Type.UNDIRECTED, label));
                                    }
                                }
                            } catch (ObjectDoesntExistException e) {
                                /*
                                 * The UNDIRECTED edge list for this label
                                 * doesn't exist. Continue...
                                 */
                            }
                        }
                    } catch (ObjectDoesntExistException e) {
                        /*
                         * The edge label list object for this vertex doesn't
                         * exist
                         */
                    }
                }
            }
        }

        if (logger.isDebugEnabled()) {
            long endTimeNs = System.nanoTime();
            logger.debug(String.format("edges(n=%d), took %dus", edgeIds.length, (endTimeNs - startTimeNs) / 1000l));
        }

        return list.iterator();
    }

    @Override
    public Transaction tx() {
        if (!initialized) {
            initialize();
        }

        return torcGraphTx;
    }

    @Override
    public Variables variables() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Configuration configuration() {
        return configuration;
    }

    @Override
    public void close() {
        if (initialized) {
            torcGraphTx.close();
            ramcloud.disconnect();
        }
    }

    public void deleteDatabaseAndCloseAllConnectionsAndTransactions() {
        long startTimeNs = 0;
        if (logger.isDebugEnabled()) {
            startTimeNs = System.nanoTime();
        }
        
        if (!initialized) {
            initialize();
        }

        
        torcGraphTx.doRollbackAllThreads();
        ramcloud.dropTable(graphName + "_" + ID_TABLE_NAME);
        ramcloud.dropTable(graphName + "_" + VERTEX_TABLE_NAME);
        ramcloud.dropTable(graphName + "_" + EDGE_TABLE_NAME);
        ramcloud.disconnect();
        
        if (logger.isDebugEnabled()) {
            long endTimeNs = System.nanoTime();
            logger.debug(String.format("deleteDatabaseAndCloseAllConnectionsAndTransactions(), took %dus", (endTimeNs - startTimeNs) / 1000l));
        }
    }

    @Override
    public String toString() {
        return StringFactory.graphString(this, "coordLoc:" + this.coordinatorLocator + " graphName:" + this.graphName);
    }

    @Override
    public Features features() {
        return new TorcGraphFeatures();
    }

    /**
     * Methods called by TorcVertex.
     */
    void removeVertex(final TorcVertex vertex) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    Edge addEdge(final TorcVertex vertex1, final TorcVertex vertex2, final String label, final TorcEdge.Type type, final Object[] keyValues) {
        long startTimeNs = 0;
        if (logger.isDebugEnabled()) {
            startTimeNs = System.nanoTime();
        }

        torcGraphTx.readWrite();
        RAMCloudTransaction rctx = torcGraphTx.getThreadLocalRAMCloudTx();

        // Validate that these key/value pairs are all strings
        if (keyValues.length % 2 != 0) {
            throw Element.Exceptions.providedKeyValuesMustBeAMultipleOfTwo();
        }
        for (int i = 0; i < keyValues.length; i = i + 2) {
            if (keyValues[i] instanceof T) {
                if (keyValues[i].equals(T.id)) {
                    throw Edge.Exceptions.userSuppliedIdsNotSupported();
                } else {
                    throw new UnsupportedOperationException("Not supported yet.");
                }
            }
            if (!(keyValues[i] instanceof String)) {
                throw Element.Exceptions.providedKeyValuesMustHaveALegalKeyOnEvenIndices();
            }
            if (!(keyValues[i + 1] instanceof String)) {
                throw Property.Exceptions.dataTypeOfPropertyValueNotSupported(keyValues[i + 1]);
            }
        }

        // Create property map.
        Map<String, String> properties = new HashMap<>();
        for (int i = 0; i < keyValues.length; i = i + 2) {
            properties.put((String) keyValues[i], (String) keyValues[i + 1]);
        }

        ByteBuffer serializedProperties = TorcHelper.serializeProperties(properties);
        int serializedEdgeLength = Long.BYTES * 2 + Short.BYTES + serializedProperties.array().length;

        // TODO: Figure out a way to work this logic into a function so it's not
        // repeated twice as it is below. 
        // Update vertex1's edge list
        byte[] edgeListKey;
        if (type == TorcEdge.Type.UNDIRECTED) {
            edgeListKey = TorcHelper.getVertexEdgeListKey(vertex1.id(), label, TorcEdgeDirection.UNDIRECTED);
        } else {
            edgeListKey = TorcHelper.getVertexEdgeListKey(vertex1.id(), label, TorcEdgeDirection.DIRECTED_OUT);
        }
        try {
            RAMCloudObject edgeListRCObj = rctx.read(vertexTableId, edgeListKey);

            ByteBuffer newEdgeList = ByteBuffer.allocate(serializedEdgeLength + edgeListRCObj.getValueBytes().length);

            newEdgeList.put(vertex2.id().toByteArray());
            newEdgeList.putShort((short) serializedProperties.array().length);
            newEdgeList.put(serializedProperties.array());
            newEdgeList.put(edgeListRCObj.getValueBytes());

            rctx.write(vertexTableId, edgeListKey, newEdgeList.array());
        } catch (ObjectDoesntExistException e) {
            ByteBuffer newEdgeList = ByteBuffer.allocate(serializedEdgeLength);

            newEdgeList.put(vertex2.id().toByteArray());
            newEdgeList.putShort((short) serializedProperties.array().length);
            newEdgeList.put(serializedProperties.array());

            rctx.write(vertexTableId, edgeListKey, newEdgeList.array());

            byte[] edgeLabelListKey = TorcHelper.getVertexEdgeLabelListKey(vertex1.id());
            try {
                RAMCloudObject edgeLabelListRCObj = rctx.read(vertexTableId, edgeLabelListKey);
                List<String> edgeLabelList = TorcHelper.deserializeEdgeLabelList(edgeLabelListRCObj);
                if (!edgeLabelList.contains(label)) {
                    edgeLabelList.add(label);
                    rctx.write(vertexTableId, edgeLabelListKey, TorcHelper.serializeEdgeLabelList(edgeLabelList).array());
                }
            } catch (ObjectDoesntExistException e2) {
                List<String> edgeLabelList = new ArrayList<>();
                edgeLabelList.add(label);
                rctx.write(vertexTableId, edgeLabelListKey, TorcHelper.serializeEdgeLabelList(edgeLabelList).array());
            }
        }

        // Update vertex2's edge list
        if (type == TorcEdge.Type.UNDIRECTED) {
            edgeListKey = TorcHelper.getVertexEdgeListKey(vertex2.id(), label, TorcEdgeDirection.UNDIRECTED);
        } else {
            edgeListKey = TorcHelper.getVertexEdgeListKey(vertex2.id(), label, TorcEdgeDirection.DIRECTED_IN);
        }
        try {
            RAMCloudObject edgeListRCObj = rctx.read(vertexTableId, edgeListKey);

            ByteBuffer newEdgeList = ByteBuffer.allocate(serializedEdgeLength + edgeListRCObj.getValueBytes().length);

            newEdgeList.put(vertex1.id().toByteArray());
            newEdgeList.putShort((short) serializedProperties.array().length);
            newEdgeList.put(serializedProperties.array());
            newEdgeList.put(edgeListRCObj.getValueBytes());

            rctx.write(vertexTableId, edgeListKey, newEdgeList.array());
        } catch (ObjectDoesntExistException e) {
            ByteBuffer newEdgeList = ByteBuffer.allocate(serializedEdgeLength);

            newEdgeList.put(vertex1.id().toByteArray());
            newEdgeList.putShort((short) serializedProperties.array().length);
            newEdgeList.put(serializedProperties.array());

            rctx.write(vertexTableId, edgeListKey, newEdgeList.array());

            byte[] edgeLabelListKey = TorcHelper.getVertexEdgeLabelListKey(vertex2.id());
            try {
                RAMCloudObject edgeLabelListRCObj = rctx.read(vertexTableId, edgeLabelListKey);
                List<String> edgeLabelList = TorcHelper.deserializeEdgeLabelList(edgeLabelListRCObj);
                if (!edgeLabelList.contains(label)) {
                    edgeLabelList.add(label);
                    rctx.write(vertexTableId, edgeLabelListKey, TorcHelper.serializeEdgeLabelList(edgeLabelList).array());
                }
            } catch (ObjectDoesntExistException e2) {
                List<String> edgeLabelList = new ArrayList<>();
                edgeLabelList.add(label);
                rctx.write(vertexTableId, edgeLabelListKey, TorcHelper.serializeEdgeLabelList(edgeLabelList).array());
            }
        }

        if (logger.isDebugEnabled()) {
            long endTimeNs = System.nanoTime();
            int propLength = TorcHelper.serializeProperties(properties).array().length;
            logger.debug(String.format("addEdge(from=%s,to=%s,label=%s,propLen=%d), took %dus", vertex1.id().toString(), vertex2.id().toString(), label, propLength, (endTimeNs - startTimeNs) / 1000l));
        }

        return new TorcEdge(this, vertex1.id(), vertex2.id(), type, label);
    }

    Iterator<Edge> vertexEdges(final TorcVertex vertex, final EnumSet<TorcEdgeDirection> edgeDirections, final String[] edgeLabels) {
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
                RAMCloudObject vertEdgeLabelListRCObj = rctx.read(vertexTableId, TorcHelper.getVertexEdgeLabelListKey(vertex.id()));
                labels = TorcHelper.deserializeEdgeLabelList(vertEdgeLabelListRCObj);
            } catch (ObjectDoesntExistException e) {

            }
        }

        for (String label : labels) {
            for (TorcEdgeDirection dir : edgeDirections) {
                try {
                    byte[] edgeListKey = TorcHelper.getVertexEdgeListKey(vertex.id(), label, dir);
                    RAMCloudObject edgeListRCObj = rctx.read(vertexTableId, edgeListKey);
                    List<UInt128> neighborVertexIds = TorcHelper.parseNeighborIdsFromEdgeList(edgeListRCObj);
                    for (UInt128 neighborVertexId : neighborVertexIds) {
                        switch (dir) {
                            case DIRECTED_OUT:
                                edges.add(new TorcEdge(this, vertex.id(), neighborVertexId, TorcEdge.Type.DIRECTED, label));
                                break;
                            case DIRECTED_IN:
                                edges.add(new TorcEdge(this, neighborVertexId, vertex.id(), TorcEdge.Type.DIRECTED, label));
                                break;
                            case UNDIRECTED:
                                edges.add(new TorcEdge(this, vertex.id(), neighborVertexId, TorcEdge.Type.UNDIRECTED, label));
                                break;
                        }
                    }
                } catch (ObjectDoesntExistException e) {
                    /* 
                     * The list of edges for this direction and label does not 
                     * exist. Continue...
                     */
                }
            }
        }

        if (logger.isDebugEnabled()) {
            long endTimeNs = System.nanoTime();
            logger.debug(String.format("vertexEdges(vertex=%s,directions=%s,labels=%s), took %dus", vertex.id().toString(), edgeDirections.toString(), labels.toString(), (endTimeNs - startTimeNs) / 1000l));
        }

        return edges.iterator();
    }

    Iterator<Vertex> vertexNeighbors(final TorcVertex vertex, final EnumSet<TorcEdgeDirection> edgeDirections, final String[] edgeLabels) {
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
                RAMCloudObject vertEdgeLabelListRCObj = rctx.read(vertexTableId, TorcHelper.getVertexEdgeLabelListKey(vertex.id()));
                labels = TorcHelper.deserializeEdgeLabelList(vertEdgeLabelListRCObj);
            } catch (ObjectDoesntExistException e) {

            }
        }

        for (String label : labels) {
            for (TorcEdgeDirection dir : edgeDirections) {
                try {
                    byte[] edgeListKey = TorcHelper.getVertexEdgeListKey(vertex.id(), label, dir);
                    RAMCloudObject edgeListRCObj = rctx.read(vertexTableId, edgeListKey);
                    List<UInt128> neighborIds = TorcHelper.parseNeighborIdsFromEdgeList(edgeListRCObj);
                    for (UInt128 neighborId : neighborIds) {
                        RAMCloudObject neighborLabelRCObj = rctx.read(vertexTableId, TorcHelper.getVertexLabelKey(neighborId));
                        vertices.add(new TorcVertex(this, neighborId, neighborLabelRCObj.getValue()));
                    }
                } catch (ObjectDoesntExistException e) {
                    /* 
                     * The list of edges for this direction and label does not 
                     * exist. Continue...
                     */
                }
            }
        }

        if (logger.isDebugEnabled()) {
            long endTimeNs = System.nanoTime();
            logger.debug(String.format("vertexNeighbors(vertex=%s,directions=%s,labels=%s), took %dus", vertex.id().toString(), edgeDirections.toString(), labels.toString(), (endTimeNs - startTimeNs) / 1000l));
        }

        return vertices.iterator();
    }

    <V> Iterator<VertexProperty<V>> getVertexProperties(final TorcVertex vertex, final String[] propertyKeys) {
        long startTimeNs = 0;
        if (logger.isDebugEnabled()) {
            startTimeNs = System.nanoTime();
        }

        torcGraphTx.readWrite();
        RAMCloudTransaction rctx = torcGraphTx.getThreadLocalRAMCloudTx();

        RAMCloudObject obj = rctx.read(vertexTableId, TorcHelper.getVertexPropertiesKey(vertex.id()));

        Map<String, String> properties = TorcHelper.deserializeProperties(obj);

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
            for (Map.Entry<String, String> property : properties.entrySet()) {
                // TODO: Here I am implicitly assuming that V is of type String, 
                // since property.getValue() returns a string, making the new 
                // elemennt to propList TorcVertexProperty<String>
                propList.add(new TorcVertexProperty(vertex, property.getKey(), property.getValue()));
            }
        }

        if (logger.isDebugEnabled()) {
            long endTimeNs = System.nanoTime();
            logger.debug(String.format("getVertexProperties(vertex=%s,propKeys=%s), took %dus", vertex.id().toString(), Arrays.asList(propertyKeys).toString(), (endTimeNs - startTimeNs) / 1000l));
        }

        return propList.iterator();
    }

    <V> VertexProperty<V> setVertexProperty(final TorcVertex vertex, final VertexProperty.Cardinality cardinality, final String key, final V value, final Object[] keyValues) {
        long startTimeNs = 0;
        if (logger.isDebugEnabled()) {
            startTimeNs = System.nanoTime();
        }

        torcGraphTx.readWrite();
        RAMCloudTransaction rctx = torcGraphTx.getThreadLocalRAMCloudTx();

        if (keyValues != null) {
            throw VertexProperty.Exceptions.metaPropertiesNotSupported();
        }

        if (!(value instanceof String)) {
            throw Property.Exceptions.dataTypeOfPropertyValueNotSupported(value);
        }

        RAMCloudObject obj = rctx.read(vertexTableId, TorcHelper.getVertexPropertiesKey(vertex.id()));

        Map<String, String> properties = TorcHelper.deserializeProperties(obj);

        if (properties.containsKey(key)) {
            if (cardinality == VertexProperty.Cardinality.single) {
                properties.put(key, (String) value);
            } else if (cardinality == VertexProperty.Cardinality.list) {
                throw VertexProperty.Exceptions.multiPropertiesNotSupported();
            } else if (cardinality == VertexProperty.Cardinality.set) {
                throw VertexProperty.Exceptions.multiPropertiesNotSupported();
            } else {
                throw new UnsupportedOperationException("Do not recognize Cardinality of this type: " + cardinality.toString());
            }
        } else {
            properties.put(key, (String) value);
        }

        rctx.write(vertexTableId, TorcHelper.getVertexPropertiesKey(vertex.id()), TorcHelper.serializeProperties(properties).array());

        if (logger.isDebugEnabled()) {
            long endTimeNs = System.nanoTime();
            logger.debug(String.format("setVertexProperty(vertex=%s,card=%s,key=%s,value=%s), took %dus", vertex.id().toString(), cardinality.toString(), key, value, (endTimeNs - startTimeNs) / 1000l));
        }

        return new TorcVertexProperty(vertex, key, value);
    }

    /**
     * Methods called by TorcEdge.
     */
    void removeEdge(final TorcEdge edge) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    Iterator<Vertex> edgeVertices(final TorcEdge edge, final Direction direction) {
        long startTimeNs = 0;
        if (logger.isDebugEnabled()) {
            startTimeNs = System.nanoTime();
        }

        if (edge.getType() == TorcEdge.Type.UNDIRECTED &&
                direction != Direction.BOTH) 
            throw new RuntimeException(String.format("Tried get source/destination vertex of an undirected edge: [edge:%s, direction:%s]", edge.toString(), direction.toString()));
        
        torcGraphTx.readWrite();
        RAMCloudTransaction rctx = torcGraphTx.getThreadLocalRAMCloudTx();

        List<Vertex> list = new ArrayList<>();

        if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH)) {
            RAMCloudObject obj = rctx.read(vertexTableId, TorcHelper.getVertexLabelKey(edge.getV1Id()));
            list.add(new TorcVertex(this, edge.getV1Id(), obj.getValue()));
        }

        if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH)) {
            RAMCloudObject obj = rctx.read(vertexTableId, TorcHelper.getVertexLabelKey(edge.getV2Id()));
            list.add(new TorcVertex(this, edge.getV2Id(), obj.getValue()));
        }

        if (logger.isDebugEnabled()) {
            long endTimeNs = System.nanoTime();
            // TODO: Update this when we have TorcEdgeId class to stringify
            logger.debug(String.format("edgeVertices(edgeIdGoesHere,dir=%s), took %dus", direction.toString(), (endTimeNs - startTimeNs) / 1000l));
        }

        return list.iterator();
    }

    <V> Iterator<Property<V>> getEdgeProperties(final TorcEdge edge, final String[] propertyKeys) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    <V> Property<V> setEdgeProperty(final TorcEdge edge, final String key, final V value) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
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

        public static IllegalArgumentException userSuppliedIdNotValid(String message) {
            throw new IllegalArgumentException("Invalid vertex ID: " + message);
        }
    }

    class TorcGraphTransaction extends AbstractTransaction {

        private final ConcurrentHashMap<Thread, RAMCloudTransaction> txMap = new ConcurrentHashMap<>();
        
        public TorcGraphTransaction() {
            super(TorcGraph.this);
        }

        /** 
         * This method returns the underlying RAMCloudTransaction object for
         * this thread that contains all of the transaction state.
         *
         * @return RAMCloudTransaction for current thread.
         */
        protected RAMCloudTransaction getThreadLocalRAMCloudTx() {
            return txMap.get(Thread.currentThread());
        }

        /**
         * This method rolls back the transactions of all threads that have not
         * closed their transactions themselves. It is meant to be used as a
         * final cleanup method to free all transaction state before exiting, in
         * the case that threads had executed without performing final cleanup
         * themselves before exiting. This currently happens in TinkerPop unit
         * tests (3.1.0-incubating). See
         * {@link org.apache.tinkerpop.gremlin.structure.TransactionTest#shouldExecuteCompetingThreadsOnMultipleDbInstances}.
         * This method is *not* meant to be called while other threads and still
         * executing.
         */
        protected void doRollbackAllThreads() {
            txMap.forEach((thread, rctx) -> {
                try {
                    rctx.close();
                } catch (Exception e) {
                    logger.error("TorcGraphTransaction.doRollbackAllThreads(): could not close transaction of thread " + thread.getId());
                }

                logger.debug(String.format("TorcGraphTransaction.doRollbackAllThreads(): %d", thread.getId()));
            });

            txMap.clear();
        }
        
        @Override
        public void doOpen() {
            if (txMap.get(Thread.currentThread()) == null)
                txMap.put(Thread.currentThread(), new RAMCloudTransaction(TorcGraph.this.ramcloud));
            else
                throw Transaction.Exceptions.transactionAlreadyOpen();
            
            logger.debug(String.format("TorcGraphTransaction.doOpen(thread=%d)", Thread.currentThread().getId()));
        }

        @Override
        public void doCommit() throws AbstractTransaction.TransactionException {
            long startTimeNs = 0;
            if (logger.isDebugEnabled()) {
                startTimeNs = System.nanoTime();
            }

            RAMCloudTransaction rctx = txMap.get(Thread.currentThread());
            
            try {
                if (!rctx.commitAndSync()) {
                    throw new AbstractTransaction.TransactionException("RAMCloud commitAndSync failed.");
                }
            } catch (ClientException ex) {
                throw new AbstractTransaction.TransactionException(ex);
            } finally {
                rctx.close();
                txMap.remove(Thread.currentThread());
            }
            
            if (logger.isDebugEnabled()) {
                long endTimeNs = System.nanoTime();
                logger.debug(String.format("TorcGraphTransaction.doCommit(thread=%d), took %dus", Thread.currentThread().getId(), (endTimeNs - startTimeNs) / 1000l));
            }
        }

        @Override
        public void doRollback() throws AbstractTransaction.TransactionException {
            long startTimeNs = 0;
            if (logger.isDebugEnabled()) {
                startTimeNs = System.nanoTime();
            }
            
            RAMCloudTransaction rctx = txMap.get(Thread.currentThread());
            
            try {
                rctx.close();
            } catch (Exception e) {
                throw new AbstractTransaction.TransactionException(e);
            } finally {
                txMap.remove(Thread.currentThread());
            }
            
            if (logger.isDebugEnabled()) {
                long endTimeNs = System.nanoTime();
                logger.debug(String.format("TorcGraphTransaction.doRollback(thread=%d), took %dus", Thread.currentThread().getId(), (endTimeNs - startTimeNs) / 1000l));
            }
        }

        @Override
        public boolean isOpen() {
            boolean isOpen = (txMap.get(Thread.currentThread()) != null);
            
            logger.debug(String.format("TorcGraphTransaction.isOpen(thread=%d): returning %s", Thread.currentThread().getId(), isOpen));
            
            return isOpen;
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
            return false;
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

    public class TorcGraphVertexPropertyFeatures implements Features.VertexPropertyFeatures {

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

    public class TorcGraphEdgePropertyFeatures implements Features.EdgePropertyFeatures {

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

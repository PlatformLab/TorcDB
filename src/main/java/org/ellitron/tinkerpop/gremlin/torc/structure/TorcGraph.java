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

import org.ellitron.tinkerpop.gremlin.torc.structure.util.UInt128;
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
import org.ellitron.tinkerpop.gremlin.torc.structure.util.TorcVertexEdgeList;

/**
 * TODO: Write documentation - Bidirectional edges
 *
 * @author Jonathan Ellithorpe <jde@cs.stanford.edu>
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
    private static final String EDGELIST_TABLE_NAME = "edgeListTable";
    private static final int MAX_TX_RETRY_COUNT = 100;
    private static final int NUM_ID_COUNTERS = 16;
    private static final int RAMCLOUD_OBJECT_SIZE_LIMIT = 1 << 20;

    // Normal private members.
    private final Configuration configuration;
    private final String coordinatorLocator;
    private final int totalMasterServers;
    private final ConcurrentHashMap<Thread, RAMCloud> threadLocalClientMap = new ConcurrentHashMap<>();
    private long idTableId, vertexTableId, edgeListTableId;
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

    public static TorcGraph open(final Configuration configuration) {
        return new TorcGraph(configuration);
    }

    @Override
    public Variables variables() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Features features() {
        return new TorcGraphFeatures();
    }

    @Override
    public Configuration configuration() {
        return configuration;
    }

    public boolean isInitialized() {
        return initialized;
    }

    @Override
    public Transaction tx() {
        initialize();

        return torcGraphTx;
    }

    /**
     * This method ensures three things are true before it returns to the
     * caller:
     *
     * 1) This thread has an initialized thread-local RAMCloud client (its own
     * connection to RAMCloud)
     *
     * 2) RAMCloud tables have been created for this graph
     *
     * 3) RAMCloud table IDs have been fetched.
     *
     *
     * This method is intended to be used as a way of deferring costly
     * initialization until absolutely needed. This method should be called at
     * the top of any public method of TorcGraph that performs operations
     * against RAMCloud.
     */
    private void initialize() {
        if (!threadLocalClientMap.containsKey(Thread.currentThread())) {
            threadLocalClientMap.put(Thread.currentThread(), new RAMCloud(coordinatorLocator));

            logger.debug(String.format("initialize(): Thread %d made connection to RAMCloud cluster.", Thread.currentThread().getId()));

            if (!initialized) {
                RAMCloud client = threadLocalClientMap.get(Thread.currentThread());
                idTableId = client.createTable(graphName + "_" + ID_TABLE_NAME, totalMasterServers);
                vertexTableId = client.createTable(graphName + "_" + VERTEX_TABLE_NAME, totalMasterServers);
                edgeListTableId = client.createTable(graphName + "_" + EDGELIST_TABLE_NAME, totalMasterServers);

                initialized = true;

                logger.debug(String.format("initialize(): Fetched table Ids (%s=%d,%s=%d,%s=%d)", graphName + "_" + ID_TABLE_NAME, idTableId, graphName + "_" + VERTEX_TABLE_NAME, vertexTableId, graphName + "_" + EDGELIST_TABLE_NAME, edgeListTableId));
            }
        }
    }

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
        
        ElementHelper.legalPropertyKeyValueArray(keyValues);

        // Only values of type String supported, currently.
        for (int i = 0; i < keyValues.length; i = i + 2) {
            if (!(keyValues[i] instanceof T) && !(keyValues[i + 1] instanceof String)) {
                throw Property.Exceptions.dataTypeOfPropertyValueNotSupported(keyValues[i + 1]);
            }
        }

        Object idValue = ElementHelper.getIdValue(keyValues).orElse(null);
        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);

        if (logger.isTraceEnabled()) {
            long endTime = System.nanoTime();
            logger.trace(String.format("addVertex(label=%s):setup, took %dus", label, (endTime - startTime) / 1000l));
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
                logger.trace(String.format("addVertex(id=%s,label=%s):idCheck, took %dus", vertexId.toString(), label, (endTime - startTime) / 1000l));
            }
            
        } else {
            startTime = 0;
            if (logger.isTraceEnabled()) {
                startTime = System.nanoTime();
            }
            
            long id_counter = (long) (Math.random() * NUM_ID_COUNTERS);
            RAMCloud client = threadLocalClientMap.get(Thread.currentThread());
            
            long id = client.incrementInt64(idTableId, Long.toString(id_counter).getBytes(), 1, null);

            vertexId = new UInt128((1L << 63) + id_counter, id);
            
            if (logger.isTraceEnabled()) {
                long endTime = System.nanoTime();
                logger.trace(String.format("addVertex(id=%s,label=%s):idGen, took %dus", vertexId.toString(), label, (endTime - startTime) / 1000l));
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
            logger.trace(String.format("addVertex(id=%s,label=%s):propMap, took %dus", vertexId.toString(), label, (endTime - startTime) / 1000l));
        }
        
        startTime = 0;
        if (logger.isTraceEnabled()) {
            startTime = System.nanoTime();
        }
        
        /*
         * Perform size checks on objects to be written to RAMCloud.
         */
        if (label.getBytes().length > RAMCLOUD_OBJECT_SIZE_LIMIT) {
            throw new IllegalArgumentException(String.format("Size of vertex label exceeds maximum allowable (size=%dB, max=%dB)", label.getBytes().length, RAMCLOUD_OBJECT_SIZE_LIMIT));
        }

        byte[] serializedProps = TorcHelper.serializeProperties(properties).array();
        if (serializedProps.length > RAMCLOUD_OBJECT_SIZE_LIMIT) {
            throw new IllegalArgumentException(String.format("Total size of properties exceeds maximum allowable (size=%dB, max=%dB)", serializedProps.length, RAMCLOUD_OBJECT_SIZE_LIMIT));
        }
        
        if (logger.isTraceEnabled()) {
            long endTime = System.nanoTime();
            logger.trace(String.format("addVertex(id=%s,label=%s):sizeCheck, took %dus", vertexId.toString(), label, (endTime - startTime) / 1000l));
        }

        startTime = 0;
        if (logger.isTraceEnabled()) {
            startTime = System.nanoTime();
        }
            
        rctx.write(vertexTableId, TorcHelper.getVertexLabelKey(vertexId), label.getBytes());
        
        if (logger.isTraceEnabled()) {
            long endTime = System.nanoTime();
            logger.trace(String.format("addVertex(id=%s,label=%s):writeLabel, took %dus", vertexId.toString(), label, (endTime - startTime) / 1000l));
        }
        
        startTime = 0;
        if (logger.isTraceEnabled()) {
            startTime = System.nanoTime();
        }
        
        rctx.write(vertexTableId, TorcHelper.getVertexPropertiesKey(vertexId), serializedProps);

        if (logger.isTraceEnabled()) {
            long endTime = System.nanoTime();
            logger.trace(String.format("addVertex(id=%s,label=%s):writeProps, took %dus", vertexId.toString(), label, (endTime - startTime) / 1000l));
        }
        
        if (logger.isDebugEnabled()) {
            long endTimeNs = System.nanoTime();
            int propLength = serializedProps.length;
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
                        byte[] edgeLabelListKey = TorcHelper.getEdgeLabelListKey(baseVertexId);
                        RAMCloudObject obj = rctx.read(vertexTableId, edgeLabelListKey);
                        List<String> edgeLabels = TorcHelper.deserializeEdgeLabelList(obj);

                        for (String label : edgeLabels) {
                            /*
                             * Add all the directed edges.
                             */
                            byte[] keyPrefix = TorcHelper.getEdgeListKeyPrefix(baseVertexId, label, TorcEdgeDirection.DIRECTED_OUT);
                            TorcVertexEdgeList edgeList = TorcVertexEdgeList.open(rctx, edgeListTableId, keyPrefix);
                            list.addAll(edgeList.readEdges(this, baseVertexId, label, TorcEdgeDirection.DIRECTED_OUT));

                            /*
                             * Add all the undirected edges.
                             */
                            keyPrefix = TorcHelper.getEdgeListKeyPrefix(baseVertexId, label, TorcEdgeDirection.UNDIRECTED);
                            edgeList = TorcVertexEdgeList.open(rctx, edgeListTableId, keyPrefix);
                            edgeList.readEdges(this, baseVertexId, label, TorcEdgeDirection.UNDIRECTED).forEach((edge) -> {
                                if (edge.getV1Id().compareTo(edge.getV2Id()) < 0) {
                                    list.add(edge);
                                }
                            });
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

        ByteBuffer serializedProperties = TorcHelper.serializeProperties(properties);

        /*
         * Add one vertex to the other's edge list, and vice versa.
         */
        for (int i = 0; i < 2; ++i) {
            TorcVertex baseVertex;
            TorcVertex neighborVertex;
            TorcEdgeDirection direction;

            /*
             * Choose which vertex acts as the base and which acts as the
             * neighbor in this half of the edge addition operation.
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

            byte[] keyPrefix = TorcHelper.getEdgeListKeyPrefix(baseVertex.id(), label, direction);
            TorcVertexEdgeList edgeList = TorcVertexEdgeList.open(rctx, edgeListTableId, keyPrefix);
            boolean newListCreated = edgeList.prependEdge(neighborVertex.id(), serializedProperties.array());

            if (newListCreated) {
                /*
                 * It's possible that this is the first edge that has this
                 * particular label, so we must check the edge label list and
                 * add it if necessary.
                 */

                byte[] edgeLabelListKey = TorcHelper.getEdgeLabelListKey(baseVertex.id());
                try {
                    RAMCloudObject labelListRCObj = rctx.read(vertexTableId, edgeLabelListKey);
                    List<String> edgeLabelList = TorcHelper.deserializeEdgeLabelList(labelListRCObj);
                    if (!edgeLabelList.contains(label)) {
                        edgeLabelList.add(label);
                        rctx.write(vertexTableId, edgeLabelListKey, TorcHelper.serializeEdgeLabelList(edgeLabelList).array());
                    }
                } catch (ClientException.ObjectDoesntExistException e) {
                    List<String> edgeLabelList = new ArrayList<>();
                    edgeLabelList.add(label);
                    rctx.write(vertexTableId, edgeLabelListKey, TorcHelper.serializeEdgeLabelList(edgeLabelList).array());
                }
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
                byte[] edgeLabelListKey = TorcHelper.getEdgeLabelListKey(vertex.id());
                RAMCloudObject labelListRCObj = rctx.read(vertexTableId, edgeLabelListKey);
                labels = TorcHelper.deserializeEdgeLabelList(labelListRCObj);
            } catch (ObjectDoesntExistException e) {

            }
        }

        for (String label : labels) {
            for (TorcEdgeDirection dir : edgeDirections) {
                byte[] keyPrefix = TorcHelper.getEdgeListKeyPrefix(vertex.id(), label, dir);
                TorcVertexEdgeList edgeList = TorcVertexEdgeList.open(rctx, edgeListTableId, keyPrefix);
                edges.addAll(edgeList.readEdges(this, vertex.id(), label, dir));
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
                byte[] edgeLabelListKey = TorcHelper.getEdgeLabelListKey(vertex.id());
                RAMCloudObject labelListRCObj = rctx.read(vertexTableId, edgeLabelListKey);
                labels = TorcHelper.deserializeEdgeLabelList(labelListRCObj);
            } catch (ObjectDoesntExistException e) {

            }
        }

        for (String label : labels) {
            for (TorcEdgeDirection dir : edgeDirections) {
                byte[] keyPrefix = TorcHelper.getEdgeListKeyPrefix(vertex.id(), label, dir);
                TorcVertexEdgeList edgeList = TorcVertexEdgeList.open(rctx, edgeListTableId, keyPrefix);
                List<UInt128> neighborIdList = edgeList.readNeighborIds();
                for (UInt128 neighborId : neighborIdList) {
                    RAMCloudObject neighborLabelRCObj = rctx.read(vertexTableId, TorcHelper.getVertexLabelKey(neighborId));
                    vertices.add(new TorcVertex(this, neighborId, neighborLabelRCObj.getValue()));
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

        Map<String, List<String>> properties = TorcHelper.deserializeProperties(obj);

        List<VertexProperty<V>> propList = new ArrayList<>();

        if (propertyKeys.length > 0) {
            for (String key : propertyKeys) {
                if (properties.containsKey(key)) {
                    for (String value : properties.get(key))
                        propList.add(new TorcVertexProperty(vertex, key, value));
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
                for (String value : property.getValue())
                    propList.add(new TorcVertexProperty(vertex, key, value));
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

        if (!(keyValues == null || keyValues.length == 0)) {
            throw VertexProperty.Exceptions.metaPropertiesNotSupported();
        }

        if (!(value instanceof String)) {
            throw Property.Exceptions.dataTypeOfPropertyValueNotSupported(value);
        }

        RAMCloudObject obj = rctx.read(vertexTableId, TorcHelper.getVertexPropertiesKey(vertex.id()));

        Map<String, List<String>> properties = TorcHelper.deserializeProperties(obj);

        if (properties.containsKey(key)) {
            if (cardinality == VertexProperty.Cardinality.single) {
                properties.put(key, new ArrayList<>(Arrays.asList((String) value)));
            } else if (cardinality == VertexProperty.Cardinality.list) {
                properties.get(key).add((String) value);
            } else if (cardinality == VertexProperty.Cardinality.set) {
                if (!properties.get(key).contains((String) value)) 
                    properties.get(key).add((String) value);
            } else {
                throw new UnsupportedOperationException("Do not recognize Cardinality of this type: " + cardinality.toString());
            }
        } else {
            properties.put(key, new ArrayList<>(Arrays.asList((String) value)));
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

        if (edge.getType() == TorcEdge.Type.UNDIRECTED
                && direction != Direction.BOTH) {
            throw new RuntimeException(String.format("Tried get source/destination vertex of an undirected edge: [edge:%s, direction:%s]", edge.toString(), direction.toString()));
        }

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

    /**
     * Closes the thread-local transaction (if it is open), and closes the
     * thread-local connection to RAMCloud (if one has been made). This may
     * affect the state of the graph in RAMCloud depending on the close behavior
     * set for the transaction (e.g. in the case that there is an open
     * transaction which is set to automatically commit when closed).
     *
     * Important: Every thread that performs any operation on this graph
     * instance has the responsibility of calling this close method before
     * exiting. Otherwise it is possible that state that has been created via
     * the RAMCloud JNI library will not be cleaned up properly (for instance,
     * although {@link RAMCloud} and {@link RAMCloudTransaction} objects have
     * implemented finalize() methods to clean up their mirrored C++ objects, it
     * is still possible that the garbage collector will clean up the RAMCloud
     * object before the RAMCloudTransaction object that uses it. This *may*
     * lead to unexpected behavior).
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
            logger.debug(String.format("close(), took %dus", (endTimeNs - startTimeNs) / 1000l));
        }
    }

    /**
     * This method closes all open transactions on all threads (using rollback),
     * and closes all open client connections to RAMCloud on all threads. Since
     * this method uses rollback as the close mechanism for open transactions,
     * and RAMCloud transactions keep no server-side state until commit, it is
     * safe to execute this method even after the graph has been deleted with
     * {@link #deleteAll()}. Its intended use is primarily for unit tests to
     * ensure the freeing of all client-side state remaining across JNI (i.e.
     * C++ RAMCloud client objects, C++ RAMCloud Transaction objects) before
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
                logger.error("closeAllThreads(): could not close transaction of thread " + thread.getId());
            }

            logger.debug(String.format("closeAllThreads(): closed client connection of %d", thread.getId()));
        });

        threadLocalClientMap.clear();

        if (logger.isDebugEnabled()) {
            long endTimeNs = System.nanoTime();
            logger.debug(String.format("closeAllThreads(), took %dus", (endTimeNs - startTimeNs) / 1000l));
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
        idTableId = client.createTable(graphName + "_" + ID_TABLE_NAME, totalMasterServers);
        vertexTableId = client.createTable(graphName + "_" + VERTEX_TABLE_NAME, totalMasterServers);
        edgeListTableId = client.createTable(graphName + "_" + EDGELIST_TABLE_NAME, totalMasterServers);

        if (logger.isDebugEnabled()) {
            long endTimeNs = System.nanoTime();
            logger.debug(String.format("deleteGraph(), took %dus", (endTimeNs - startTimeNs) / 1000l));
        }
    }

    @Override
    public String toString() {
        return StringFactory.graphString(this, "coordLoc:" + this.coordinatorLocator + " graphName:" + this.graphName);
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

        private final ConcurrentHashMap<Thread, RAMCloudTransaction> threadLocalRCTXMap = new ConcurrentHashMap<>();

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
            return threadLocalRCTXMap.get(Thread.currentThread());
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
        private void doRollbackAllThreads() {
            threadLocalRCTXMap.forEach((thread, rctx) -> {
                try {
                    rctx.close();
                } catch (Exception e) {
                    logger.error("TorcGraphTransaction.doRollbackAllThreads(): could not close transaction of thread " + thread.getId());
                }

                logger.debug(String.format("TorcGraphTransaction.doRollbackAllThreads(): rolling back oustanding transaction of thread %d", thread.getId()));
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
                logger.debug(String.format("TorcGraphTransaction.doOpen(thread=%d), took %dus", us.getId(), (endTimeNs - startTimeNs) / 1000l));
            }
        }

        @Override
        public void doCommit() throws AbstractTransaction.TransactionException {
            long startTimeNs = 0;
            if (logger.isDebugEnabled()) {
                startTimeNs = System.nanoTime();
            }

            RAMCloudTransaction rctx = threadLocalRCTXMap.get(Thread.currentThread());

            try {
                if (!rctx.commitAndSync()) {
                    throw new AbstractTransaction.TransactionException("RAMCloud commitAndSync failed.");
                }
            } catch (ClientException ex) {
                throw new AbstractTransaction.TransactionException(ex);
            } finally {
                rctx.close();
                threadLocalRCTXMap.remove(Thread.currentThread());
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

            RAMCloudTransaction rctx = threadLocalRCTXMap.get(Thread.currentThread());

            try {
                rctx.close();
            } catch (Exception e) {
                throw new AbstractTransaction.TransactionException(e);
            } finally {
                threadLocalRCTXMap.remove(Thread.currentThread());
            }

            if (logger.isDebugEnabled()) {
                long endTimeNs = System.nanoTime();
                logger.debug(String.format("TorcGraphTransaction.doRollback(thread=%d), took %dus", Thread.currentThread().getId(), (endTimeNs - startTimeNs) / 1000l));
            }
        }

        @Override
        public boolean isOpen() {
            boolean isOpen = (threadLocalRCTXMap.get(Thread.currentThread()) != null);
            
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

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.commons.configuration.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.PropertyConfigurator;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.ellitron.tinkerpop.gremlin.ramcloud.structure.util.RAMCloudHelper;

/**
 *
 * @author ellitron
 */
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
public final class RAMCloudGraph implements Graph {
    private static final Logger LOGGER = LoggerFactory.getLogger(RAMCloudGraph.class);
    
    static {
        BasicConfigurator.configure();
    }
    
    // Configuration keys.
    public static final String CONFIG_GRAPH_NAME = "gremlin.ramcloud.graphName";
    public static final String CONFIG_COORD_LOC = "gremlin.ramcloud.coordinatorLocator";
    public static final String CONFIG_NUM_MASTER_SERVERS = "gremlin.ramcloud.numMasterServers";
    public static final String CONFIG_LOG_LEVEL = "gremlin.ramcloud.logLevel";
    
    // TODO: Make graph name a configuration parameter to enable separate graphs
    // to co-exist in the same ramcloud cluster.
    
    // Constants.
    private static final String ID_TABLE_NAME = "idTable";
    private static final String VERTEX_TABLE_NAME = "vertexTable";
    private static final String EDGE_TABLE_NAME = "edgeTable";
    private static final String LARGEST_CLIENT_ID_KEY = "largestClientId";
    private static final int MAX_TX_RETRY_COUNT = 100;
    
    // Normal private members.
    private final Configuration configuration;
    private final String coordinatorLocator;
    private final int totalMasterServers;
    private final RAMCloud ramcloud;
    private final long idTableId, vertexTableId, edgeTableId;
    private final String graphName;
    private final long clientId;
    private long nextLocalVertexId = 1;
//    private RAMCloudTransaction tx;
    
    private RAMCloudGraph(final Configuration configuration) {
        this.configuration = configuration;
        
        graphName = configuration.getString(CONFIG_GRAPH_NAME);
        coordinatorLocator = configuration.getString(CONFIG_COORD_LOC);
        totalMasterServers = configuration.getInt(CONFIG_NUM_MASTER_SERVERS);
        
        try {
            ramcloud = new RAMCloud(coordinatorLocator);
        } catch(ClientException e) {
            System.out.println(e.toString());
            throw e;
        }
        
        idTableId = ramcloud.createTable(graphName + "_" + ID_TABLE_NAME, totalMasterServers);
        vertexTableId = ramcloud.createTable(graphName + "_" + VERTEX_TABLE_NAME, totalMasterServers);
        edgeTableId = ramcloud.createTable(graphName + "_" + EDGE_TABLE_NAME, totalMasterServers);
        
        clientId = ramcloud.incrementInt64(idTableId, LARGEST_CLIENT_ID_KEY.getBytes(), 1, null);
        
//        tx = new RAMCloudTransaction(ramcloud);
    }
    
    public static RAMCloudGraph open(final Configuration configuration) {
        return new RAMCloudGraph(configuration);
    }
    
    private byte[] getNextVertexId() {
        return RAMCloudHelper.makeVertexId(clientId, nextLocalVertexId++);
    }
    
    @Override
    public Vertex addVertex(final Object... keyValues) {
        // Validate key/value pairs
        if (keyValues.length % 2 != 0)
            throw Element.Exceptions.providedKeyValuesMustBeAMultipleOfTwo();
        for (int i = 0; i < keyValues.length; i = i + 2) {
            if (!(keyValues[i] instanceof String) && !(keyValues[i] instanceof T))
                throw Element.Exceptions.providedKeyValuesMustHaveALegalKeyOnEvenIndices();
            if (keyValues[i].equals(T.id))
                throw Vertex.Exceptions.userSuppliedIdsNotSupported();
            if (!(keyValues[i+1] instanceof String))
                throw Property.Exceptions.dataTypeOfPropertyValueNotSupported(keyValues[i+1]);
        }
        
        // Create property map.
        Map<String, String> properties = new HashMap<>();
        // TODO: think about moving label to its own RAMCloud object.
        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);
        properties.put(T.label.getAccessor(), label);
        for (int i = 0; i < keyValues.length; i = i + 2) {
            if (keyValues[i] instanceof String)
                properties.put((String)keyValues[i], (String)keyValues[i+1]);
        }
        
        byte[] vertexId = getNextVertexId();
        
        ramcloud.write(vertexTableId, RAMCloudHelper.getVertexPropertiesKey(vertexId), RAMCloudHelper.serializeProperties(properties).array());
        
        return new RAMCloudVertex(this, vertexId, label);
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
        ElementHelper.validateMixedElementIds(RAMCloudVertex.class, vertexIds);
        
        List<Vertex> list = new ArrayList<>();
        if (vertexIds.length > 0) {
            if (vertexIds[0] instanceof RAMCloudVertex) {
                return Arrays.asList((Vertex[]) vertexIds).iterator();
            }
            
            for (int i = 0; i < vertexIds.length; ++i) {
                byte[] vertexId;
                if (vertexIds[i] instanceof RAMCloudVertex) {
                    vertexId = ((RAMCloudVertex) vertexIds[i]).id();
                } else {
                    // TODO: Is this needed? Did not do this for edges.
                    if (!RAMCloudHelper.validateVertexId(vertexIds[i])) {
//                        throw Vertex.Exceptions.userSuppliedIdsOfThisTypeNotSupported();
                        throw Graph.Exceptions.elementNotFound(RAMCloudVertex.class, vertexIds[i]);
                    }

                    vertexId = (byte[]) vertexIds[i];
                }

                RAMCloudObject obj;
                try {
                    obj = ramcloud.read(vertexTableId, RAMCloudHelper.getVertexPropertiesKey(vertexId));
                } catch (ObjectDoesntExistException e) {
                    throw Graph.Exceptions.elementNotFound(RAMCloudVertex.class, vertexIds[i]);
                }

                Map<String, String> properties = RAMCloudHelper.deserializeProperties(obj);

                list.add(new RAMCloudVertex(this, vertexId, properties.get(T.label.getAccessor())));
            }

            return list.iterator();
        } else {
            // TODO: This is so horribly wrong to have to do things this way...
            // Need a better way to scan all vertices in the database, although
            // admittedly this ought to be a very rare operation.
            TableIterator iterator = ramcloud.getTableIterator(vertexTableId);
            iterator.forEachRemaining((obj) -> {
                if (RAMCloudHelper.isVertexPropertiesKey(obj.getKey())) {
                    byte[] vertexId = RAMCloudHelper.parseVertexIdFromKey(obj.getKey());
                    Map<String, String> props = RAMCloudHelper.deserializeProperties(obj);
                    list.add(new RAMCloudVertex(this, vertexId, props.get(T.label.getAccessor())));
                }
            });
            
            return list.iterator();
        }
    }

    @Override
    public Iterator<Edge> edges(final Object... edgeIds) {
        ElementHelper.validateMixedElementIds(RAMCloudEdge.class, edgeIds);
        
        if (edgeIds.length > 0) {
            if (edgeIds[0] instanceof RAMCloudEdge) {
                return Arrays.asList((Edge[]) edgeIds).iterator();
            }
            
            List<Edge> list = new ArrayList<>();
            for (int i = 0; i < edgeIds.length; ++i) {
                if (RAMCloudHelper.validateEdgeId(edgeIds[i]))
                    list.add(new RAMCloudEdge(this, (byte[]) edgeIds[i], RAMCloudHelper.parseLabelFromEdgeId((byte[]) edgeIds[i])));
                else 
//                    throw Edge.Exceptions.userSuppliedIdsOfThisTypeNotSupported();
                    // TODO: Work out what's the right thing to do here. Must 
                    // throw this for now to support
                    // shouldHaveExceptionConsistencyWhenFindEdgeByIdThatIsNonExistentViaIterator
                    throw Graph.Exceptions.elementNotFound(RAMCloudEdge.class, edgeIds[i]);
            }

            return list.iterator();
        } else {
            List<Edge> list = new ArrayList<>();
            TableIterator iterator = ramcloud.getTableIterator(vertexTableId);
            iterator.forEachRemaining((obj) -> {
                if (RAMCloudHelper.isVertexEdgeListKey(obj.getKey())) {
                    byte[] vertexId = RAMCloudHelper.parseVertexIdFromKey(obj.getKey());
                    String label = RAMCloudHelper.parseEdgeLabelFromKey(obj.getKey());
                    Direction dir = RAMCloudHelper.parseEdgeDirectionFromKey(obj.getKey());
                    List<byte[]> neighborIds = RAMCloudHelper.parseNeighborIdsFromEdgeList(obj);
                    for (byte[] neighborId : neighborIds) {
                        // Only count in one direction, since each edge appears 
                        // in both an outgoing edge list and a corresponding 
                        // incoming edge list.
                        if (dir.equals(Direction.OUT))
                            list.add(new RAMCloudEdge(this, RAMCloudHelper.makeEdgeId(vertexId, neighborId, label), label));
//                         
//                        if (dir.equals(Direction.IN))
//                            list.add(new RAMCloudEdge(this, RAMCloudHelper.makeEdgeId(neighborId, vertexId, label), label));
                    }
                }
            });
            
            return list.iterator();
        }
    }

    @Override
    public Transaction tx() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
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
        ramcloud.disconnect();
    }
    
    public void deleteDatabase() {
        ramcloud.dropTable(graphName + "_" + ID_TABLE_NAME);
        ramcloud.dropTable(graphName + "_" + VERTEX_TABLE_NAME);
        ramcloud.dropTable(graphName + "_" + EDGE_TABLE_NAME);
    }
    
    public void deleteDatabaseAndCloseConnection() {
        ramcloud.dropTable(graphName + "_" + ID_TABLE_NAME);
        ramcloud.dropTable(graphName + "_" + VERTEX_TABLE_NAME);
        ramcloud.dropTable(graphName + "_" + EDGE_TABLE_NAME);
        ramcloud.disconnect();
    }
    
    @Override
    public String toString() {
        return StringFactory.graphString(this, "coordLoc:" + this.coordinatorLocator + " graphName:" + this.graphName);
    }
    
    @Override
    public Features features() {
        return new RAMCloudGraphFeatures();
    }

    /** Methods called by RAMCloudVertex. */
    
    void removeVertex(final RAMCloudVertex vertex) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    Edge addEdge(final RAMCloudVertex outVertex, final RAMCloudVertex inVertex, final String label, final Object[] keyValues) {
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

        ByteBuffer serializedProperties = RAMCloudHelper.serializeProperties(properties);
        int serializedEdgeLength = Long.BYTES * 2 + Short.BYTES + serializedProperties.array().length;

        try {
            RAMCloudObject outVertEdgeListRCObj = ramcloud.read(vertexTableId, RAMCloudHelper.getVertexEdgeListKey(outVertex.id(), label, Direction.OUT));

            ByteBuffer newOutVertEdgeList = ByteBuffer.allocate(serializedEdgeLength + outVertEdgeListRCObj.getValueBytes().length);

            newOutVertEdgeList.put(inVertex.id());
            newOutVertEdgeList.putShort((short) serializedProperties.array().length);
            newOutVertEdgeList.put(serializedProperties.array());
            newOutVertEdgeList.put(outVertEdgeListRCObj.getValueBytes());

            ramcloud.write(vertexTableId, RAMCloudHelper.getVertexEdgeListKey(outVertex.id(), label, Direction.OUT), newOutVertEdgeList.array());
        } catch (ObjectDoesntExistException e) {
            ByteBuffer newOutVertEdgeList = ByteBuffer.allocate(serializedEdgeLength);
            
            newOutVertEdgeList.put(inVertex.id());
            newOutVertEdgeList.putShort((short) serializedProperties.array().length);
            newOutVertEdgeList.put(serializedProperties.array());
            
            ramcloud.write(vertexTableId, RAMCloudHelper.getVertexEdgeListKey(outVertex.id(), label, Direction.OUT), newOutVertEdgeList.array());
            
            try {
                RAMCloudObject outVertEdgeLabelListRCObj = ramcloud.read(vertexTableId, RAMCloudHelper.getVertexEdgeLabelListKey(outVertex.id()));
                List<String> edgeLabelList = RAMCloudHelper.deserializeEdgeLabelList(outVertEdgeLabelListRCObj);
                if (!edgeLabelList.contains(label)) {
                    edgeLabelList.add(label);
                    ramcloud.write(vertexTableId, RAMCloudHelper.getVertexEdgeLabelListKey(outVertex.id()), RAMCloudHelper.serializeEdgeLabelList(edgeLabelList).array());
                }
            } catch (ObjectDoesntExistException e2) {
                List<String> edgeLabelList = new ArrayList<>();
                edgeLabelList.add(label);
                ramcloud.write(vertexTableId, RAMCloudHelper.getVertexEdgeLabelListKey(outVertex.id()), RAMCloudHelper.serializeEdgeLabelList(edgeLabelList).array());
            }
        }
        
        try {
            RAMCloudObject inVertEdgeListRCObj = ramcloud.read(vertexTableId, RAMCloudHelper.getVertexEdgeListKey(inVertex.id(), label, Direction.IN));

            ByteBuffer newInVertEdgeList = ByteBuffer.allocate(serializedEdgeLength + inVertEdgeListRCObj.getValueBytes().length);

            newInVertEdgeList.put(outVertex.id());
            newInVertEdgeList.putShort((short) serializedProperties.array().length);
            newInVertEdgeList.put(serializedProperties.array());
            newInVertEdgeList.put(inVertEdgeListRCObj.getValueBytes());

            ramcloud.write(vertexTableId, RAMCloudHelper.getVertexEdgeListKey(inVertex.id(), label, Direction.IN), newInVertEdgeList.array());
        } catch (ObjectDoesntExistException e) {
            ByteBuffer newInVertEdgeList = ByteBuffer.allocate(serializedEdgeLength);

            newInVertEdgeList.put(outVertex.id());
            newInVertEdgeList.putShort((short) serializedProperties.array().length);
            newInVertEdgeList.put(serializedProperties.array());

            ramcloud.write(vertexTableId, RAMCloudHelper.getVertexEdgeListKey(inVertex.id(), label, Direction.IN), newInVertEdgeList.array());
            
            try {
                RAMCloudObject inVertEdgeLabelListRCObj = ramcloud.read(vertexTableId, RAMCloudHelper.getVertexEdgeLabelListKey(inVertex.id()));
                List<String> edgeLabelList = RAMCloudHelper.deserializeEdgeLabelList(inVertEdgeLabelListRCObj);
                if (!edgeLabelList.contains(label)) {
                    edgeLabelList.add(label);
                    ramcloud.write(vertexTableId, RAMCloudHelper.getVertexEdgeLabelListKey(inVertex.id()), RAMCloudHelper.serializeEdgeLabelList(edgeLabelList).array());
                }
            } catch (ObjectDoesntExistException e2) {
                List<String> edgeLabelList = new ArrayList<>();
                edgeLabelList.add(label);
                ramcloud.write(vertexTableId, RAMCloudHelper.getVertexEdgeLabelListKey(inVertex.id()), RAMCloudHelper.serializeEdgeLabelList(edgeLabelList).array());
            }
        }
        
        return new RAMCloudEdge(this, RAMCloudHelper.makeEdgeId(outVertex.id(), inVertex.id(), label), label);
    }

    Iterator<Edge> vertexEdges(final RAMCloudVertex vertex, final Direction direction, final String[] edgeLabels) {
        List<Edge> edges = new ArrayList<>();
        List<String> labels = Arrays.asList(edgeLabels);
        
        if (labels.isEmpty()) {
            try {
                RAMCloudObject vertEdgeLabelListRCObj = ramcloud.read(vertexTableId, RAMCloudHelper.getVertexEdgeLabelListKey(vertex.id()));
                labels = RAMCloudHelper.deserializeEdgeLabelList(vertEdgeLabelListRCObj);
                if (labels.isEmpty())
                    return edges.iterator();
            } catch (ObjectDoesntExistException e) {
                return edges.iterator();
            }
        }
        
        for (String label : labels) {
            if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH)) {
                try {
                    RAMCloudObject edgeListRCObj = ramcloud.read(vertexTableId, RAMCloudHelper.getVertexEdgeListKey(vertex.id(), label, Direction.OUT));
                    List<byte[]> inVertexIds = RAMCloudHelper.parseNeighborIdsFromEdgeList(edgeListRCObj);
                    for (byte[] inVertexId : inVertexIds) {
                        edges.add(new RAMCloudEdge(this, RAMCloudHelper.makeEdgeId(vertex.id(), inVertexId, label), label));
                    }
                } catch (ObjectDoesntExistException e) {
                    // Catch and ignore. This case is ok.
                }
            } 
            
            if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH)) {
                try {
                    RAMCloudObject edgeListRCObj = ramcloud.read(vertexTableId, RAMCloudHelper.getVertexEdgeListKey(vertex.id(), label, Direction.IN));
                    List<byte[]> outVertexIds = RAMCloudHelper.parseNeighborIdsFromEdgeList(edgeListRCObj);
                    for (byte[] outVertexId : outVertexIds) {
                        edges.add(new RAMCloudEdge(this, RAMCloudHelper.makeEdgeId(outVertexId, vertex.id(), label), label));
                    }
                } catch (ObjectDoesntExistException e) {
                    // Catch and ignore. This case is ok.
                }
            } 
        }
        
        return edges.iterator();
    }

    Iterator<Vertex> vertexNeighbors(final RAMCloudVertex vertex, final Direction direction, final String[] edgeLabels) {
        List<Vertex> vertices = new ArrayList<>();
        List<String> labels = Arrays.asList(edgeLabels);
        
        if (labels.isEmpty()) {
            try {
                RAMCloudObject vertEdgeLabelListRCObj = ramcloud.read(vertexTableId, RAMCloudHelper.getVertexEdgeLabelListKey(vertex.id()));
                labels = RAMCloudHelper.deserializeEdgeLabelList(vertEdgeLabelListRCObj);
                if (labels.isEmpty())
                    return vertices.iterator();
            } catch (ObjectDoesntExistException e) {
                return vertices.iterator();
            }
        }
        
        for (String label : labels) {
            if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH)) {
                try {
                    RAMCloudObject edgeListRCObj = ramcloud.read(vertexTableId, RAMCloudHelper.getVertexEdgeListKey(vertex.id(), label, Direction.OUT));
                    List<byte[]> neighborIds = RAMCloudHelper.parseNeighborIdsFromEdgeList(edgeListRCObj);
                    for (byte[] neighborId : neighborIds) {
                        // TODO: make label a separate object in RAMCloud, not a part of the vertex's properties.
                        RAMCloudObject neighborPropsRCObj = ramcloud.read(vertexTableId, RAMCloudHelper.getVertexPropertiesKey(neighborId));
                        Map<String, String> neighborProps = RAMCloudHelper.deserializeProperties(neighborPropsRCObj);
                        String neighborLabel = neighborProps.get(T.label.getAccessor());
                        vertices.add(new RAMCloudVertex(this, neighborId, neighborLabel));
                    }
                } catch (ObjectDoesntExistException e) {
                    // Catch and ignore. This case is ok.
                }
            } 
            
            if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH)) {
                try {
                    RAMCloudObject edgeListRCObj = ramcloud.read(vertexTableId, RAMCloudHelper.getVertexEdgeListKey(vertex.id(), label, Direction.IN));
                    List<byte[]> neighborIds = RAMCloudHelper.parseNeighborIdsFromEdgeList(edgeListRCObj);
                    for (byte[] neighborId : neighborIds) {
                        // TODO: make label a separate object in RAMCloud, not a part of the vertex's properties.
                        RAMCloudObject neighborPropsRCObj = ramcloud.read(vertexTableId, RAMCloudHelper.getVertexPropertiesKey(neighborId));
                        Map<String, String> neighborProps = RAMCloudHelper.deserializeProperties(neighborPropsRCObj);
                        String neighborLabel = neighborProps.get(T.label.getAccessor());
                        vertices.add(new RAMCloudVertex(this, neighborId, neighborLabel));
                    }
                } catch (ObjectDoesntExistException e) {
                    // Catch and ignore. This case is ok.
                }
            } 
        }
        
        return vertices.iterator();
    }

    <V> Iterator<VertexProperty<V>> getVertexProperties(final RAMCloudVertex vertex, final String[] propertyKeys) {
        RAMCloudObject obj = ramcloud.read(vertexTableId, RAMCloudHelper.getVertexPropertiesKey(vertex.id()));
        
        Map<String, String> properties = RAMCloudHelper.deserializeProperties(obj);
        
        List<VertexProperty<V>> propList = new ArrayList<>();
        
        for (String key: propertyKeys) {
            if (properties.containsKey(key))
                propList.add(new RAMCloudVertexProperty(vertex, key, properties.get(key)));
            else
                throw Property.Exceptions.propertyDoesNotExist(vertex, key);
        }
        
        return propList.iterator();
    }

    <V> VertexProperty<V> setVertexProperty(final RAMCloudVertex vertex, final VertexProperty.Cardinality cardinality, final String key, final V value, final Object[] keyValues) {
        if (keyValues != null) 
            throw VertexProperty.Exceptions.metaPropertiesNotSupported();
        
        if (!(value instanceof String))
            throw Property.Exceptions.dataTypeOfPropertyValueNotSupported(value);
        
        RAMCloudObject obj = ramcloud.read(vertexTableId, RAMCloudHelper.getVertexPropertiesKey(vertex.id()));

        Map<String, String> properties = RAMCloudHelper.deserializeProperties(obj);

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

        ramcloud.write(vertexTableId, RAMCloudHelper.getVertexPropertiesKey(vertex.id()), RAMCloudHelper.serializeProperties(properties).array());

        return new RAMCloudVertexProperty(vertex, key, value);
    }

    /** Methods called by RAMCloudEdge. */
    
    void removeEdge(final RAMCloudEdge edge) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    Iterator<Vertex> edgeVertices(final RAMCloudEdge edge, final Direction direction) {
        List<Vertex> list = new ArrayList<>();
        
        if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH)) {
            byte[] outVertexId = RAMCloudHelper.parseOutVertexIdFromEdgeId(edge.id());
            // TODO: need to store vertex label in separate ramcloud object.
            RAMCloudObject obj = ramcloud.read(vertexTableId, RAMCloudHelper.getVertexPropertiesKey(outVertexId));
            Map<String, String> properties = RAMCloudHelper.deserializeProperties(obj);
            list.add(new RAMCloudVertex(this, outVertexId, properties.get(T.label.getAccessor())));
        }
        
        if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH)) {
            byte[] inVertexId = RAMCloudHelper.parseInVertexIdFromEdgeId(edge.id());
            // TODO: need to store vertex label in separate ramcloud object.
            RAMCloudObject obj = ramcloud.read(vertexTableId, RAMCloudHelper.getVertexPropertiesKey(inVertexId));
            Map<String, String> properties = RAMCloudHelper.deserializeProperties(obj);
            list.add(new RAMCloudVertex(this, inVertexId, properties.get(T.label.getAccessor())));
        }
        
        return list.iterator();
    }

    <V> Iterator<Property<V>> getEdgeProperties(final RAMCloudEdge edge, final String[] propertyKeys) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    <V> Property<V> setEdgeProperty(final RAMCloudEdge edge, final String key, final V value) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
    public class RAMCloudGraphFeatures implements Features {

        private RAMCloudGraphFeatures() {
        }

        @Override
        public Features.GraphFeatures graph() {
            return new RAMCloudGraphGraphFeatures();
        }

        @Override
        public Features.VertexFeatures vertex() {
            return new RAMCloudGraphVertexFeatures();
        }
        
        @Override
        public Features.EdgeFeatures edge() {
            return new RAMCloudGraphEdgeFeatures();
        }

        @Override
        public String toString() {
            return StringFactory.featureString(this);
        }
    }

    public class RAMCloudGraphGraphFeatures implements Features.GraphFeatures {

        private RAMCloudGraphGraphFeatures() {
        }

        @Override
        public boolean supportsComputer() {
            return false;
        }

        @Override
        public boolean supportsPersistence() {
            return false;
        }

        @Override
        public boolean supportsConcurrentAccess() {
            return false;
        }

        @Override
        public boolean supportsTransactions() {
            return false;
        }

        @Override
        public boolean supportsThreadedTransactions() {
            return false;
        }

        @Override
        public Features.VariableFeatures variables() {
            return new RAMCloudGraphVariableFeatures() {
            };
        }

    }
    
    public class RAMCloudGraphVertexFeatures implements Features.VertexFeatures {

        private RAMCloudGraphVertexFeatures() {
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
            return new RAMCloudGraphVertexPropertyFeatures();
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

    public class RAMCloudGraphEdgeFeatures implements Features.EdgeFeatures {

        private RAMCloudGraphEdgeFeatures() {
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
            return new RAMCloudGraphEdgePropertyFeatures() {
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

    public class RAMCloudGraphVertexPropertyFeatures implements Features.VertexPropertyFeatures {

        private RAMCloudGraphVertexPropertyFeatures() {
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
    
    public class RAMCloudGraphEdgePropertyFeatures implements Features.EdgePropertyFeatures {

        private RAMCloudGraphEdgePropertyFeatures() {
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
    
    public class RAMCloudGraphVariableFeatures implements Features.VariableFeatures {

        private RAMCloudGraphVariableFeatures() {
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

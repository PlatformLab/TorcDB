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
    private static final String LARGEST_ID_KEY = "largestId";
    private static final int MAX_TX_RETRY_COUNT = 100;
    
    // Normal private members.
    private final Configuration configuration;
    private final String coordinatorLocator;
    private final int totalMasterServers;
    private final RAMCloud ramcloud;
    private final long idTableId, vertexTableId, edgeTableId;
    private final String graphName;
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
        
//        tx = new RAMCloudTransaction(ramcloud);
    }
    
    public static RAMCloudGraph open(final Configuration configuration) {
        return new RAMCloudGraph(configuration);
    }
    
    @Override
    public Vertex addVertex(Object... keyValues) {
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
        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);
        properties.put(T.label.getAccessor(), label);
        for (int i = 0; i < keyValues.length; i = i + 2) {
            if (keyValues[i] instanceof String)
                properties.put((String)keyValues[i], (String)keyValues[i+1]);
        }
        
        long id = ramcloud.incrementInt64(idTableId, LARGEST_ID_KEY.getBytes(), 1, null);
        
        // Serialize the key/value pairs, starting with the label.
        String key = String.format("%d:%s", id, "properties");
        ByteBuffer value = RAMCloudHelper.serializeProperties(properties);
        
        ramcloud.write(vertexTableId, key, value.array());
        
        RAMCloudVertex resultVertex = new RAMCloudVertex(this, id, label);
        
        return resultVertex;
    }
    
    @Override
    public <C extends GraphComputer> C compute(Class<C> type) throws IllegalArgumentException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Iterator<Vertex> vertices(Object... vertexIds) {
        ElementHelper.validateMixedElementIds(RAMCloudVertex.class, vertexIds);
        
        List<Vertex> list = new ArrayList<>();
        
        for (int i = 0; i < vertexIds.length; ++i) {
            if(!(vertexIds[i] instanceof Long))
                throw Vertex.Exceptions.userSuppliedIdsOfThisTypeNotSupported();
            
            String key = String.format("%d:%s", (Long)vertexIds[i], "properties");
            
            RAMCloudObject obj;
            try {
                obj = ramcloud.read(vertexTableId, key);
            } catch (ObjectDoesntExistException e) {
                throw Graph.Exceptions.elementNotFound(RAMCloudVertex.class, (Long)vertexIds[i]);
            }
            
            ByteBuffer value = ByteBuffer.allocate(obj.getValueBytes().length);
            value.put(obj.getValueBytes());
            value.rewind();
            
            short strLen = value.getShort();
            byte labelKey[] = new byte[strLen];
            value.get(labelKey);
            strLen = value.getShort();
            byte label[] = new byte[strLen];
            value.get(label);
            
            list.add(new RAMCloudVertex(this, (Long)vertexIds[i], new String(label)));
        }
        
        return list.iterator();
    }

    @Override
    public Iterator<Edge> edges(Object... os) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
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
        ramcloud.dropTable(ID_TABLE_NAME);
        ramcloud.dropTable(VERTEX_TABLE_NAME);
        ramcloud.dropTable(EDGE_TABLE_NAME);
    }
    
    public void deleteDatabaseAndCloseConnection() {
        ramcloud.dropTable(ID_TABLE_NAME);
        ramcloud.dropTable(VERTEX_TABLE_NAME);
        ramcloud.dropTable(EDGE_TABLE_NAME);
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
    
    void removeVertex(RAMCloudVertex vertex) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    Edge addEdge(RAMCloudVertex vertex, Vertex inVertex, String label, Object[] keyValues) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    Iterator<Edge> vertexEdges(RAMCloudVertex vertex, Direction direction, String[] edgeLabels) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    Iterator<Vertex> vertexNeighbors(RAMCloudVertex vertex, Direction direction, String[] edgeLabels) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    <V> Iterator<VertexProperty<V>> getVertexProperties(final RAMCloudVertex vertex, final String[] propertyKeys) {
        String key = String.format("%d:%s", (long) vertex.id(), "properties");
        RAMCloudObject obj = ramcloud.read(vertexTableId, key);
        
        ByteBuffer value = ByteBuffer.allocate(obj.getValueBytes().length);
        value.put(obj.getValueBytes());
        value.rewind();

        short strLen = value.getShort();
        byte labelKey[] = new byte[strLen];
        value.get(labelKey);
        strLen = value.getShort();
        byte label[] = new byte[strLen];
        value.get(label);
        
        List<VertexProperty<V>> propList = new ArrayList<>();
        
        List<String> propertyKeysList = Arrays.asList(propertyKeys);
        
        while(value.hasRemaining()) {
            strLen = value.getShort();
            byte propKey[] = new byte[strLen];
            value.get(propKey);
            strLen = value.getShort();
            byte propVal[] = new byte[strLen];
            value.get(propVal);
            
            if (propertyKeysList.contains(new String(propKey)))
                propList.add(new RAMCloudVertexProperty(vertex, new String(propKey), new String(propVal)));
        }
        
        return propList.iterator();
    }

    <V> VertexProperty<V> setVertexProperty(final RAMCloudVertex vertex, final VertexProperty.Cardinality cardinality, final String key, final V value, final Object[] keyValues) {
        if (keyValues != null) 
            throw VertexProperty.Exceptions.metaPropertiesNotSupported();
        
        if (!(value instanceof String))
            throw Property.Exceptions.dataTypeOfPropertyValueNotSupported(value);
        
        String rcKey = String.format("%d:%s", (long) vertex.id(), "properties");
        
        RAMCloudObject obj = ramcloud.read(vertexTableId, rcKey);

        ByteBuffer rcValue = ByteBuffer.allocate(obj.getValueBytes().length);
        rcValue.put(obj.getValueBytes());
        rcValue.rewind();

        Map<String, String> properties = RAMCloudHelper.deserializeProperties(rcValue);

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

        ByteBuffer newRcValue = RAMCloudHelper.serializeProperties(properties);

        ramcloud.write(vertexTableId, rcKey, newRcValue.array());

        return new RAMCloudVertexProperty(vertex, key, value);
    }

    /** Methods called by RAMCloudEdge. */
    
    void removeEdge(RAMCloudEdge edge) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    Iterator<Vertex> edgeVertices(RAMCloudEdge edge, Direction direction) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    <V> Iterator<Property<V>> getEdgeProperties(RAMCloudEdge edge, String[] propertyKeys) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    <V> Property<V> setEdgeProperty(RAMCloudEdge edge, String key, V value) {
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

    public class RAMCloudGraphEdgeFeatures implements Features.EdgeFeatures {

        private RAMCloudGraphEdgeFeatures() {
        }
        
        @Override
        public boolean supportsAddEdges() {
            return false;
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

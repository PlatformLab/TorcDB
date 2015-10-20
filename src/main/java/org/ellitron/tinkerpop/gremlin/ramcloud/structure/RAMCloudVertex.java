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

import edu.stanford.ramcloud.RAMCloud;
import edu.stanford.ramcloud.RAMCloudObject;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

/**
 *
 * @author ellitron
 */
public class RAMCloudVertex implements Vertex {
    private final RAMCloudGraph graph;
    private Long id;
    private String label;
    
    public RAMCloudVertex(final RAMCloudGraph graph, final long id, final String label) {
        this.graph = graph;
        this.id = id;
        this.label = label;
    }
    
    @Override
    public Iterator<Edge> edges(final Direction direction, final String... edgeLabels) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
    @Override
    public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
    @Override
    public void remove() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
    @Override
    public Long id() {
        return id;
    }
    
    @Override
    public String label() {
        return label;
    }
    
    @Override
    public Graph graph() {
        return graph;
    }
    
    @Override
    public Iterator<Vertex> vertices(final Direction direction, final String... edgeLabels) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
    @Override
    public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
        return graph.getVertexProperties(this, propertyKeys);
    }
    
    @Override
    public <V> VertexProperty<V> property(final VertexProperty.Cardinality cardinality, final String key, final V value, final Object... keyValues) {
        return graph.setVertexProperty(this, cardinality, key, value, keyValues);
    }

    @Override
    public <V> VertexProperty<V> property(final String key, final V value) {
        return property(VertexProperty.Cardinality.single, key, value);
    }
}

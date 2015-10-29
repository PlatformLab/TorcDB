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
public class RAMCloudVertex extends RAMCloudElement implements Vertex {
    
    public RAMCloudVertex(final RAMCloudGraph graph, final byte[] id, final String label) {
        super(graph, id, label);
    }

    @Override
    public void remove() {
        graph.removeVertex(this);
    }

    @Override
    public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
        return graph.addEdge(this, (RAMCloudVertex) inVertex, label, keyValues);
    }
    
    @Override
    public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
        return graph.vertexEdges(this, direction, edgeLabels);
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
        return graph.vertexNeighbors(this, direction, edgeLabels);
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
        return graph.getVertexProperties(this, propertyKeys);
    }

    @Override
    public <V> VertexProperty<V> property(String key, V value) {
        return property(VertexProperty.Cardinality.single, key, value);
    }
    
    @Override
    public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value, Object... keyValues) {
        return graph.setVertexProperty(this, cardinality, key, value, keyValues);
    }
}

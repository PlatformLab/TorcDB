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

import java.util.Iterator;
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
public class RAMCloudEdge extends RAMCloudElement implements Edge {
    
    public RAMCloudEdge(final RAMCloudGraph graph, final String id, final String label) {
        super(graph, id, label);
    }

    @Override
    public void remove() {
        graph.removeEdge(this);
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction) {
        return graph.edgeVertices(this, direction);
    }

    @Override
    public <V> Iterator<Property<V>> properties(String... propertyKeys) {
        return graph.getEdgeProperties(this, propertyKeys);
    }
    
    @Override
    public <V> Property<V> property(String key, V value) {
        return graph.setEdgeProperty(this, key, value);
    }
    
}

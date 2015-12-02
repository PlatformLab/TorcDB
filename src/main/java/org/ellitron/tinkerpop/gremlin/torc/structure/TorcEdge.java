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
package org.ellitron.tinkerpop.gremlin.torc.structure;

import java.math.BigInteger;
import java.util.Iterator;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.ellitron.tinkerpop.gremlin.torc.structure.util.TorcHelper;

/**
 *
 * @author Jonathan Ellithorpe <jde@cs.stanford.edu>
 */
public class TorcEdge implements Edge, Element {
    private final TorcGraph graph;
    // TODO: This needs to be wrapped in its own type.
    private byte[] id;
    private String label;
    
    public enum Directionality {
        DIRECTED,
        UNDIRECTED;
    }
    
    public TorcEdge(final TorcGraph graph, final byte[] id, final String label) {
        this.graph = graph;
        this.id = id;
        this.label = label;
    }

    @Override
    public byte[] id() {
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
    
    @Override
    public boolean equals(final Object object) {
        if (object instanceof TorcEdge) {
            byte[] otherId = ((TorcEdge) object).id;
            
            if (otherId.length != id.length)
                return false;
            
            for (int i = 0; i < id.length; ++i) {
                if (otherId[i] != id[i])
                    return false;
            }
            
            return true;
        }
        
        return false;
    }
    
    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }
    
}

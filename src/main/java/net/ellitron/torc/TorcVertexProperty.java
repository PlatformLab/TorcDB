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

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 *
 * @author Jonathan Ellithorpe <jde@cs.stanford.edu>
 * @param <V>
 */
public class TorcVertexProperty<V> implements VertexProperty<V>, Element {
    private final TorcVertex vertex;
    private final String key;
    private final V value;
    
    
    public TorcVertexProperty(final TorcVertex vertex, final String key, V value) {
        this.vertex = vertex;
        this.key = key;
        this.value = value;
    }
    
    @Override
    public Object id() {
        return vertex.id();
    }

    @Override
    public String label() {
        return vertex.label();
    }

    @Override
    public Graph graph() {
        return vertex.graph();
    }
    @Override
    public void remove() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
    @Override
    public Vertex element() {
        return vertex;
    }

    @Override
    public <U> Iterator<Property<U>> properties(String... propertyKeys) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String key() {
        return key;
    }

    @Override
    public V value() throws NoSuchElementException {
        return value;
    }

    @Override
    public boolean isPresent() {
        return (value != null);
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}

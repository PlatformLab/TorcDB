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

import java.util.Iterator;
import java.util.Objects;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

/**
 *
 * @author Jonathan Ellithorpe <jde@cs.stanford.edu>
 */
public class TorcEdge implements Edge, Element {

    private final TorcGraph graph;
    private final UInt128 v1Id;
    private final UInt128 v2Id;
    private final Type type;
    private String label;

    public enum Type {

        DIRECTED,
        UNDIRECTED;
    }

    /**
     * Inner class that represents this edge's Id. The TorcEdge class itself is
     * the exclusive owner of all edge related information, including the source
     * and destination vertex IDs, the type of the edge (directed, undirected),
     * the edge label, the graph to which the edge belongs, etc. This inner
     * class simply provides methods that represent specifically the components
     * of the edge state that uniquely identify this edge, and does not hold any
     * copy of state.
     */
    public class Id {

        /**
         * Get TorcEdge which this Id represents.
         *
         * Since this is an inner class to TorcEdge, every Id has an enclosing
         * TorcEdge outer class instance which it represents the Id of. It is
         * sometimes useful to be able to get a reference to this edge given an
         * Id.
         *
         * @return TorcEdge which this Id represents.
         */
        public TorcEdge getEdge() {
            return TorcEdge.this;
        }

        /**
         * Creates a simple formatted string uniquely representing this TorcEdge
         * ID, in the format [id,id,type,label]. If the ID is for a directed
         * edge, then the first id is the source and the second is the
         * destination. Otherwise the least valued id is placed first (thus
         * defining a unique ID representation for undirected edges).
         *
         * @return String representation of this ID.
         */
        @Override
        public String toString() {
            if (type == Type.DIRECTED) {
                return String.format("[%s,%s,%d,%s]", v1Id.toString(), v2Id.toString(), type.ordinal(), label);
            } else {
                if (v1Id.compareTo(v2Id) < 0) {
                    return String.format("[%s,%s,%d,%s]", v1Id.toString(), v2Id.toString(), type.ordinal(), label);
                } else {
                    return String.format("[%s,%s,%d,%s]", v2Id.toString(), v1Id.toString(), type.ordinal(), label);
                }
            }
        }

        /**
         * Equality is defined by equality in vertex ids, types, and labels of
         * the edges represented by these IDs.
         *
         * @param that Object representing the thing to be tested for equality.
         * @return True if equal, false otherwise.
         */
        @Override
        public boolean equals(Object that) {
            if (!(that instanceof Id)) {
                return false;
            }

            TorcEdge thatEdge = ((Id) that).getEdge();

            return v1Id.equals(thatEdge.v1Id)
                    && v2Id.equals(thatEdge.v2Id)
                    && type.equals(thatEdge.type)
                    && label.equals(thatEdge.label);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int hashCode() {
            int hash = 3;
            hash = 59 * hash + Objects.hashCode(v1Id);
            hash = 59 * hash + Objects.hashCode(v2Id);
            hash = 59 * hash + Objects.hashCode(type);
            hash = 59 * hash + Objects.hashCode(label);
            return hash;
        }
    }

    public TorcEdge(final TorcGraph graph, UInt128 v1Id, UInt128 v2Id, Type type, final String label) {
        this.graph = graph;
        this.v1Id = v1Id;
        this.v2Id = v2Id;
        this.type = type;
        this.label = label;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Id id() {
        return new Id();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String label() {
        return label;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Graph graph() {
        return graph;
    }

    /**
     * Returns the source vertex ID in the case that this edge's type is
     * directed. If the edge type is undirected, then just returns one of the
     * vertex IDs (while {@link #getV2Id()} returns the other).
     *
     * @return Vertex ID.
     */
    public UInt128 getV1Id() {
        return v1Id;
    }

    /**
     * Returns the destination vertex ID in the case that this edge's type is
     * directed. If the edge type is undirected, then just returns one of the
     * vertex IDs (while {@link #getV1Id()} returns the other).
     *
     * @return Vertex ID.
     */
    public UInt128 getV2Id() {
        return v2Id;
    }

    /** 
     * Returns the {@link Type} of this edge.
     * 
     * @return Edge type.
     */
    public Type getType() {
        return type;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void remove() {
        graph.removeEdge(this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<Vertex> vertices(Direction direction) {
        return graph.edgeVertices(this, direction);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <V> Iterator<Property<V>> properties(String... propertyKeys) {
        return graph.getEdgeProperties(this, propertyKeys);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <V> Property<V> property(String key, V value) {
        return graph.setEdgeProperty(this, key, value);
    }

    /**
     * Creates a Neo4j-style string representation of this edge between vertices
     * a and b in the format: (a.id)-[:label]->(b.id) for a directed edge (a is
     * the source and b is the destination), and (a.id)-[:label]-(b.id) for an
     * undirected edge if a.id < b.id, otherwise it will be
     * (b.id)-[:label]-(a.id). This is so that each edge has one unique string
     * representation (otherwise undirected edges would have two valid
     * representations). This gives us the property that if e1 and e2 are edges,
     * if e1.equals(e2), then e1.toString().equals(e2.toString(). Note, however,
     * that the converse is not true in general. This is particularly the case
     * when edges that have the same vertex ids, type, and label, but exist in
     * separate databases. Those edges are not considered equal. See {@link
     * #equals}.
     *
     * @return Unique string representation of this edge.
     */
    @Override
    public String toString() {
        if (type == Type.DIRECTED) {
            return String.format("(%s)-[:%s]->(%s)", v1Id.toString(), label, v2Id.toString());
        } else {
            if (v1Id.compareTo(v2Id) < 0) {
                return String.format("(%s)-[:%s]-(%s)", v1Id.toString(), label, v2Id.toString());
            } else {
                return String.format("(%s)-[:%s]-(%s)", v2Id.toString(), label, v1Id.toString());
            }
        }
    }

    /**
     * This method defines equality to be equality in vertex ids, types, labels,
     * *and* graph. So, for example, if databases A and B have been loaded with
     * the exact same dataset, even though the structures are exactly the same,
     * the edges in A are not equal to those in B. They are considered distinct
     * edges.
     *
     * @param that Object representing the thing to be tested for equality.
     * @return True if equal, false otherwise.
     */
    @Override
    public boolean equals(final Object that) {
        if (!(that instanceof TorcEdge)) {
            return false;
        }

        TorcEdge thatEdge = (TorcEdge) that;

        return this.graph.equals(thatEdge.graph)
                && this.v1Id.equals(thatEdge.v1Id)
                && this.v2Id.equals(thatEdge.v2Id)
                && this.type.equals(thatEdge.type)
                && this.label.equals(thatEdge.label);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }
}

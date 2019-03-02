/* Copyright (c) 2015-2019 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */
package net.ellitron.torc;

import net.ellitron.torc.util.UInt128;
import net.ellitron.torc.util.TorcHelper;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;

/**
 *
 * @author Jonathan Ellithorpe (jde@cs.stanford.edu)
 */
public class TorcEdge implements Edge, Element {

  private final TorcGraph graph;
  private final TorcVertex v1;
  private final TorcVertex v2;
  private String label;
  private byte[] serializedProperties = null;
  private Map<Object, Object> properties = null;

  /**
   * Inner class that represents this edge's Id. The TorcEdge class itself is
   * the exclusive owner of all edge related information, including the source
   * and destination vertex IDs, the type of the edge (directed, undirected),
   * the edge label, the graph to which the edge belongs, etc. This inner class
   * simply provides methods that represent specifically the components of the
   * edge state that uniquely identify this edge, and does not hold any copy of
   * state.
   */
  public class Id {

    /**
     * Get TorcEdge which this Id represents.
     *
     * Since this is an inner class to TorcEdge, every Id has an enclosing
     * TorcEdge outer class instance which it represents the Id of. It is
     * sometimes useful to be able to get a reference to this edge given an Id.
     *
     * @return TorcEdge which this Id represents.
     */
    public TorcEdge getEdge() {
      return TorcEdge.this;
    }

    /**
     * Creates a simple formatted string uniquely representing this TorcEdge
     * ID, in the format [id,id,type,label]. If the ID is for a directed edge,
     * then the first id is the source and the second is the destination.
     * Otherwise the least valued id is placed first (thus defining a unique ID
     * representation for undirected edges).
     *
     * @return String representation of this ID.
     */
    @Override
    public String toString() {
      return String.format("(%s,%s,%s)", getV1Id().toString(),
          getV2Id().toString(), label);
    }

    /**
     * Equality is defined by equality in vertex ids, types, and labels of the
     * edges represented by these IDs.
     *
     * @param that Object representing the thing to be tested for equality.
     *
     * @return True if equal, false otherwise.
     */
    @Override
    public boolean equals(Object that) {
      if (!(that instanceof Id)) {
        return false;
      }

      TorcEdge thatEdge = ((Id) that).getEdge();

      return getV1Id().equals(thatEdge.getV1Id())
          && getV2Id().equals(thatEdge.getV2Id())
          && label.equals(thatEdge.label);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
      int hash = 3;
      hash = 59 * hash + Objects.hashCode(getV1Id());
      hash = 59 * hash + Objects.hashCode(getV2Id());
      hash = 59 * hash + Objects.hashCode(label);
      return hash;
    }
  }

  public TorcEdge(final TorcGraph graph, UInt128 v1Id, UInt128 v2Id,
      final String label) {
    this.graph = graph;
    this.v1 = new TorcVertex(graph, v1Id);
    this.v2 = new TorcVertex(graph, v2Id);
    this.label = label;
  }

  public TorcEdge(final TorcGraph graph, TorcVertex v1, TorcVertex v2,
      final String label) {
    this.graph = graph;
    this.v1 = v1;
    this.v2 = v2;
    this.label = label;
  }

  public TorcEdge(final TorcGraph graph, UInt128 v1Id, UInt128 v2Id,
      final String label, Map<Object, Object> properties) {
    this(graph, v1Id, v2Id, label);
    this.properties = properties;
  }

  public TorcEdge(final TorcGraph graph, UInt128 v1Id, UInt128 v2Id,
      final String label, byte[] serializedProperties) {
    this(graph, v1Id, v2Id, label);
    this.serializedProperties = serializedProperties;
  }

  public TorcEdge(final TorcGraph graph, UInt128 v1Id, UInt128 v2Id,
      final String label, Map<Object, Object> properties,
      byte[] serializedProperties) {
    this(graph, v1Id, v2Id, label);
    this.properties = properties;
    this.serializedProperties = serializedProperties;
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
    return v1.id();
  }

  /**
   * Returns the destination vertex ID in the case that this edge's type is
   * directed. If the edge type is undirected, then just returns one of the
   * vertex IDs (while {@link #getV1Id()} returns the other).
   *
   * @return Vertex ID.
   */
  public UInt128 getV2Id() {
    return v2.id();
  }

  /**
   * Returns the properties of this edge, if any.
   *
   * @return Edge properties.
   */
  public Map<Object, Object> getProperties() {
    if (properties != null) {
      return properties;
    } else {
      if (serializedProperties != null) {
        properties = (Map<Object, Object>)
          TorcHelper.deserializeObject(serializedProperties);
        return properties;
      } else {
        return null;
      }
    }
  }

  /**
   * Returns the serialized properties of this edge, if any.
   *
   * @return Edge serialized properties.
   */
  public byte[] getSerializedProperties() {
    if (serializedProperties != null) {
      return serializedProperties;
    } else {
      if (properties != null) {
        serializedProperties = TorcHelper.serializeObject(properties);
        return serializedProperties;
      } else {
        return null;
      }
    }
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
    return StringFactory.edgeString(this);
  }

  /**
   * This method defines equality to be equality in vertex ids, types, labels,
   * *and* graph. So, for example, if databases A and B have been loaded with
   * the exact same dataset, even though the structures are exactly the same,
   * the edges in A are not equal to those in B. They are considered distinct
   * edges.
   *
   * @param that Object representing the thing to be tested for equality.
   *
   * @return True if equal, false otherwise.
   */
  @Override
  public boolean equals(final Object that) {
    if (!(that instanceof TorcEdge)) {
      return false;
    }

    TorcEdge thatEdge = (TorcEdge) that;

    return this.graph.equals(thatEdge.graph)
        && this.getV1Id().equals(thatEdge.getV1Id())
        && this.getV2Id().equals(thatEdge.getV2Id())
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

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

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.EnumSet;
import java.util.Iterator;
import java.util.Arrays;
import java.util.Map;
import java.util.List;

/**
 *
 * @author Jonathan Ellithorpe (jde@cs.stanford.edu)
 */
public class TorcVertex implements Vertex, Element {

  private final TorcGraph graph;
  private UInt128 id;
  private String label;
  private Map<Object, Object> properties = null;

  public TorcVertex(final TorcGraph graph, final UInt128 id,
      final String label, Map<Object, Object> properties) {
    this.graph = graph;
    this.id = id;
    this.label = label;
    this.properties = properties;
  }

  public TorcVertex(final TorcGraph graph, final UInt128 id,
      final String label) {
    this(graph, id, label, null);
  }

  public TorcVertex(final TorcGraph graph, final UInt128 id) {
    this(graph, id, null, null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public UInt128 id() {
    return id;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String label() {
    if (label != null) {
      return label;
    } else {
      this.label = graph.getLabel(this);
      return label;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public TorcGraph graph() {
    return graph;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void remove() {
    graph.removeVertex(this);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
    return graph.addEdge(this, (TorcVertex) inVertex, label, keyValues);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
    throw new UnsupportedOperationException("Must specify the neighbor vertex labels when fetching vertex edges.");
  }

  public Iterator<Edge> edges(Direction direction, String[] edgeLabels, 
      String[] neighborLabels) {
    return graph.vertexEdges(this, direction, edgeLabels, neighborLabels);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
    throw new UnsupportedOperationException("Must specify the neighbor vertex labels when fetching vertex neighbors.");
  }

  public <V> V getProperty(String key) {
    if (properties == null) {
      graph.fillProperties(this);
    }

    return (V)properties.get(key);
  }

  public Map<Object, Object> getProperties() {
    if (properties == null) {
      graph.fillProperties(this);
    }

    return properties;
  }

  public void setProperties(Map<Object, Object> properties) {
    this.properties = properties;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
    return graph.getVertexProperties(this, propertyKeys);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <V> VertexProperty<V> property(String key, V value) {
    return property(VertexProperty.Cardinality.single, key, value);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality,
      String key, V value, Object... keyValues) {
    return graph.setVertexProperty(this, cardinality, key, value, keyValues);
  }

  /**
   * {@inheritDoc}
   *
   * Notes: - This implementation only checks that the IDs of the vertices are
   * the same, but not does check whether or not these vertices are existing in
   * the same graph. Two vertices existing in two different TorcGraphs may have
   * the same ID, and this method will still return true.
   */
  @Override
  public boolean equals(final Object object) {
    if (object instanceof TorcVertex) {
      return this.id().equals(((TorcVertex) object).id());
    }

    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int hashCode() {
    return ElementHelper.hashCode(this);
  }

  @Override
  public String toString() {
    return id.toString();
  }

}

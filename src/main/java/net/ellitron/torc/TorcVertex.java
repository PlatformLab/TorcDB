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

/**
 *
 * @author Jonathan Ellithorpe (jde@cs.stanford.edu)
 */
public class TorcVertex implements Vertex, Element {

  private final TorcGraph graph;
  private UInt128 id;
  private String label;

  public TorcVertex(final TorcGraph graph, final UInt128 id,
      final String label) {
    this.graph = graph;
    this.id = id;
    this.label = label;
  }

  public TorcVertex(final TorcGraph graph, final UInt128 id) {
    this(graph, id, null);
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
    throw new UnsupportedOperationException("Must specify the edge labels and neighbor vertex labels when fetching vertex neighbors.");
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

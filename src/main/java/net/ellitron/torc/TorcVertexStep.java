/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package net.ellitron.torc;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.Direction;                        
import org.apache.tinkerpop.gremlin.structure.Edge;                             
import org.apache.tinkerpop.gremlin.structure.Element;                          
import org.apache.tinkerpop.gremlin.structure.Vertex; 
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;

import java.util.Arrays;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

/**
 * Custom VertexStep that takes advantage of special methods in TorcGraph for
 * fetching neighbor vertices in bulk.
 *
 * @author Jonathan Ellithorpe (jde@cs.stanford.edu)
 */
public class TorcVertexStep<E extends Element> 
  extends AbstractStep<Vertex, E> {

  private Iterator<Traverser.Admin<E>> ends = null;

  private final Class<E> returnClass; 
  private Direction direction;                                                
  private final String[] edgeLabels;                                          
  private final String[] neighborLabels;

  public TorcVertexStep(final Traversal.Admin traversal,
      final Class<E> returnClass,
      final Direction direction,
      final String... edgeLabels) {
    super(traversal);
    this.direction = direction;                                             
    this.edgeLabels = edgeLabels;                                           
    this.returnClass = returnClass;
    this.neighborLabels = new String[0];
  }

  public TorcVertexStep(final VertexStep<E> originalVertexStep) {
    this(originalVertexStep.getTraversal(), 
        originalVertexStep.getReturnClass(), 
        originalVertexStep.getDirection(), 
        originalVertexStep.getEdgeLabels());
  }

  @Override
  protected Traverser.Admin<E> processNextStart() {
    if ((ends == null || !ends.hasNext()) && this.starts.hasNext()) {
      /* First fetch the complete set of starting vertices. */
      List<TorcVertex> startList = new ArrayList<>();
      Map<Vertex, Traverser.Admin<Vertex>> traverserMap = new HashMap<>();
      while(this.starts.hasNext()) {
        Traverser.Admin<Vertex> t = this.starts.next();
        traverserMap.put(t.get(), t);
        startList.add((TorcVertex)t.get());
      }

      if (Vertex.class.isAssignableFrom(this.returnClass)) {
        /* Bulk fetch neighbor vertices from TorcGraph. */
        Map<Vertex, Iterator<Vertex>> neighborMap = 
            ((TorcGraph)this.getTraversal().getGraph().get()).vertexNeighbors(
                startList,
                direction, 
                edgeLabels,
                neighborLabels);

        /* Use results to build a complete list of ending elements. */
        List<Traverser.Admin<E>> endList = new ArrayList<>();
        for (Map.Entry<Vertex, Iterator<Vertex>> entry : 
            neighborMap.entrySet()) {
          Vertex startVertex = entry.getKey();
          Iterator<Vertex> endVertices = entry.getValue();
         
          while (endVertices.hasNext()) {
            Traverser.Admin<E> endTraverser = (Traverser.Admin<E>) 
                traverserMap.get(startVertex).split((E)endVertices.next(), this);
            endList.add(endTraverser);
          }
        }

        ends = endList.iterator();
//        System.out.println(String.format("processNextStart(): ends was null, and have %d vertices to start with, which lead to %d neighbor vertices.", startList.size(), endList.size()));
      } else {
        /* Bulk fetch incident edges from TorcGraph. */
        Map<Vertex, Iterator<Edge>> edgeMap = 
            ((TorcGraph)this.getTraversal().getGraph().get()).vertexEdges(
                startList,
                direction, 
                edgeLabels);

        /* Use results to build a complete list of ending elements. */
        List<Traverser.Admin<E>> endList = new ArrayList<>();
        for (Map.Entry<Vertex, Iterator<Edge>> entry : 
            edgeMap.entrySet()) {
          Vertex startVertex = entry.getKey();
          Iterator<Edge> endEdges = entry.getValue();
         
          while (endEdges.hasNext()) {
            Traverser.Admin<E> endTraverser = (Traverser.Admin<E>) 
                traverserMap.get(startVertex).split((E)endEdges.next(), this);
            endList.add(endTraverser);
          }
        }

        ends = endList.iterator();
      }
    }

    if (ends != null && ends.hasNext()) {
      return ends.next();
    } else {
      throw FastNoSuchElementException.instance();
    }
  }
}

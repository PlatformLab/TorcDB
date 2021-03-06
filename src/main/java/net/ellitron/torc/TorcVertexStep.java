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
import java.util.Set;

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
  private final List<String> neighborLabels;

  public TorcVertexStep(final Traversal.Admin traversal,
      final Class<E> returnClass,
      final Direction direction,
      final String[] edgeLabels,
      final Set<String> labels) {
    super(traversal);
    this.direction = direction;                                             
    this.edgeLabels = edgeLabels;                                           
    this.returnClass = returnClass;
    this.neighborLabels = new ArrayList<>();

    for (String label : labels) {
      this.addLabel(label);
    }
  }

  public TorcVertexStep(final VertexStep<E> originalVertexStep) {
    this(originalVertexStep.getTraversal(), 
        originalVertexStep.getReturnClass(), 
        originalVertexStep.getDirection(), 
        originalVertexStep.getEdgeLabels(),
        originalVertexStep.getLabels());
  }

  public void addNeighborLabel(String label) {
    neighborLabels.add(label);
  }

  @Override
  protected Traverser.Admin<E> processNextStart() {
    if ((ends == null || !ends.hasNext()) && this.starts.hasNext()) {
      /* First fetch the complete set of starting vertices. */
      List<TorcVertex> startList = new ArrayList<>();
      Map<Vertex, List<Traverser.Admin<Vertex>>> traverserMap = new HashMap<>();
      while(this.starts.hasNext()) {
        Traverser.Admin<Vertex> t = this.starts.next();
        TorcVertex v = (TorcVertex)t.get();

        if (!traverserMap.containsKey(v)) {
          startList.add(v);
          List<Traverser.Admin<Vertex>> tList = new ArrayList<>();
          tList.add(t);
          traverserMap.put(v, tList);
        } else {
          List<Traverser.Admin<Vertex>> tList = traverserMap.get(v);
          tList.add(t);
        }
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
          Iterator<Vertex> endVerticesItr = entry.getValue();
          List<Vertex> endVertices = new ArrayList<>();
          endVerticesItr.forEachRemaining(e -> {endVertices.add(e);});
          
          List<Traverser.Admin<Vertex>> startTraverserList 
              = traverserMap.get(startVertex);

          for (Traverser.Admin<Vertex> startTraverser : startTraverserList) {
            for (int i = 0; i < endVertices.size(); i++) { 
              Vertex endVertex = endVertices.get(i);
              Traverser.Admin<E> endTraverser = 
                  (Traverser.Admin<E>)startTraverser.split((E) endVertex, this);
              endList.add(endTraverser);
            }
          }
        }

        ends = endList.iterator();
      } else {
        /* Bulk fetch incident edges from TorcGraph. */
        Map<Vertex, Iterator<Edge>> edgeMap = 
            ((TorcGraph)this.getTraversal().getGraph().get()).vertexEdges(
                startList,
                direction, 
                edgeLabels,
                neighborLabels);

        /* Use results to build a complete list of ending elements. */
        List<Traverser.Admin<E>> endList = new ArrayList<>();
        for (Map.Entry<Vertex, Iterator<Edge>> entry : 
            edgeMap.entrySet()) {
          Vertex startVertex = entry.getKey();
          Iterator<Edge> endEdgesItr = entry.getValue();
          List<Edge> endEdges = new ArrayList<>();
          endEdgesItr.forEachRemaining(e -> {endEdges.add(e);});

          List<Traverser.Admin<Vertex>> startTraverserList 
              = traverserMap.get(startVertex);

          for (Traverser.Admin<Vertex> startTraverser : startTraverserList) {
            for (int i = 0; i < endEdges.size(); i++) { 
              Edge endEdge = endEdges.get(i);
              Traverser.Admin<E> endTraverser = 
                  (Traverser.Admin<E>) startTraverser.split((E)endEdge, this);
              endList.add(endTraverser);
            }
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

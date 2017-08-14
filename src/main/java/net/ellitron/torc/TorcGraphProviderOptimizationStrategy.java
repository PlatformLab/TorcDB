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

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

/**
 * A TinkerPop provider optimization strategy for TorcGraph. Modifies a
 * traversal prior to its evaluation to take advantage of TorcGraph specific
 * functionality and performance optimizing features, such as bulk fetching of
 * edges and vertices.
 *
 * @author Jonathan Ellithorpe (jde@cs.stanford.edu)
 */
public final class TorcGraphProviderOptimizationStrategy extends 
  AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> 
  implements TraversalStrategy.ProviderOptimizationStrategy {

    private static final TorcGraphProviderOptimizationStrategy INSTANCE 
        = new TorcGraphProviderOptimizationStrategy();

    private TorcGraphProviderOptimizationStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {

      if (TraversalHelper.onGraphComputer(traversal))
        return;

      for (final GraphStep originalGraphStep : 
        TraversalHelper.getStepsOfClass(GraphStep.class, traversal)) {
      }

      for (final VertexStep originalVertexStep : 
        TraversalHelper.getStepsOfClass(VertexStep.class, traversal)) {
        final TorcVertexStep<?> torcVertexStep = new
            TorcVertexStep<>(originalVertexStep);
        TraversalHelper.replaceStep(originalVertexStep, torcVertexStep,
            traversal);
        Step<?, ?> currentStep = torcVertexStep.getNextStep();
        if (currentStep instanceof HasStep) {
          for (final HasContainer hasContainer : 
              ((HasContainerHolder) currentStep).getHasContainers()) {
            if (hasContainer.getKey().equals(T.label.getAccessor())) {
              String label = (String) hasContainer.getPredicate().getValue();
              torcVertexStep.addNeighborLabel(label);
            }
          }
        }
      }
    }

    public static TorcGraphProviderOptimizationStrategy instance() {
        return INSTANCE;
    }
}

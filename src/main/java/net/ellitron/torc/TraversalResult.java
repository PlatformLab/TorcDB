/*
 * Copyright 2019 Apache Software Foundation.
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

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Represents the result of a traversal (ie getVertices or getEdges).
 *
 * @author Jonathan Ellithorpe (jde@cs.stanford.edu)
 */
public class TraversalResult {
  public Map<TorcVertex, List<TorcVertex>> vMap;
  public Map<TorcVertex, List<Map<String, List<String>>>> pMap;
  public Set<TorcVertex> vSet;

  public TraversalResult(
      Map<TorcVertex, List<TorcVertex>> vMap, 
      Map<TorcVertex, List<Map<String, List<String>>>> pMap,
      Set<TorcVertex> vSet) {
    this.vMap = vMap;
    this.pMap = pMap;
    this.vSet = vSet;
  }
}


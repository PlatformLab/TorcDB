/* Copyright (c) 2019-2019 Stanford University
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


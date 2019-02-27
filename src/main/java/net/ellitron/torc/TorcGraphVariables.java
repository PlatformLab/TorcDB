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

import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.Optional;
import java.util.Set;

/**
 *
 * @author Jonathan Ellithorpe (jde@cs.stanford.edu)
 */
public class TorcGraphVariables implements Graph.Variables {

  @Override
  public Set<String> keys() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public <R> Optional<R> get(String key) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void set(String key, Object value) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void remove(String key) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

}

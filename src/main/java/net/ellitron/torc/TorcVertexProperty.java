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

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 *
 * @author Jonathan Ellithorpe (jde@cs.stanford.edu)
 * @param <V>
 */
public class TorcVertexProperty<V> implements VertexProperty<V>, Element {

  private final TorcVertex vertex;
  private final String key;
  private final V value;

  public TorcVertexProperty(final TorcVertex vertex, final String key,
      V value) {
    this.vertex = vertex;
    this.key = key;
    this.value = value;
  }

  @Override
  public Object id() {
    return vertex.id();
  }

  @Override
  public String label() {
    return vertex.label();
  }

  @Override
  public Graph graph() {
    return vertex.graph();
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Vertex element() {
    return vertex;
  }

  @Override
  public <U> Iterator<Property<U>> properties(String... propertyKeys) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public String key() {
    return key;
  }

  @Override
  public V value() throws NoSuchElementException {
    return value;
  }

  @Override
  public boolean isPresent() {
    return (value != null);
  }

  @Override
  public <V> Property<V> property(String key, V value) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

}

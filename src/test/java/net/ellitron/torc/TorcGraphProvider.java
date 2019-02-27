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

import edu.stanford.ramcloud.RAMCloud;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author Jonathan Ellithorpe (jde@cs.stanford.edu)
 */
public class TorcGraphProvider extends AbstractGraphProvider {

  private final String graphNamePrefix;
  private ConcurrentHashMap<Thread, RAMCloud> cachedThreadLocalClientMap;

  public TorcGraphProvider() {
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss");
    Date date = new Date();
    this.graphNamePrefix = dateFormat.format(date);
    cachedThreadLocalClientMap = new ConcurrentHashMap<>();
  }

  private static final Set<Class> IMPLEMENTATIONS = new HashSet<Class>() {
    {
      add(TorcEdge.class);
      add(TorcGraph.class);
      add(TorcGraphVariables.class);
      add(TorcProperty.class);
      add(TorcVertex.class);
      add(TorcVertexProperty.class);
    }
  };

  @Override
  public Map<String, Object> getBaseConfiguration(String graphName,
      Class<?> test, String testMethodName,
      LoadGraphWith.GraphData loadGraphWith) {
    Map<String, Object> config = new HashMap<>();
    config.put(Graph.GRAPH, TorcGraph.class.getName());
    config.put(TorcGraph.CONFIG_GRAPH_NAME, graphNamePrefix + "_"
        + test.getSimpleName() + "_" + testMethodName);

    config.put(TorcGraph.CONFIG_THREADLOCALCLIENTMAP,
        cachedThreadLocalClientMap);

    String ramcloudCoordinatorLocator =
        System.getenv("RAMCLOUD_COORDINATOR_LOCATOR");
    if (ramcloudCoordinatorLocator == null) {
      throw new RuntimeException("RAMCLOUD_COORDINATOR_LOCATOR environment "
          + "variable not set. Please set this to your RAMCloud cluster's "
          + "coordinator locator string (e.g. "
          + "infrc:host=192.168.1.1,port=12246).");
    }

    String ramcloudServers = System.getenv("RAMCLOUD_SERVERS");
    if (ramcloudServers == null) {
      throw new RuntimeException("RAMCLOUD_SERVERS environment variable not "
          + "set. Please set this to the number of master servers in your "
          + "RAMCloud cluster.");
    }

    config.put(TorcGraph.CONFIG_COORD_LOCATOR,
        ramcloudCoordinatorLocator.replace(",", "\\,"));
    config.put(TorcGraph.CONFIG_NUM_MASTER_SERVERS, ramcloudServers);
    return config;
  }

  @Override
  public void clear(Graph graph, Configuration configuration)
      throws Exception {
    if (graph != null) {
      TorcGraph g = (TorcGraph) graph;
      g.rollbackAllThreads();
    }
  }

  @Override
  public Set<Class> getImplementations() {
    return IMPLEMENTATIONS;
  }

  @Override
  public Object convertId(final Object id, final Class<? extends Element> c) {
    return UInt128.decode(id);
  }
}

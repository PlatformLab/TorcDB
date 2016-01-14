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
package org.ellitron.tinkerpop.gremlin.torc;

import edu.stanford.ramcloud.RAMCloud;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.configuration.Configuration;

import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.ellitron.tinkerpop.gremlin.torc.structure.TorcEdge;
import org.ellitron.tinkerpop.gremlin.torc.structure.TorcGraph;
import org.ellitron.tinkerpop.gremlin.torc.structure.TorcGraphVariables;
import org.ellitron.tinkerpop.gremlin.torc.structure.TorcProperty;
import org.ellitron.tinkerpop.gremlin.torc.structure.TorcVertex;
import org.ellitron.tinkerpop.gremlin.torc.structure.TorcVertexProperty;
import org.ellitron.tinkerpop.gremlin.torc.structure.util.UInt128;

/**
 *
 * @author ellitron
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
    
    private static final Set<Class> IMPLEMENTATIONS = new HashSet<Class>() {{
        add(TorcEdge.class);
        add(TorcGraph.class);
        add(TorcGraphVariables.class);
        add(TorcProperty.class);
        add(TorcVertex.class);
        add(TorcVertexProperty.class);
    }};
    
    /**
     * TODO: Use environment variables for the source of configuration
     * information.
     */
    @Override
    public Map<String, Object> getBaseConfiguration(String graphName, Class<?> test, String testMethodName, LoadGraphWith.GraphData loadGraphWith) {
        Map<String, Object> config = new HashMap<>();
        config.put(Graph.GRAPH, TorcGraph.class.getName());
        config.put(TorcGraph.CONFIG_GRAPH_NAME, graphNamePrefix + "_" + test.getSimpleName() + "_" + testMethodName);
        
        config.put(TorcGraph.CONFIG_THREADLOCALCLIENTMAP, cachedThreadLocalClientMap);
        
        String ramcloudCoordinatorLocator = System.getenv("RAMCLOUD_COORDINATOR_LOCATOR");
        if (ramcloudCoordinatorLocator == null) 
            throw new RuntimeException("RAMCLOUD_COORDINATOR_LOCATOR environment variable not set. Please set this to your RAMCloud cluster's coordinator locator string (e.g. infrc:host=192.168.1.1,port=12246).");
        
        String ramcloudServers = System.getenv("RAMCLOUD_SERVERS");
        if (ramcloudServers == null)
            throw new RuntimeException("RAMCLOUD_SERVERS environment variable not set. Please set this to the number of master servers in your RAMCloud cluster.");
        
        config.put(TorcGraph.CONFIG_COORD_LOCATOR, ramcloudCoordinatorLocator.replace(",", "\\,"));
        config.put(TorcGraph.CONFIG_NUM_MASTER_SERVERS, ramcloudServers);
        return config;
    }

    @Override
    public void clear(Graph graph, Configuration configuration) throws Exception {
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

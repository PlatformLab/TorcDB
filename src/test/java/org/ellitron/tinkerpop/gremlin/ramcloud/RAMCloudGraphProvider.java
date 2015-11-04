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
package org.ellitron.tinkerpop.gremlin.ramcloud;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.configuration.Configuration;

import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.ellitron.tinkerpop.gremlin.ramcloud.structure.RAMCloudEdge;
import org.ellitron.tinkerpop.gremlin.ramcloud.structure.RAMCloudElement;
import org.ellitron.tinkerpop.gremlin.ramcloud.structure.RAMCloudGraph;
import org.ellitron.tinkerpop.gremlin.ramcloud.structure.RAMCloudGraphVariables;
import org.ellitron.tinkerpop.gremlin.ramcloud.structure.RAMCloudProperty;
import org.ellitron.tinkerpop.gremlin.ramcloud.structure.RAMCloudVertex;
import org.ellitron.tinkerpop.gremlin.ramcloud.structure.RAMCloudVertexProperty;

/**
 *
 * @author ellitron
 */
public class RAMCloudGraphProvider extends AbstractGraphProvider {

    private static final Set<Class> IMPLEMENTATIONS = new HashSet<Class>() {{
        add(RAMCloudEdge.class);
        add(RAMCloudElement.class);
        add(RAMCloudGraph.class);
        add(RAMCloudGraphVariables.class);
        add(RAMCloudProperty.class);
        add(RAMCloudVertex.class);
        add(RAMCloudVertexProperty.class);
    }};
    
    @Override
    public Map<String, Object> getBaseConfiguration(String graphName, Class<?> test, String testMethodName, LoadGraphWith.GraphData loadGraphWith) {
        Map<String, Object> config = new HashMap<>();
        config.put(Graph.GRAPH, RAMCloudGraph.class.getName());
        config.put(RAMCloudGraph.CONFIG_GRAPH_NAME, graphName);
        config.put(RAMCloudGraph.CONFIG_COORD_LOC, "infrc:host=192.168.1.129\\,port=12246");
        config.put(RAMCloudGraph.CONFIG_NUM_MASTER_SERVERS, 1);
        return config;
    }

    @Override
    public void clear(Graph graph, Configuration configuration) throws Exception {
        if (graph != null)
            if (((RAMCloudGraph) graph).isInitialized())
                ((RAMCloudGraph)graph).deleteDatabaseAndCloseConnection();
    }

    @Override
    public Set<Class> getImplementations() {
        return IMPLEMENTATIONS;
    }

    @Override
    public Object convertId(final Object id, final Class<? extends Element> c) {
        if (id instanceof String) {
            return Long.decode((String) id);
        }

        return id;
    }
}

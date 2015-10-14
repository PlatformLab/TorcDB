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
package org.ellitron.tinkerpop.gremlin.ramcloud.structure;

import java.util.Iterator;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import edu.stanford.ramcloud.*;
import static edu.stanford.ramcloud.ClientException.*;
import edu.stanford.ramcloud.multiop.*;
import edu.stanford.ramcloud.transactions.*;

/**
 *
 * @author ellitron
 */
public final class RAMCloudGraph implements Graph {

    static final String CONFIG_COORD_LOC = "gremlin.ramcloud.coordinatorLocator";
    
    private final Configuration configuration;
    private final String coordinatorLocator;
    private final RAMCloud ramcloud;
    
//    static {
//        // Load C++ shared library for JNI
//        Util.loadLibrary("ramcloud_java");
//    }
    
    private RAMCloudGraph(final Configuration configuration) {
        this.configuration = configuration;
        
        this.coordinatorLocator = configuration.getString(CONFIG_COORD_LOC);
        
        try {
            ramcloud = new RAMCloud(coordinatorLocator);
        } catch(ClientException e) {
            System.out.println(e.toString());
            throw e;
        }
    }
    
    public static RAMCloudGraph open(final Configuration configuration) {
        return new RAMCloudGraph(configuration);
    }
    
    @Override
    public RAMCloudVertex addVertex(Object... os) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public <C extends GraphComputer> C compute(Class<C> type) throws IllegalArgumentException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Iterator<Vertex> vertices(Object... os) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Iterator<Edge> edges(Object... os) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Transaction tx() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Variables variables() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Configuration configuration() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void close() throws Exception {
        ramcloud.disconnect();
    }
    
}

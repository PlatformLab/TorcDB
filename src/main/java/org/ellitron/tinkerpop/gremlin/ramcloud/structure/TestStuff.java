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

import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.T;

import edu.stanford.ramcloud.*;
import edu.stanford.ramcloud.multiop.*;
import edu.stanford.ramcloud.transactions.*;
import static edu.stanford.ramcloud.ClientException.*;

/**
 *
 * @author ellitron
 */
public class TestStuff {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        RAMCloud ramcloud = new RAMCloud("infrc:host=192.168.1.109,port=12246");
        
        long tableId = ramcloud.createTable("test");
        
        long retVal;
        String key = "bob";
        
        System.out.println(ramcloud.incrementInt64(tableId, key.getBytes(), 1, null));
        System.out.println(ramcloud.incrementInt64(tableId, key.getBytes(), 1, null));
        System.out.println(ramcloud.incrementInt64(tableId, key.getBytes(), 1, null));
        
        System.out.println(ramcloud.incrementInt64(tableId, key.getBytes(), 2, null));
        System.out.println(ramcloud.incrementInt64(tableId, key.getBytes(), 2, null));
        System.out.println(ramcloud.incrementInt64(tableId, key.getBytes(), 2, null));
        
//        Configuration configuration = new BaseConfiguration();
//        configuration.setProperty(RAMCloudGraph.CONFIG_COORD_LOC, args[0]);
//        
//        RAMCloudGraph graphdb = RAMCloudGraph.open(configuration);
//        
//        RAMCloudVertex person1 = graphdb.addVertex(T.label, "person");
//        
//        try {
//            graphdb.close();
//        } catch (Exception ex) {
//            Logger.getLogger(TestStuff.class.getName()).log(Level.SEVERE, null, ex);
//        }
    }
}

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
package org.ellitron.tinkerpop.gremlin.ramcloud.structure.util;

import edu.stanford.ramcloud.RAMCloudObject;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import org.apache.tinkerpop.gremlin.structure.Direction;

/**
 *
 * @author ellitron
 */
public class RAMCloudHelper {
    public static ByteBuffer serializeProperties(Map<String, String> propertyMap) {
        int serializedLength = 0;
        for (Map.Entry<String, String> property : propertyMap.entrySet()) {
            serializedLength += Short.BYTES + property.getKey().length();
            serializedLength += Short.BYTES + property.getValue().length();
        }

        ByteBuffer buffer = ByteBuffer.allocate(serializedLength);
        for (Map.Entry<String, String> property : propertyMap.entrySet()) {
            buffer.putShort((short) property.getKey().length());
            buffer.put(property.getKey().getBytes());
            buffer.putShort((short) property.getValue().length());
            buffer.put(property.getValue().getBytes());
        }
        
        return buffer;
    }
    
    public static Map<String, String> deserializeProperties(ByteBuffer buffer) {
        Map<String, String> propertyMap = new HashMap<>();
        while (buffer.hasRemaining()) {
            short len = buffer.getShort();
            byte key[] = new byte[len];
            buffer.get(key);
            len = buffer.getShort();
            byte value[] = new byte[len];
            buffer.get(value);

            propertyMap.put(new String(key), new String(value));
        }
        
        return propertyMap;
    }
    
    public static Map<String, String> deserializeProperties(RAMCloudObject obj) {
        ByteBuffer value = ByteBuffer.allocate(obj.getValueBytes().length);
        value.put(obj.getValueBytes());
        value.rewind();
        return deserializeProperties(value);
    }
    
    public static byte[] makeVertexId(long clientId, long localVertexId) {
        ByteBuffer id = ByteBuffer.allocate(Long.BYTES*2);
        id.putLong(clientId);
        id.putLong(localVertexId);
        return id.array();
    }
    
    public static boolean validateVertexId(Object vertexId) {
        if (!(vertexId instanceof byte[]))
            return false;
        
        if (((byte[]) vertexId).length != Long.BYTES*2)
            return false;
        
        return true;
    }
    
    public static String stringifyVertexId(byte[] vertexId) {
        ByteBuffer id = ByteBuffer.allocate(vertexId.length);
        id.put(vertexId);
        id.rewind();
        long creatorId = id.getLong();
        long creatorAssignedId = id.getLong();
        return String.format("%X-%X", creatorId, creatorAssignedId);
    }
    
    public static String getVertexLabelKey(byte[] vertexId) {
        return stringifyVertexId(vertexId) + ":label";
    }
    
    public static String getVertexPropertiesKey(byte[] vertexId) {
        return stringifyVertexId(vertexId) + ":props";
    }
    
    public static String getVertexEdgeListKey(byte[] vertexId, String label, Direction direction) {
        return stringifyVertexId(vertexId) + ":edges:" + label + ":" + direction.name();
    }
    
    public static String getVertexLabelListKey(byte[] vertexId) {
        return stringifyVertexId(vertexId) + ":edgeLabels";
    }
}

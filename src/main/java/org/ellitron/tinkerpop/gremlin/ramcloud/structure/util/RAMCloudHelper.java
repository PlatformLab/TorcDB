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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import static org.apache.tinkerpop.gremlin.structure.util.ElementHelper.haveEqualIds;
import org.ellitron.tinkerpop.gremlin.ramcloud.structure.RAMCloudEdge;
import org.ellitron.tinkerpop.gremlin.ramcloud.structure.RAMCloudVertex;
import org.ellitron.tinkerpop.gremlin.ramcloud.structure.RAMCloudVertexProperty;

/**
 *
 * @author ellitron
 */
public class RAMCloudHelper {
    public static ByteBuffer serializeProperties(Map<String, String> propertyMap) {
        int serializedLength = 0;
        for (Map.Entry<String, String> property : propertyMap.entrySet()) {
            serializedLength += Short.BYTES + property.getKey().getBytes().length;
            serializedLength += Short.BYTES + property.getValue().getBytes().length;
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
    
    public static ByteBuffer serializeEdgeLabelList(List<String> edgeLabelList) {
        int serializedLength = 0;
        for (String label : edgeLabelList) {
            serializedLength += Short.BYTES + label.length();
        }

        ByteBuffer buffer = ByteBuffer.allocate(serializedLength);
        for (String label : edgeLabelList) {
            buffer.putShort((short) label.length());
            buffer.put(label.getBytes());
        }
        
        return buffer;
    }
    
    public static List<String> deserializeEdgeLabelList(ByteBuffer buffer) {
        List<String> edgeLabelList = new ArrayList<>();
        while (buffer.hasRemaining()) {
            short len = buffer.getShort();
            byte label[] = new byte[len];
            buffer.get(label);
            
            edgeLabelList.add(new String(label));
        }
        
        return edgeLabelList;
    }
    
    public static List<String> deserializeEdgeLabelList(RAMCloudObject obj) {
        ByteBuffer value = ByteBuffer.allocate(obj.getValueBytes().length);
        value.put(obj.getValueBytes());
        value.rewind();
        return deserializeEdgeLabelList(value);
    }
    
    public static List<byte[]> parseNeighborIdsFromEdgeList(RAMCloudObject obj) {
        ByteBuffer value = ByteBuffer.allocate(obj.getValueBytes().length);
        value.put(obj.getValueBytes());
        value.rewind();
        
        List<byte[]> neighborIds = new ArrayList<>();
        while (value.hasRemaining()) {
            byte[] vertexId = new byte[Long.BYTES*2];
            value.get(vertexId);
            short propsTotalLen = value.getShort();
            value.position(value.position() + propsTotalLen);
            
            neighborIds.add(vertexId);
        }
        
        return neighborIds;
    }
    
    public static byte[] parseVertexIdFromKey(String key) {
        String[] parts = key.split(":");
        String[] subParts = parts[0].split("-");
        Long creatorId = Long.parseLong(subParts[0], 16);
        Long localVertexId = Long.parseLong(subParts[1], 16);
        ByteBuffer vertexId = ByteBuffer.allocate(Long.BYTES*2);
        vertexId.putLong(creatorId);
        vertexId.putLong(localVertexId);
        return vertexId.array();
    }
    
    public static String parseEdgeLabelFromKey(String key) {
        String[] parts = key.split(":");
        return parts[2];
    }
    
    public static Direction parseEdgeDirectionFromKey(String key) {
        String[] parts = key.split(":");
        return Direction.valueOf(parts[3]);
    }
    
    public static byte[] makeVertexId(long counterId, long counterValue) {
        ByteBuffer id = ByteBuffer.allocate(Long.BYTES*2);
        id.putLong(counterId);
        id.putLong(counterValue);
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
    
    public static String getVertexEdgeLabelListKey(byte[] vertexId) {
        return stringifyVertexId(vertexId) + ":edgeLabels";
    }
    
    public static boolean isVertexLabelKey(String key) {
        String[] parts = key.split(":");
        if (parts.length > 1) {
            if (parts[1].equals("label"))
                return true;
            
            return false;
        }
        
        return false;
    }
    
    public static boolean isVertexPropertiesKey(String key) {
        String[] parts = key.split(":");
        if (parts.length > 1) {
            if (parts[1].equals("props"))
                return true;
            
            return false;
        }
        
        return false;
    }
    
    public static boolean isVertexEdgeListKey(String key) {
        String[] parts = key.split(":");
        if (parts.length > 1) {
            if (parts[1].equals("edges"))
                return true;
            
            return false;
        }
        
        return false;
    }
    
    public static byte[] makeEdgeId(byte[] outVertexId, byte[] inVertexId, String label) {
        ByteBuffer id = ByteBuffer.allocate(Long.BYTES*4 + label.length());
        id.put(outVertexId);
        id.put(inVertexId);
        id.put(label.getBytes());
        return id.array();
    }
    
    public static boolean validateEdgeId(Object edgeId) {
        if (!(edgeId instanceof byte[]))
            return false;
        
        if (!(((byte[]) edgeId).length > Long.BYTES*4))
            return false;
        
        return true;
    }
    
    public static String parseLabelFromEdgeId(byte[] edgeId) {
        byte[] label = Arrays.copyOfRange(edgeId, Long.BYTES*4, edgeId.length);
        return new String(label);
    }
    
    public static byte[] parseOutVertexIdFromEdgeId(byte[] edgeId) {
        return Arrays.copyOfRange(edgeId, 0, Long.BYTES*2);
    }
    
    public static byte[] parseInVertexIdFromEdgeId(byte[] edgeId) {
        return Arrays.copyOfRange(edgeId, Long.BYTES*2, Long.BYTES*4);
    }
}

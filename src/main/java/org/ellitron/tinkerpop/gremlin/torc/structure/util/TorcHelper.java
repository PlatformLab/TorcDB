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
package org.ellitron.tinkerpop.gremlin.torc.structure.util;

import edu.stanford.ramcloud.RAMCloudObject;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.ellitron.tinkerpop.gremlin.torc.structure.TorcEdgeDirection;
import org.ellitron.tinkerpop.gremlin.torc.structure.UInt128;

/**
 *
 * @author Jonathan Ellithorpe <jde@cs.stanford.edu>
 */
public class TorcHelper {
    public static ByteBuffer serializeProperties(Map<String, String> propertyMap) {
        int serializedLength = 0;
        for (Map.Entry<String, String> property : propertyMap.entrySet()) {
            serializedLength += Short.BYTES + property.getKey().getBytes().length;
            serializedLength += Short.BYTES + property.getValue().getBytes().length;
        }
        
        ByteBuffer buffer = ByteBuffer.allocate(serializedLength);
        for (Map.Entry<String, String> property : propertyMap.entrySet()) {
            buffer.putShort((short) property.getKey().getBytes().length);
            buffer.put(property.getKey().getBytes());
            buffer.putShort((short) property.getValue().getBytes().length);
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
    
    public static List<UInt128> parseNeighborIdsFromEdgeList(RAMCloudObject obj) {
        ByteBuffer value = ByteBuffer.allocate(obj.getValueBytes().length);
        value.put(obj.getValueBytes());
        value.flip();
        
        List<UInt128> neighborIds = new ArrayList<>();
        while (value.hasRemaining()) {
            byte[] vertexId = new byte[UInt128.BYTES];
            value.get(vertexId);
            short propsTotalLen = value.getShort();
            value.position(value.position() + propsTotalLen);
            
            neighborIds.add(new UInt128(vertexId));
        }
        
        return neighborIds;
    }
    
    public static enum VertexKeyType {
        LABEL,
        PROPERTIES,
        EDGE_LIST,
        EDGE_LABELS,
    }
    
    public static byte[] getVertexLabelKey(UInt128 vertexId) {
        ByteBuffer buffer = ByteBuffer.allocate(UInt128.BYTES + Byte.BYTES);
        buffer.putLong(vertexId.getUpperLong());
        buffer.putLong(vertexId.getLowerLong());
        buffer.put((byte) VertexKeyType.LABEL.ordinal());
        return buffer.array();
    }
    
    public static byte[] getVertexPropertiesKey(UInt128 vertexId) {
        ByteBuffer buffer = ByteBuffer.allocate(UInt128.BYTES + Byte.BYTES);
        buffer.putLong(vertexId.getUpperLong());
        buffer.putLong(vertexId.getLowerLong());
        buffer.put((byte) VertexKeyType.PROPERTIES.ordinal());
        return buffer.array();
    }
    
    public static byte[] getVertexEdgeListKey(UInt128 vertexId, String label, TorcEdgeDirection dir) {
        ByteBuffer buffer = ByteBuffer.allocate(UInt128.BYTES + Byte.BYTES*2 + label.getBytes().length);
        buffer.putLong(vertexId.getUpperLong());
        buffer.putLong(vertexId.getLowerLong());
        buffer.put((byte) VertexKeyType.EDGE_LIST.ordinal());
        buffer.put((byte) dir.ordinal());
        buffer.put(label.getBytes());
        return buffer.array();
    }
    
    public static byte[] getVertexEdgeLabelListKey(UInt128 vertexId) {
        ByteBuffer buffer = ByteBuffer.allocate(UInt128.BYTES + Byte.BYTES);
        buffer.putLong(vertexId.getUpperLong());
        buffer.putLong(vertexId.getLowerLong());
        buffer.put((byte) VertexKeyType.EDGE_LABELS.ordinal());
        return buffer.array();
    }
    
    public static VertexKeyType getVertexKeyType(byte[] key) {
        return VertexKeyType.values()[key[UInt128.BYTES]];
    }
}

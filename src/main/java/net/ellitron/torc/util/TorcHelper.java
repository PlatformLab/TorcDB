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
package net.ellitron.torc.util;

import net.ellitron.torc.TorcEdgeDirection;

import edu.stanford.ramcloud.RAMCloudObject;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author Jonathan Ellithorpe (jde@cs.stanford.edu)
 */
public class TorcHelper {

  public static Charset DEFAULT_CHAR_ENCODING = Charset.forName("UTF-8");

  public static void legalPropertyKeyValueArray(
      final Class<? extends Element> clazz,
      final Object... propertyKeyValues) throws IllegalArgumentException {
    if (propertyKeyValues.length % 2 != 0) {
      throw Element.Exceptions.providedKeyValuesMustBeAMultipleOfTwo();
    }
    for (int i = 0; i < propertyKeyValues.length; i = i + 2) {
      if (propertyKeyValues[i] == null) {
        throw Property.Exceptions.propertyKeyCanNotBeNull();
      }
      if (!(propertyKeyValues[i] instanceof String)
          && !(propertyKeyValues[i] instanceof T)) {
        throw Element.Exceptions
            .providedKeyValuesMustHaveALegalKeyOnEvenIndices();
      }
      if (propertyKeyValues[i] instanceof T) {
        if ((propertyKeyValues[i].equals(T.label))) {
          if (propertyKeyValues[i + 1] == null) {
            throw Element.Exceptions.labelCanNotBeNull();
          } else if (((String) propertyKeyValues[i + 1]).length() == 0) {
            throw Element.Exceptions.labelCanNotBeEmpty();
          }
        }
        if ((propertyKeyValues[i].equals(T.id))) {
          if (clazz.isAssignableFrom(Edge.class)) {
            throw Edge.Exceptions.userSuppliedIdsNotSupported();
          }
        }
      } else {
        if (((String) propertyKeyValues[i]).length() == 0) {
          throw Property.Exceptions.propertyKeyCanNotBeEmpty();
        }
        if (propertyKeyValues[i + 1] == null) {
          throw Property.Exceptions.propertyValueCanNotBeNull();
        }
        if (!(propertyKeyValues[i + 1] instanceof String)) {
          throw Property.Exceptions
              .dataTypeOfPropertyValueNotSupported(propertyKeyValues[i + 1]);
        }
      }
    }
  }

  public static byte[] serializeString(String str) {
    return str.getBytes(DEFAULT_CHAR_ENCODING);
  }

  public static String deserializeString(byte[] b) {
    return new String(b, DEFAULT_CHAR_ENCODING);
  }

  public static ByteBuffer serializeProperties(
      Map<String, List<String>> propertyMap) {
    int serializedLength = 0;
    for (Map.Entry<String, List<String>> property : propertyMap.entrySet()) {
      serializedLength += Integer.BYTES;
      String key = property.getKey();
      serializedLength += Short.BYTES + serializeString(key).length;
      for (String value : property.getValue()) {
        serializedLength += Short.BYTES + serializeString(value).length;
      }
    }

    ByteBuffer buffer = ByteBuffer.allocate(serializedLength);
    for (Map.Entry<String, List<String>> property : propertyMap.entrySet()) {
      int propLenPos = buffer.position();
      buffer.putInt(0); // will fill this in when we've got the total len
      String key = property.getKey();
      byte[] keyByteArray = serializeString(key);
      buffer.putShort((short) keyByteArray.length);
      buffer.put(keyByteArray);
      for (String value : property.getValue()) {
        byte[] valueByteArray = serializeString(value);
        buffer.putShort((short) valueByteArray.length);
        buffer.put(valueByteArray);
      }
      int propLen = buffer.position() - propLenPos - Integer.BYTES;
      buffer.putInt(propLenPos, propLen);
    }

    return buffer;
  }

  public static Map<String, List<String>> deserializeProperties(
      ByteBuffer buffer) {
    Map<String, List<String>> propertyMap = new HashMap<>();
    while (buffer.hasRemaining()) {
      int propLen = buffer.getInt();
      if (propLen > 0) {
        int propStartPos = buffer.position();
        short keyLen = buffer.getShort();
        byte key[] = new byte[keyLen];
        buffer.get(key);
        List<String> values = new ArrayList<>();
        while (buffer.position() - propStartPos < propLen) {
          short valLen = buffer.getShort();
          byte value[] = new byte[valLen];
          buffer.get(value);
          values.add(deserializeString(value));
        }

        propertyMap.put(deserializeString(key), values);
      }
    }

    return propertyMap;
  }

  public static Map<String, List<String>> deserializeProperties(
      RAMCloudObject obj) {
    ByteBuffer value = ByteBuffer.allocate(obj.getValueBytes().length);
    value.put(obj.getValueBytes());
    value.rewind();
    return deserializeProperties(value);
  }

  public static Map<String, List<String>> deserializeProperties(byte[] buf) {
    ByteBuffer value = ByteBuffer.allocate(buf.length);
    value.put(buf);
    value.rewind();
    return deserializeProperties(value);
  }

  public static ByteBuffer serializeEdgeLabelList(List<String> edgeLabelList) {
    int serializedLength = 0;
    for (String label : edgeLabelList) {
      serializedLength += Short.BYTES + serializeString(label).length;
    }

    ByteBuffer buffer = ByteBuffer.allocate(serializedLength);
    for (String label : edgeLabelList) {
      byte[] labelByteArray = serializeString(label);
      buffer.putShort((short) labelByteArray.length);
      buffer.put(serializeString(label));
    }

    return buffer;
  }

  public static List<String> deserializeEdgeLabelList(ByteBuffer buffer) {
    List<String> edgeLabelList = new ArrayList<>();
    while (buffer.hasRemaining()) {
      short len = buffer.getShort();
      byte label[] = new byte[len];
      buffer.get(label);

      edgeLabelList.add(deserializeString(label));
    }

    return edgeLabelList;
  }

  public static List<String> deserializeEdgeLabelList(RAMCloudObject obj) {
    ByteBuffer value = ByteBuffer.allocate(obj.getValueBytes().length);
    value.put(obj.getValueBytes());
    value.rewind();
    return deserializeEdgeLabelList(value);
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

  public static VertexKeyType getVertexKeyType(byte[] key) {
    return VertexKeyType.values()[key[UInt128.BYTES]];
  }

  /**
   * Generates a key prefix that defines an exclusive key-space for this
   * combination of vertex ID, edge label, and edge direction. No other (vertex
   * ID, label, direction) combination will have a key prefix that is a prefix
   * of this key, or for which this key is a prefix of.
   *
   * @param vertexId
   * @param label
   * @param dir
   *
   * @return
   */
  public static byte[] getEdgeListKeyPrefix(UInt128 vertexId, String label,
      TorcEdgeDirection dir) {
    byte[] labelByteArray = serializeString(label);
    ByteBuffer buffer =
        ByteBuffer.allocate(UInt128.BYTES + Short.BYTES + labelByteArray.length
            + Byte.BYTES);
    buffer.putLong(vertexId.getUpperLong());
    buffer.putLong(vertexId.getLowerLong());
    buffer.putShort((short) labelByteArray.length);
    buffer.put(labelByteArray);
    buffer.put((byte) dir.ordinal());
    return buffer.array();
  }

  public static byte[] getEdgeLabelListKey(UInt128 vertexId) {
    return vertexId.toByteArray();
  }
}

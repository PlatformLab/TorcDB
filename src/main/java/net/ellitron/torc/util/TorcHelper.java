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

import net.ellitron.torc.*;

import edu.stanford.ramcloud.RAMCloudObject;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

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

    ByteBuffer buffer = ByteBuffer.allocate(serializedLength)
        .order(ByteOrder.LITTLE_ENDIAN);
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
    ByteBuffer value = ByteBuffer.allocate(obj.getValueBytes().length)
        .order(ByteOrder.LITTLE_ENDIAN);
    value.put(obj.getValueBytes());
    value.rewind();
    return deserializeProperties(value);
  }

  public static Map<String, List<String>> deserializeProperties(byte[] buf) {
    ByteBuffer value = ByteBuffer.allocate(buf.length)
        .order(ByteOrder.LITTLE_ENDIAN);
    value.put(buf);
    value.rewind();
    return deserializeProperties(value);
  }

  public static ByteBuffer serializeStringList(List<String> list) {
    int serializedLength = 0;
    for (String s : list) {
      serializedLength += Short.BYTES + serializeString(s).length;
    }

    ByteBuffer buffer = ByteBuffer.allocate(serializedLength)
        .order(ByteOrder.LITTLE_ENDIAN);
    for (String s : list) {
      byte[] byteArray = serializeString(s);
      buffer.putShort((short) byteArray.length);
      buffer.put(byteArray);
    }

    return buffer;
  }

  public static List<String> deserializeStringList(ByteBuffer buffer) {
    List<String> list = new ArrayList<>();
    while (buffer.hasRemaining()) {
      short len = buffer.getShort();
      byte byteArray[] = new byte[len];
      buffer.get(byteArray);

      list.add(deserializeString(byteArray));
    }

    return list;
  }

  public static List<String> deserializeStringList(RAMCloudObject obj) {
    ByteBuffer value = ByteBuffer.allocate(obj.getValueBytes().length)
        .order(ByteOrder.LITTLE_ENDIAN);
    value.put(obj.getValueBytes());
    value.rewind();
    return deserializeStringList(value);
  }

  public static enum VertexKeyType {

    LABEL,
    PROPERTIES,
    EDGE_LIST,
    EDGE_LABELS,
  }

  public static byte[] getVertexLabelKey(UInt128 vertexId) {
    ByteBuffer buffer = ByteBuffer.allocate(UInt128.BYTES + Byte.BYTES)
        .order(ByteOrder.LITTLE_ENDIAN);
    buffer.putLong(vertexId.getUpperLong());
    buffer.putLong(vertexId.getLowerLong());
    buffer.put((byte) VertexKeyType.LABEL.ordinal());
    return buffer.array();
  }

  public static byte[] getVertexPropertiesKey(UInt128 vertexId) {
    ByteBuffer buffer = ByteBuffer.allocate(UInt128.BYTES + Byte.BYTES)
        .order(ByteOrder.LITTLE_ENDIAN);
    buffer.putLong(vertexId.getUpperLong());
    buffer.putLong(vertexId.getLowerLong());
    buffer.put((byte) VertexKeyType.PROPERTIES.ordinal());
    return buffer.array();
  }

  public static VertexKeyType getVertexKeyType(byte[] key) {
    return VertexKeyType.values()[key[UInt128.BYTES]];
  }


  /**
   * Generates the key for the RAMCloud object that stores the list of edge
   * labels of all incident edges to the given vertex. This is useful
   * because edges are stored by edge label, direction, and neighbor vertex
   * label. Therefore, to get all the edges incident to a vertex, one must know
   * the incident edge labels (as well as the neighbor labels for each of those
   * edge labels). In normal queries, however, the edge label is specified and
   * there's no need to read this list.
   *
   * @param vertexId
   *
   * @return RAMCloud Key.
   */
  public static byte[] getIncidentEdgeLabelListKey(UInt128 vertexId) {
    return vertexId.toByteArray();
  }

  /**
   * Generates the key for the RAMCloud object that stores the list of vertex
   * labels of vertices that lie on the other side of edges with label
   * edgeLabel for vertex with ID vertexId and in direction dir. This is useful
   * because edges are stored by edge label, direction, and neighbor vertex
   * label. Therefore, to get all the edges given an edge label, one must know
   * all the vertex labels that lie on the other side of those edges.
   *
   * @param vertexId
   * @param edgeLabel
   * @param dir
   *
   * @return RAMCloud Key.
   */
  public static byte[] getNeighborLabelListKey(UInt128 vertexId, 
      String edgeLabel, Direction dir) {
    byte[] labelByteArray = serializeString(edgeLabel);
    ByteBuffer buffer =
        ByteBuffer.allocate(UInt128.BYTES + Short.BYTES + labelByteArray.length
            + Byte.BYTES)
        .order(ByteOrder.LITTLE_ENDIAN);
    buffer.putLong(vertexId.getUpperLong());
    buffer.putLong(vertexId.getLowerLong());
    buffer.putShort((short) labelByteArray.length);
    buffer.put(labelByteArray);
    buffer.put((byte) dir.ordinal());
    return buffer.array();
  }

  /**
   * Generates a key prefix that defines an exclusive key-space for this
   * combination of vertex ID, edge label, edge direction, and vertex label. No
   * other (vertex ID, elabel, direction, vlabel) combination will have a key
   * prefix that is a prefix of this key, or for which this key is a prefix of.
   * This key-prefix is used to generate unique keys for RAMCloud objects that
   * store the potentially multiple segments that make up an edge list.
   *
   * @param vertexId
   * @param edgeLabel
   * @param dir
   * @param vertexLabel
   *
   * @return RAMCloud Key.
   */
  public static byte[] getEdgeListKeyPrefix(UInt128 vertexId, String edgeLabel,
      Direction dir, String vertexLabel) {
    byte[] edgeLabelByteArray = serializeString(edgeLabel);
    byte[] vertexLabelByteArray = serializeString(vertexLabel);
    ByteBuffer buffer =
        ByteBuffer.allocate(UInt128.BYTES 
            + Short.BYTES + edgeLabelByteArray.length
            + Byte.BYTES 
            + Short.BYTES + vertexLabelByteArray.length)
        .order(ByteOrder.LITTLE_ENDIAN);
    appendEdgeListKeyPrefixToBuffer(vertexId, edgeLabelByteArray, dir, 
        vertexLabelByteArray, buffer);
    return buffer.array();
  }

  /**
   * Generates a key prefix that defines an exclusive key-space for this
   * combination of vertex ID, edge label, edge direction, and vertex label. No
   * other (vertex ID, elabel, direction, vlabel) combination will have a key
   * prefix that is a prefix of this key, or for which this key is a prefix of.
   * This key-prefix is used to generate unique keys for RAMCloud objects that
   * store the potentially multiple segments that make up an edge list.
   *
   * @param vertexId
   * @param edgeLabel
   * @param dir
   * @param vertexLabel
   * @param byteBuffer
   *
   * @return RAMCloud Key.
   */
  public static void appendEdgeListKeyPrefixToBuffer(UInt128 vertexId, 
      byte[] edgeLabelByteArray,
      Direction dir, 
      byte[] vertexLabelByteArray, 
      ByteBuffer buffer) {
    buffer.putLong(vertexId.getUpperLong());
    buffer.putLong(vertexId.getLowerLong());
    buffer.putShort((short) edgeLabelByteArray.length);
    buffer.put(edgeLabelByteArray);
    buffer.put((byte) dir.ordinal());
    buffer.putShort((short) vertexLabelByteArray.length);
    buffer.put(vertexLabelByteArray);
  }

  /** 
   * Given a traversal map, return a list of all the unique neighbors.
   *
   * @param vMap Map of neighbors
   *
   * @return List of all unique neighbor vertices.
   */
  public static List<TorcVertex> neighborList(
      Map<TorcVertex, List<TorcVertex>> vMap) {
    Set<TorcVertex> set = new HashSet<TorcVertex>();
    for (Map.Entry e : vMap.entrySet()) {
        List<TorcVertex> list = (List<TorcVertex>)e.getValue();
        set.addAll(list);
    }

    List<TorcVertex> list = new ArrayList<TorcVertex>(set.size());
    list.addAll(set);

    return list;
  }

  /** 
   * Take two traversal results and merge them. 
   *
   * @param a First traversal result
   * @param b Second traversal result
   * @param dedup Whether or not to dedup the lists in the values of the merge.
   *
   * @return Joined traversal result.
   */
  public static TraversalResult fuse(
      TraversalResult trA,
      TraversalResult trB,
      boolean dedup) {
    Map<TorcVertex, List<TorcVertex>> a = trA.vMap;
    Map<TorcVertex, List<TorcVertex>> b = trB.vMap;

    Map<TorcVertex, List<TorcVertex>> fusedMap = new HashMap<>(a.size());
    Set<TorcVertex> globalFusedSet = new HashSet<>();

    for (Map.Entry e : a.entrySet()) {
      TorcVertex aVertex = (TorcVertex)e.getKey();
      List<TorcVertex> aVertexList = (List<TorcVertex>)e.getValue();

      if (dedup) {
        Set<TorcVertex> fusedSet = new HashSet<>();
        for (TorcVertex v : aVertexList) {
          if (b.containsKey(v))
            fusedSet.addAll(b.get(v));
        }

        if (fusedSet.size() > 0) {
          fusedMap.put(aVertex, new ArrayList<>(fusedSet));
          globalFusedSet.addAll(fusedSet);
        }
      } else {
        List<TorcVertex> fusedList = new ArrayList<>();
        for (TorcVertex v : aVertexList) {
          if (b.containsKey(v))
            fusedList.addAll(b.get(v));
        }

        if (fusedList.size() > 0) {
          fusedMap.put(aVertex, fusedList);
          globalFusedSet.addAll(fusedList);
        }
      }
    }

    return new TraversalResult(fusedMap, new ArrayList<>(globalFusedSet));
  }

  /**
   * Intersects the values in the map with the values in the list.
   * If the resulting value is an empty list, then remove the key from the map.
   * The resulting map will never have emtpy list values.
   *
   * @param a Map to intersect values on.
   * @param b Values to intersect map values with.
   */
  public static void intersect(
      TraversalResult trA,
      List<TorcVertex> b) {
    Map<TorcVertex, List<TorcVertex>> a = trA.vMap;
    Map<TorcVertex, List<TorcVertex>> newMap = new HashMap<>(a.size());
    for (Map.Entry e : a.entrySet()) {
      List<TorcVertex> aVertexList = (List<TorcVertex>)e.getValue();

      aVertexList.retainAll(b);
      
      if (aVertexList.size() > 0)
        newMap.put((TorcVertex)e.getKey(), aVertexList);
    }

    trA.vMap = newMap;
  }

  /**
   * Intersects the values in the map with the values in the list.
   * If the resulting value is an empty list, then remove the key from the map.
   * The resulting map will never have emtpy list values.
   *
   * @param a Map to intersect values on.
   * @param b Values to intersect map values with.
   */
  public static void intersect(
      TraversalResult trA,
      Set<TorcVertex> b) {
    Map<TorcVertex, List<TorcVertex>> a = trA.vMap;
    Map<TorcVertex, List<TorcVertex>> newMap = new HashMap<>(a.size());
    for (Map.Entry e : a.entrySet()) {
      List<TorcVertex> aVertexList = (List<TorcVertex>)e.getValue();

      aVertexList.retainAll(b);
      
      if (aVertexList.size() > 0)
        newMap.put((TorcVertex)e.getKey(), aVertexList);
    }

    trA.vMap = newMap;

//    trA.vMap.entrySet().removeIf( e -> {
//        List<TorcVertex> aVertexList = (List<TorcVertex>)e.getValue();
//        aVertexList.retainAll(b);
//        return aVertexList.size() == 0;
//      });
  }

  public static List<TorcVertex> keylist(
      TraversalResult trA) {
    Map<TorcVertex, List<TorcVertex>> a = trA.vMap;
    List<TorcVertex> keylist = new ArrayList<TorcVertex>(a.size());
    keylist.addAll(a.keySet());
    return keylist;
  }
}

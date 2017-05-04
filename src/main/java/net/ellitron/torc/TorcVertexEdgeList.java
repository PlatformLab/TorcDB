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
package net.ellitron.torc;

import net.ellitron.torc.util.TorcHelper;
import net.ellitron.torc.util.UInt128;

import edu.stanford.ramcloud.ClientException;
import edu.stanford.ramcloud.RAMCloudObject;
import edu.stanford.ramcloud.RAMCloudTransaction;

import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.function.Function;
import java.util.List;
import java.util.Map;

/**
 * TODO: Fix comments, no longer has vertexId, direction, or label.
 * <p>
 * A TorcVertexEdgeList is a RAMCloud-resident data structure representing a
 * list of edges of a certain TorcVertex that have the same label and direction
 * from the vertex. It abstracts details of the RAMCloud storage layer, and
 * provides a set of high-level methods for adding, fetching, and removing
 * edges from the list. These operations are all performed within a transaction
 * context, and therefore writes to this data structure are only externally
 * visible after a successful commit of the transaction. The implementation has
 * been performance tuned to make optimal use of the RAMCloud storage layer.
 * <p>
 * The storage location in RAMCloud for this object is uniquely defined by a
 * RAMCloud table and object key-prefix combination. The data structure assumes
 * exclusive access to all objects that have a key with the given key-prefix.
 * Thus, to allow multiple objects to share the same table, it is necessary to
 * assign them key-prefixes such that for all pairs of prefixes p1 and p2, p1
 * is never itself a prefix of p2.
 * <p>
 * Internally, an edge list is composed of multiple segments, where each
 * segment is stored in its own RAMCloud object. When a segment exceeds a
 * certain size limit (see EDGE_LIST_SIZE_LIMIT) due to edge list insertions or
 * updates, a certain amount is taken from the tail of the segment and from it
 * a new segment is created. To keep track of the segments, every segment has a
 * major and minor number. The head segment has (major,minor) = (0,0). When the
 * head segment splits for the first time, its tail is chopped off (see
 * EDGE_LIST_SPLIT_POINT), and a new segment is created with (major,minor) =
 * (1,0). This is referred to as major segment #1. The second time the head
 * segment splits, it will create major segment #2 and so on. If a major
 * segment (say major segment #N) subsequently grows too large due to insertion
 * or updates and splits for the first time, it will truncate its tail and
 * create a new segment with number (N,1). This segment is referred to as minor
 * segment #1 of major segment #N. The next time this happens the new segment
 * number will be (N,2) and so on, similar to major segments. In order find all
 * of these segments starting from the head segment, the RAMCloud object
 * storing the head segment contains a header that stores the total total
 * number of major segments for the edge list. Given this, all major segments
 * can then be read in parallel. Every major segment, similarly, contains
 * information on the total number of minor segments for the given major
 * segment, and all minor segments can be read in parallel. Therefore, at most
 * 1 RAMCloud read and 2 multi-reads need to be performed to fetch the entire
 * list, in the worst case.
 *
 * @author Jonathan Ellithorpe (jde@cs.stanford.edu)
 */
public class TorcVertexEdgeList {

  private static final Logger logger =
      Logger.getLogger(TorcVertexEdgeList.class);

  /*
   * Internal limit placed on the number of bytes allowed to be stored in a
   * single RAMCloud object. For large edge lists, if this limit is too high
   * then simple operations like prepend will need to read a lot of data only
   * to add a relatively small number of bytes to the list. If this limit is
   * too small, then operations like reading all of the edges in the list will
   * require reading many RAMCloud objects and incur high read overhead.
   */
  private static final int DEFAULT_SEGMENT_SIZE_LIMIT = 1 << 16;

  /*
   * When a RAMCloud object exceeds its size limit (EDGE_LIST_SIZE_LIMIT), the
   * object is split into two parts. This parameter specifies the byte offset
   * into the segment where the split should occur. Most of the time this will
   * not land exactly between two edges in the list, and in this case the
   * nearest boundary to the split point is selected, unless that happens to be
   * past the size limit, in which case the lower boundary is selected.
   */
  private static final int DEFAULT_SEGMENT_TARGET_SPLIT_POINT = 1 << 12;

  private final RAMCloudTransaction rctx;
  private final long rcTableId;
  private final byte[] keyPrefix;
  private int segmentSizeLimit;
  private int segmentTargetSplitPoint;

  private TorcVertexEdgeList(
      RAMCloudTransaction rctx,
      long rcTableId,
      byte[] keyPrefix,
      int segmentSizeLimit,
      int segmentTargetSplitPoint) {
    this.rctx = rctx;
    this.rcTableId = rcTableId;
    this.keyPrefix = keyPrefix;
    this.segmentSizeLimit = segmentSizeLimit;
    this.segmentTargetSplitPoint = segmentTargetSplitPoint;
  }

  /**
   * Open a TorcVertexEdgeList representing a list of all the edges that have
   * the given label and direction which are incident to the given vertex,
   * located in objects with the given key prefix and table. The given tableId
   * and key-prefix effectively define a persistent storage location for this
   * data structure. Note that this object assumes exclusive access to all
   * objects in the given table that have the given key-prefix, and so it is
   * the responsibility of the user of this object to enforce this exclusivity
   * in the rest of the system.
   *
   * @param rctx RAMCloud transaction context in which all operations are
   * performed.
   * @param rcTableId RAMCloud table Id in which edge lists are stored. This
   * table should be used exclusively for storing TorcVerteEdgeLists to avoid
   * collision with other objects in the table key-space.
   * @param keyPrefix The prefix to use for all RAMCloud objects used by this
   * data structure for storing data.
   *
   * @return
   */
  public static TorcVertexEdgeList open(
      RAMCloudTransaction rctx,
      long rcTableId,
      byte[] keyPrefix) {
    return new TorcVertexEdgeList(rctx, rcTableId, keyPrefix,
        DEFAULT_SEGMENT_SIZE_LIMIT, DEFAULT_SEGMENT_TARGET_SPLIT_POINT);
  }

  /**
   * See {@link #open(RAMCloudTransaction, long, byte[], UInt128,
   * TorcEdgeDirection, String)}.
   *
   * Has two additional parameters specifying the size limits for edge lists.
   *
   * @param segmentSizeLimit See {@link #DEFAULT_SEGMENT_SIZE_LIMIT}
   * @param segmentTargetSplitPoint See
   * {@link #DEFAULT_SEGMENT_TARGET_SPLIT_POINT}
   *
   * @return
   */
  public static TorcVertexEdgeList open(
      RAMCloudTransaction rctx,
      long rcTableId,
      byte[] keyPrefix,
      int segmentSizeLimit,
      int segmentTargetSplitPoint) {
    return new TorcVertexEdgeList(rctx, rcTableId, keyPrefix, segmentSizeLimit,
        segmentTargetSplitPoint);
  }

  /**
   * See {@link #DEFAULT_SEGMENT_SIZE_LIMIT}
   */
  public int getSegmentSizeLimit() {
    return segmentSizeLimit;
  }

  /**
   * See {@link #DEFAULT_SEGMENT_SIZE_LIMIT}
   *
   * @param segmentSizeLimit
   */
  public void setSegmentSizeLimit(int segmentSizeLimit) {
    this.segmentSizeLimit = segmentSizeLimit;
  }

  /**
   * See {@link #DEFAULT_SEGMENT_TARGET_SPLIT_POINT}
   */
  public int getSegmentTargetSplitPoint() {
    return segmentTargetSplitPoint;
  }

  /**
   * See {@link #DEFAULT_SEGMENT_TARGET_SPLIT_POINT}
   *
   * @param segmentTargetSplitPoint
   */
  public void setSegmentTargetSplitPoint(int segmentTargetSplitPoint) {
    this.segmentTargetSplitPoint = segmentTargetSplitPoint;
  }

  /**
   * Prepends the edge represented by the given neighbor vertex and serialized
   * properties to this edge list. If the edge list does not exist, then this
   * method will create a new one and return true to indicate that a new edge
   * list has been created (otherwise the method returns false).
   *
   * Note that this method allows multiple edges with the same neighbor vertex
   * to exist in the list (it will not check for duplicates).
   *
   * @param neighborId Remote vertex Id for this edge.
   * @param serializedProperties Pre-serialized properties for this edge.
   *
   * @return True if a new edge list was created, false otherwise.
   */
  public boolean prependEdge(UInt128 neighborId, byte[] serializedProperties) {

    /*
     * First retrieve the head segment.
     */
    ByteBuffer headSeg;
    byte[] headSegKey = getSegmentKey(0, 0);
    boolean newList = false;
    try {
      RAMCloudObject headSegObj = rctx.read(rcTableId, headSegKey);
      headSeg = ByteBuffer.allocate(headSegObj.getValueBytes().length)
          .put(headSegObj.getValueBytes());
      headSeg.flip();
    } catch (ClientException.ObjectDoesntExistException e) {
      headSeg = ByteBuffer.allocate(Integer.BYTES).putInt(0);
      headSeg.flip();
      newList = true;
    }

    ByteBuffer serializedEdge = serializeEdge(neighborId,
        serializedProperties);

    // Create fancy new segment with the edge we want to prepend.
    ByteBuffer prependedSeg =
        ByteBuffer.allocate(serializedEdge.capacity() + headSeg.capacity());
    int majorSegments = headSeg.getInt();
    prependedSeg.putInt(majorSegments);
    prependedSeg.put(serializedEdge);
    prependedSeg.put(headSeg);
    prependedSeg.flip();

    // See if we need to enforce our size limitation policy.
    if (prependedSeg.capacity() <= segmentSizeLimit) {
      rctx.write(rcTableId, headSegKey, prependedSeg.array());
    } else {
      // The edge list is in violation of our size policy, find a point at
      // which to split the list. In some special cases we may choose not 
      // to split (like in the case of this segment containing only a 
      // singe edge... in this case we choose not to split).
      int splitIndex = prependedSeg.capacity();
      int curMajorSegments = prependedSeg.getInt();
      while (prependedSeg.hasRemaining()) {
        int edgeStartPos = prependedSeg.position();
        int nextEdgeStartPos =
            edgeStartPos + getLengthOfCurrentEdge(prependedSeg);

        if (nextEdgeStartPos >= segmentTargetSplitPoint) {
          int left = segmentTargetSplitPoint - edgeStartPos;
          int right = nextEdgeStartPos - segmentTargetSplitPoint;

          if (edgeStartPos == Integer.BYTES) {
            // Always keep at least the first edge, even if the 
            // first edge extends beyond the size limit. It doesn't
            // make sense to leave this segment totally empty.
            splitIndex = nextEdgeStartPos;
            break;
          } else if (right < left) {
            // Target split point is closer to the start of the next
            // edge in the list than the start of this edge. In this 
            // case we want to split at the start of the next edge,
            // barring a special case.
            if (nextEdgeStartPos > segmentSizeLimit) {
              // The current edge extends beyond the size limit, 
              // so to comply with the specified size limitations
              // we choose to split at the start of this edge.
              splitIndex = edgeStartPos;
              break;
            } else {
              splitIndex = nextEdgeStartPos;
              break;
            }
          } else {
            // Target split point is closer to the start of this 
            // edge than the next. In this case we choose to make 
            // this edge part of the newly created segment.
            splitIndex = edgeStartPos;
            break;
          }
        }

        prependedSeg.position(nextEdgeStartPos);
      }

      prependedSeg.rewind();

      if (splitIndex == prependedSeg.capacity()) {
        // We have chosen not to split this segment.
        rctx.write(rcTableId, headSegKey, prependedSeg.array());
      } else {
        ByteBuffer newHeadSeg = ByteBuffer.allocate(splitIndex);
        ByteBuffer newMajorSeg = ByteBuffer.allocate(Integer.BYTES
            + prependedSeg.capacity() - splitIndex);
        int newMajorSegments = curMajorSegments + 1;

        newHeadSeg.put(prependedSeg.array(), 0, splitIndex);
        newHeadSeg.rewind();
        newHeadSeg.putInt(newMajorSegments);

        newMajorSeg.putInt(0);
        newMajorSeg.put(prependedSeg.array(), splitIndex,
            prependedSeg.capacity() - splitIndex);

        rctx.write(rcTableId, headSegKey, newHeadSeg.array());

        byte[] newMajorSegKey = getSegmentKey(newMajorSegments, 0);

        rctx.write(rcTableId, newMajorSegKey, newMajorSeg.array());
      }
    }

    return newList;
  }

  /**
   * Reads all of the TorcEdges in the edge list.
   *
   * @param graph TorcGraph to which these edges belong. Used for creating
   * TorcEdge objects.
   * @param baseVertexId ID of the vertex that owns this edge list.
   * @param label The edge label for the edges in this list.
   * @param direction Direction of the edges in this list.
   *
   * @return List of all the TorcEdges contained in this edge list.
   */
  public List<TorcEdge> readEdges(TorcGraph graph, UInt128 baseVertexId,
      String label, TorcEdgeDirection direction) {
    List<TorcEdge> edgeList = new ArrayList<>();
    parseSegments((segBuf) -> {
      while (segBuf.hasRemaining()) {
        UInt128 neighborId = getNeighborIdOfCurrentEdge(segBuf);
        if (direction == TorcEdgeDirection.DIRECTED_OUT) {
          edgeList.add(new TorcEdge(graph, baseVertexId, neighborId,
              TorcEdge.Type.DIRECTED, label));
        } else if (direction == TorcEdgeDirection.DIRECTED_IN) {
          edgeList.add(new TorcEdge(graph, neighborId, baseVertexId,
              TorcEdge.Type.DIRECTED, label));
        } else {
          edgeList.add(new TorcEdge(graph, baseVertexId, neighborId,
              TorcEdge.Type.UNDIRECTED, label));
        }
        segBuf.position(segBuf.position() + getLengthOfCurrentEdge(segBuf));
      }

      return true;
    });
    return edgeList;
  }

  /**
   * Reads all of the neighbor vertex IDs in the edge list.
   *
   * @return List of all the neighbor IDs contained in this edge list.
   */
  public List<UInt128> readNeighborIds() {
    List<UInt128> neighborIDList = new ArrayList<>();
    parseSegments((segBuf) -> {
      while (segBuf.hasRemaining()) {
        neighborIDList.add(getNeighborIdOfCurrentEdge(segBuf));
        segBuf.position(segBuf.position() + getLengthOfCurrentEdge(segBuf));
      }

      return true;
    });
    return neighborIDList;
  }

  /**
   * Finds and returns the serialized properties on the edge with the given
   * neighbor. Returns null if not found.
   */
  public byte[] getEdgeProperties(UInt128 neighborId) {
    List<byte[]> serPropList = new ArrayList<>();
    parseSegments((segBuf) -> {
      while (segBuf.hasRemaining()) {
        if (getNeighborIdOfCurrentEdge(segBuf).equals(neighborId)) {
          serPropList.add(getSerializedPropertiesOfCurrentEdge(segBuf));
          return false;
        }
        segBuf.position(segBuf.position() + getLengthOfCurrentEdge(segBuf));
      }

      return true;
    });

    if (serPropList.size() == 1) {
      return serPropList.get(0);
    } else {
      return null;
    }
  }

  /**
   * This method takes the given parser and applies it to each of the edge list
   * segments that compose the logical edge list.
   *
   * @param segmentParser Consumer object that is applied to each of the edge
   * list segments. Returns a boolean, where true means continue parsing
   * segments, and false means stop.
   */
  private void parseSegments(Function<ByteBuffer, Boolean> segmentParser) {
    byte[] headSegKey = getSegmentKey(0, 0);

    RAMCloudObject headSegObj;
    try {
      headSegObj = rctx.read(rcTableId, headSegKey);
    } catch (ClientException.ObjectDoesntExistException e) {
      return;
    }

    ByteBuffer headSeg =
        ByteBuffer.allocate(headSegObj.getValueBytes().length);
    headSeg.put(headSegObj.getValueBytes());
    headSeg.flip();

    int majorSegments = headSeg.getInt();

    if (!segmentParser.apply(headSeg)) {
      return;
    }

    for (int i = majorSegments; i > 0; --i) {
      byte[] majorSegKey = getSegmentKey(i, 0);

      RAMCloudObject majorSegObj;
      try {
        majorSegObj = rctx.read(rcTableId, majorSegKey);
      } catch (ClientException.ObjectDoesntExistException e) {
        continue;
      }

      ByteBuffer majorSeg =
          ByteBuffer.allocate(majorSegObj.getValueBytes().length);
      majorSeg.put(majorSegObj.getValueBytes());
      majorSeg.flip();

      int minorSegments = majorSeg.getInt();

      if (!segmentParser.apply(majorSeg)) {
        return;
      }

      for (int j = minorSegments; j > 0; --j) {
        byte[] minorSegKey = getSegmentKey(i, j);

        RAMCloudObject minorSegObj;
        try {
          minorSegObj = rctx.read(rcTableId, minorSegKey);
        } catch (ClientException.ObjectDoesntExistException e) {
          continue;
        }

        ByteBuffer minorSeg =
            ByteBuffer.allocate(minorSegObj.getValueBytes().length);
        minorSeg.put(minorSegObj.getValueBytes());
        minorSeg.flip();

        if (!segmentParser.apply(minorSeg)) {
          return;
        }
      }
    }
  }

  /**
   * Creates a RAMCloud key for the given edge list segment.
   *
   * @param majorSegmentNumber Major number of the segment.
   * @param minorSegmentNumber Minor number of the segment.
   *
   * @return Byte array representing the RAMCloud key.
   */
  private byte[] getSegmentKey(int majorSegmentNumber,
      int minorSegmentNumber) {
    ByteBuffer buffer =
        ByteBuffer.allocate(keyPrefix.length + Integer.BYTES * 2);
    buffer.put(keyPrefix);
    buffer.putInt(majorSegmentNumber);
    buffer.putInt(minorSegmentNumber);
    return buffer.array();
  }

  /*
   * These methods hide the serialization format of an edge in the list from
   * the rest of the code.
   */
  /**
   * Given a ByteBuffer of serialized edges, parses the serialized edge at the
   * current location of the buffer for the neighbor ID. If the buffer is empty
   * or there are no more edges to parse, then this method returns null.
   *
   * @param edgeListBuf Buffer of serialized edges.
   *
   * @return UInt128 representing the neighbor ID of the current edge in the
   * buffer. If the buffer has no more bytes remaining then null is returned.
   */
  private UInt128 getNeighborIdOfCurrentEdge(ByteBuffer edgeListBuf) {
    if (edgeListBuf.remaining() == 0) {
      return null;
    }

    if (edgeListBuf.remaining() < UInt128.BYTES + Short.BYTES) {
      throw new RuntimeException(String.format("Incomplete serialized edge "
          + "found in edge list. Should be at least %d bytes, but only %d "
          + "bytes are remaining in the buffer.", UInt128.BYTES + Short.BYTES,
          edgeListBuf.remaining()));
    }

    byte[] neighborIdBytes = new byte[UInt128.BYTES];
    edgeListBuf.mark();
    edgeListBuf.get(neighborIdBytes);
    edgeListBuf.reset();
    return new UInt128(neighborIdBytes);
  }

  /**
   * Given a ByteBuffer of serialized edges, parses the serialized edge at the
   * current location of the buffer for the serialized properties. If the
   * buffer is empty or there are no more edges to parse, then this method
   * returns null.
   *
   * @param edgeListBuf Buffer of serialized edges.
   *
   * @return Byte array representing the serialized properties of the current
   * edge in the buffer. If the buffer has no more bytes remaining then null is
   * returned.
   */
  private byte[] getSerializedPropertiesOfCurrentEdge(ByteBuffer edgeListBuf) {
    if (edgeListBuf.remaining() == 0) {
      return null;
    }

    if (edgeListBuf.remaining() < UInt128.BYTES + Short.BYTES) {
      throw new RuntimeException(String.format("Incomplete serialized edge "
          + "found in edge list. Should be at least %d bytes, but only %d "
          + "bytes are remaining in the buffer.", UInt128.BYTES + Short.BYTES,
          edgeListBuf.remaining()));
    }

    edgeListBuf.mark();
    edgeListBuf.position(edgeListBuf.position() + UInt128.BYTES);
    short propLen = edgeListBuf.getShort();

    if (edgeListBuf.remaining() < propLen) {
      edgeListBuf.reset();
      throw new RuntimeException(String.format("Incomplete serialized edge "
          + "found in edge list. Should be at least %d bytes, but only %d "
          + "bytes are remaining in the buffer.", UInt128.BYTES + Short.BYTES
          + propLen, edgeListBuf.remaining()));
    }

    byte[] serializedProperties = new byte[propLen];
    edgeListBuf.get(serializedProperties);
    edgeListBuf.reset();

    return serializedProperties;
  }

  /**
   * Calculates the byte length of the edge at the current location in the
   * ByteBuffer. If the buffer is empty or there are no more edge remaining,
   * then this method returns 0. If the bytes remaining in the buffer is less
   * than the minimum serialized edge length, or the length calculated while
   * parsing the edge exceeds the bytes remaining in the buffer, then -1 is
   * returned.
   *
   * @param edgeListBuf Buffer of serialized edges.
   *
   * @return Length of the edge at the current location in the buffer. -1 is
   * returned if the bytes remaining in the buffer is less than the minimum
   * length of an edge, or if the calculated edge length is larger than the
   * number of bytes remaining in the buffer.
   */
  private int getLengthOfCurrentEdge(ByteBuffer edgeListBuf) {
    if (edgeListBuf.remaining() == 0) {
      return 0;
    }

    if (edgeListBuf.remaining() < UInt128.BYTES + Short.BYTES) {
      throw new RuntimeException(String.format("Incomplete serialized edge "
          + "found in edge list. Should be at least %d bytes, but only %d "
          + "bytes are remaining in the buffer.", UInt128.BYTES + Short.BYTES,
          edgeListBuf.remaining()));
    }

    short propLen =
        edgeListBuf.getShort(edgeListBuf.position() + UInt128.BYTES);
    int edgeLength = UInt128.BYTES + Short.BYTES + propLen;

    if (edgeListBuf.remaining() < edgeLength) {
      throw new RuntimeException(String.format("Incomplete serialized edge "
          + "found in edge list. Should be %d bytes, but only %d bytes are "
          + "remaining in the buffer.", edgeLength, edgeListBuf.remaining()));
    }

    return edgeLength;
  }

  /**
   * Serialize an edge to a neighbor with the given properties.
   *
   * @param neighbor Neighbor vertex on the edge.
   * @param serializedProperties Serialized properties of this edge.
   *
   * @return ByteBuffer containing the serialized edge.
   */
  private ByteBuffer serializeEdge(UInt128 neighborId,
      byte[] serializedProperties) {
    int serializedEdgeLength =
        UInt128.BYTES + Short.BYTES + serializedProperties.length;
    ByteBuffer buf = ByteBuffer.allocate(serializedEdgeLength);
    buf.put(neighborId.toByteArray());
    buf.putShort((short) serializedProperties.length);
    buf.put(serializedProperties);
    buf.flip();
    return buf;
  }

  private static class EntryList {

    List<Entry> list;
    ByteBuffer serializedFormat;

    public EntryList(List<Entry> list) {
      this.list = list;
      this.serializedFormat = null;
    }

    private EntryList(ByteBuffer serializedFormat) {
      this.list = null;
      this.serializedFormat = serializedFormat.asReadOnlyBuffer();
    }

    public List<Entry> getList() {
      if (list != null) {
        return list;
      } else {
        list = new ArrayList<>();

        ByteBuffer listBuf = serializedFormat.duplicate();

        while (listBuf.hasRemaining()) {
          int entryLength = listBuf.getInt();
          ByteBuffer entryBuf = listBuf.duplicate();
          entryBuf.limit(entryBuf.position() + entryLength);
          list.add(Entry.deserialize(entryBuf));
        }

        return list;
      }
    }

    public int getSerializedLength() {
      return serialize().remaining();
    }

    public ByteBuffer serialize() {
      if (serializedFormat != null) {
        return serializedFormat;
      } else {
        int serializedLength = 0;
        for (Entry entry : list) {
          serializedLength += Integer.BYTES;
          serializedLength += entry.getSerializedLength();
        }

        ByteBuffer buf = ByteBuffer.allocate(serializedLength);
        for (Entry entry : list) {
          buf.putInt(entry.getSerializedLength());
          buf.put(entry.serialize());
        }

        serializedFormat = buf;
        return serializedFormat;
      }
    }

    public static EntryList deserialize(ByteBuffer serializedFormat) {
      return new EntryList(serializedFormat);
    }
  }

  /**
   * A class representing an entry in the edge list with methods for
   * serializing / de-serializing the Entry to / from an array of bytes.
   */
  private static class Entry {

    private UInt128 neighborId;
    private Map<String, List<String>> properties;
    private ByteBuffer serializedFormat;

    /**
     * Construct Entry from raw elements.
     *
     * @param neighborId ID of the neighbor represented by this entry.
     * @param properties Properties attached to this entry.
     */
    public Entry(UInt128 neighborId, Map<String, List<String>> properties) {
      this.neighborId = neighborId;
      this.properties = properties;
      this.serializedFormat = null;
    }

    /**
     * Construct Entry from the serialized entry in the byte array.
     *
     * @param serializedFormat Serialization of this entry.
     */
    private Entry(ByteBuffer serializedFormat) {
      this.neighborId = null;
      this.properties = null;
      this.serializedFormat = serializedFormat.asReadOnlyBuffer();
    }

    /**
     * Get neighbor ID of this entry.
     *
     * @return
     */
    public UInt128 getNeighborId() {
      if (neighborId != null) {
        return neighborId;
      } else {
        ByteBuffer neighborIdBuf = serializedFormat.duplicate();
        neighborIdBuf.limit(neighborIdBuf.position() + UInt128.BYTES);
        this.neighborId = UInt128.parseFromByteBuffer(neighborIdBuf);
        return neighborId;
      }
    }

    /**
     * Get the properties of this entry.
     *
     * @return
     */
    public Map<String, List<String>> getProperties() {
      if (properties != null) {
        return properties;
      } else {
        ByteBuffer propBuf = serializedFormat.duplicate();
        propBuf.position(propBuf.position() + UInt128.BYTES);
        this.properties = TorcHelper.deserializeProperties(propBuf);
        return properties;
      }
    }

    /**
     * Return the serialized length of this entry in bytes.
     */
    public int getSerializedLength() {
      return serialize().remaining();
    }

    /**
     * Return serialized format of this entry.
     *
     * @return
     */
    public ByteBuffer serialize() {
      if (serializedFormat != null) {
        return serializedFormat;
      } else {
        byte[] serializedNeighborId = neighborId.toByteArray();
        byte[] serializedProperties =
            TorcHelper.serializeProperties(properties).array();
        serializedFormat =
            ByteBuffer.allocate(UInt128.BYTES + serializedProperties.length);
        serializedFormat.put(serializedNeighborId);
        serializedFormat.put(serializedProperties);
        serializedFormat.flip();
        return serializedFormat;
      }
    }

    /**
     * Return entry represented by this serialized version.
     *
     * @param serializedFormat
     *
     * @return
     */
    public static Entry deserialize(ByteBuffer serializedFormat) {
      return new Entry(serializedFormat);
    }
  }
}

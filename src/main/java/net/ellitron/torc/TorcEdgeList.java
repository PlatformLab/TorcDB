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
import edu.stanford.ramcloud.RAMCloud;
import edu.stanford.ramcloud.RAMCloudObject;
import edu.stanford.ramcloud.RAMCloudTransaction;
import edu.stanford.ramcloud.RAMCloudTransactionReadOp;
import edu.stanford.ramcloud.Status;
import edu.stanford.ramcloud.multiop.MultiReadObject;

import org.apache.tinkerpop.gremlin.structure.Direction;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

/**
 * A collection of static methods for reading and writing edge lists to
 * RAMCloud in TorcDB.
 *
 * Here is an illustration of the layout of edge lists in RAMCloud and what
 * they look like before and after a split:
 *
 * +-------------------------------------------------------------------------+
 * |                                                                         |
 * |          RAMCloud Key           RAMCloud Value                          |
 * |                                                                         |
 * |                                   +numTailSegments                      |
 * |          +-----------+---+      +-v-+-------+-------+-------+---------+ |
 * | HeadSeg  |Key Prefix | 0 | +--> | 3 | edge0 | edge1 |  ...  | edge324 | |
 * |    +     +-----------+---+      +---+-------+-------+-------+---------+ |
 * |    |                                                                    |
 * |    v     +-----------+---+      +---------+---------+-------+---------+ |
 * | TailSeg0 |Key Prefix | 3 | +--> | edge325 | edge326 |  ...  | edge683 | |
 * |    +     +-----------+---+      +---------+---------+-------+---------+ |
 * |    |                                                                    |
 * |    v     +-----------+---+      +---------+---------+------+----------+ |
 * | TailSeg1 |Key Prefix | 2 | +--> | edge684 | edge685 |  ... | edge1245 | |
 * |    +     +-----------+---+      +---------+---------+------+----------+ |
 * |    |                                                                    |
 * |    v     +-----------+---+      +----------+----------+----+----------+ |
 * | TailSeg2 |Key Prefix | 1 | +--> | edge1246 | edge1247 | .. | edge1545 | |
 * |          +-----------+---+      +----------+----------+----+----------+ |
 * |                                                                         |
 * +----------------------------------+--------------------------------------+
 *                                    |
 *                       Split after edge160 creates
 *                       new tail seg w/ edges161-324
 *                                    |
 *                                    v
 * +----------------------------------+--------------------------------------+
 * |                                                                         |
 * |          RAMCloud Key           RAMCloud Value                          |
 * |                                                                         |
 * |                                   +numTailSegments                      |
 * |          +-----------+---+      +-v-+-------+-------+-------+---------+ |
 * | HeadSeg  |Key Prefix | 0 | +--> | 4 | edge0 | edge1 |  ...  | edge160 | |
 * |    +     +-----------+---+      +---+-------+-------+-------+---------+ |
 * |    |                                                                    |
 * |    v     +-----------+---+      +---------+---------+-------+---------+ |
 * | TailSeg0 |Key Prefix | 4 | +--> | edge161 | edge162 |  ...  | edge324 | |
 * |    +     +-----------+---+      +---------+---------+-------+---------+ |
 * |    |                                                                    |
 * |    v     +-----------+---+      +---------+---------+-------+---------+ |
 * | TailSeg1 |Key Prefix | 3 | +--> | edge325 | edge326 |  ...  | edge683 | |
 * |    +     +-----------+---+      +---------+---------+-------+---------+ |
 * |    |                                                                    |
 * |    v     +-----------+---+      +---------+---------+------+----------+ |
 * | TailSeg2 |Key Prefix | 2 | +--> | edge684 | edge685 |  ... | edge1245 | |
 * |    +     +-----------+---+      +---------+---------+------+----------+ |
 * |    |                                                                    |
 * |    v     +-----------+---+      +----------+----------+----+----------+ |
 * | TailSeg3 |Key Prefix | 1 | +--> | edge1246 | edge1247 | .. | edge1545 | |
 * |          +-----------+---+      +----------+----------+----+----------+ |
 * |                                                                         |
 * +-------------------------------------------------------------------------+
 *
 *
 * @author Jonathan Ellithorpe (jde@cs.stanford.edu)
 */
public class TorcEdgeList {

  /*
   * Limit placed on the number of bytes allowed to be stored in a single
   * RAMCloud object. For large edge lists, if this limit is too high then
   * simple operations like prepend will need to read a lot of data only to add
   * a relatively small number of bytes to the list. If this limit is too
   * small, then operations like reading all of the edges in the list will
   * require reading many RAMCloud objects and incur high read overhead.
   */
  private static final int DEFAULT_SEGMENT_SIZE_LIMIT = 1 << 10;

  /*
   * When a RAMCloud object exceeds its size limit (DEFAULT_SEGMENT_SIZE_LIMIT), the
   * object is split into two parts. This parameter specifies the byte offset
   * into the segment where the split should occur. Most of the time this will
   * not land exactly between two edges in the list, and in this case the
   * nearest boundary to the split point is selected, unless that happens to be
   * past the size limit, in which case the lower boundary is selected.
   */
  private static final int DEFAULT_SEGMENT_TARGET_SPLIT_POINT = 0;

  /*
   * Limit placed on the number of asynchronous reads that can be outstanding at
   * any one time.
   */
  private static final int DEFAULT_MAX_ASYNC_READS = 1 << 7;

  /*
   * Limit placed on the maximum size of multireads.
   */
  private static final int DEFAULT_MAX_MULTIREAD_SIZE = 1 << 9;

  public static boolean prepend(
      RAMCloudTransaction rctx,
      long rcTableId,
      byte[] keyPrefix,
      UInt128 neighborId, 
      byte[] serializedProperties) {
    return prepend(rctx, rcTableId, keyPrefix, neighborId, serializedProperties,
        DEFAULT_SEGMENT_SIZE_LIMIT, DEFAULT_SEGMENT_TARGET_SPLIT_POINT);
  }

  public static boolean prepend(
      RAMCloud client,
      long rcTableId,
      byte[] keyPrefix,
      UInt128 neighborId, 
      byte[] serializedProperties) {
    return prepend(client, rcTableId, keyPrefix, neighborId, serializedProperties,
        DEFAULT_SEGMENT_SIZE_LIMIT, DEFAULT_SEGMENT_TARGET_SPLIT_POINT);
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
   * @param rctx RAMCloud transaction in which to perform the operation.
   * @param rcTableId The table in which the edge list is (to be) stored.
   * @param keyPrefix Key prefix for the edge list.
   * @param neighborId Remote vertex Id for this edge.
   * @param serializedProperties Pre-serialized properties for this edge.
   * @param segment_size_limit Limit on the max size of segments.
   * @param segment_target_split_point Where to split when splitting is needed.
   *
   * @return True if a new edge list was created, false otherwise.
   */
  public static boolean prepend(
      RAMCloudTransaction rctx,
      long rcTableId,
      byte[] keyPrefix,
      UInt128 neighborId, 
      byte[] serializedProperties,
      int segment_size_limit,
      int segment_target_split_point) {
    /* Read out the head segment. */
    ByteBuffer headSeg;
    byte[] headSegKey = getSegmentKey(keyPrefix, 0);
    boolean newList = false;
    try {
      RAMCloudObject headSegObj = rctx.read(rcTableId, headSegKey);
      if (headSegObj != null) {
        headSeg = ByteBuffer.allocate(headSegObj.getValueBytes().length)
            .order(ByteOrder.LITTLE_ENDIAN)
            .put(headSegObj.getValueBytes());
        headSeg.flip();
      } else {
        headSeg = ByteBuffer.allocate(Integer.BYTES)
            .order(ByteOrder.LITTLE_ENDIAN).putInt(0);
        headSeg.flip();
        newList = true;
      }
    } catch (ClientException e) {
      throw new RuntimeException(e);
    }

    int serializedEdgeLength =
        UInt128.BYTES + Short.BYTES + serializedProperties.length;

    ByteBuffer serializedEdge = ByteBuffer.allocate(serializedEdgeLength)
        .order(ByteOrder.LITTLE_ENDIAN);
    serializedEdge.put(neighborId.toByteArray());
    serializedEdge.putShort((short) serializedProperties.length);
    serializedEdge.put(serializedProperties);
    serializedEdge.flip();

    /* Prepend edge to head segment. */
    ByteBuffer prependedSeg =
        ByteBuffer.allocate(serializedEdge.capacity() + headSeg.capacity())
        .order(ByteOrder.LITTLE_ENDIAN);
    int majorSegments = headSeg.getInt();
    prependedSeg.putInt(majorSegments);
    prependedSeg.put(serializedEdge);
    prependedSeg.put(headSeg);
    prependedSeg.flip();

    /* Check if we need to split the head segment. */
    if (prependedSeg.capacity() <= segment_size_limit) {
      /* Common case, don't need to split. */
      rctx.write(rcTableId, headSegKey, prependedSeg.array());
    } else {
      /* Head segment is too big, we need to find a good split point. In some
       * special cases we won't be able to split, like when the segment is just
       * one enormous edge. The following code sets splitIndex to the right
       * point to split the head segment. */
      int splitIndex = prependedSeg.capacity();
      int currentNumTailSegments = prependedSeg.getInt();
      while (prependedSeg.hasRemaining()) {
        int edgeStartPos = prependedSeg.position();
        int nextEdgeStartPos = edgeStartPos + UInt128.BYTES + Short.BYTES 
            + prependedSeg.getShort(edgeStartPos + UInt128.BYTES);

        if (nextEdgeStartPos >= segment_target_split_point) {
          /*
           * The current edge either stradles the split point, or is right up
           * against it.
           *
           *                                       nextEdgeStartPos
           *            <--left-->          <--right-->   V
           * ------|--------------------|-----------------|--------
           *       ^                    ^
           * edgeStartPos     DEFAULT_SEGMENT_TARGET_SPLIT_POINT
           */
          int left = segment_target_split_point - edgeStartPos;
          int right = nextEdgeStartPos - segment_target_split_point;

          if (right < left) {
            /* Target split point is closer to the start of the next edge in
             * the list than the start of this edge. In this case we generally
             * want to split at the start of the next edge, except for a
             * special case handled here. */
            if (nextEdgeStartPos > segment_size_limit) {
              /* Special case, the current edge extends beyond the size limit.
               * To still enforce the size limit policy we choose not to keep
               * this edge in the head segment. */
              splitIndex = edgeStartPos;
              break;
            } else {
              splitIndex = nextEdgeStartPos;
              break;
            }
          } else {
            /* Target split point is closer to the start of this edge than the
             * next. In this case we choose to make this edge part of the newly
             * created segment. */
            splitIndex = edgeStartPos;
            break;
          }
        }

        prependedSeg.position(nextEdgeStartPos);
      }

      prependedSeg.rewind();

      if (splitIndex == prependedSeg.capacity()) {
        /* We have chosen not to split this segment. */
        rctx.write(rcTableId, headSegKey, prependedSeg.array());
      } else {
        /* Split based on splitIndex. */
        ByteBuffer newHeadSeg = ByteBuffer.allocate(splitIndex)
            .order(ByteOrder.LITTLE_ENDIAN);
        ByteBuffer newTailSeg = ByteBuffer.allocate(prependedSeg.capacity() 
            - splitIndex).order(ByteOrder.LITTLE_ENDIAN);

        int newNumTailSegments = currentNumTailSegments + 1;

        newHeadSeg.put(prependedSeg.array(), 0, splitIndex);
        newHeadSeg.rewind();
        newHeadSeg.putInt(newNumTailSegments);

        newTailSeg.put(prependedSeg.array(), splitIndex,
            prependedSeg.capacity() - splitIndex);

        byte[] newTailSegKey = getSegmentKey(keyPrefix, newNumTailSegments);

        rctx.write(rcTableId, headSegKey, newHeadSeg.array());
        rctx.write(rcTableId, newTailSegKey, newTailSeg.array());
      }
    }

    return newList;
  }

  /**
   * Prepends the edge represented by the given neighbor vertex and serialized
   * properties to this edge list. This version of prepend performs the
   * operation outside of any transaction context. If the edge list does not
   * exist, then this method will create a new one and return true to indicate
   * that a new edge list has been created (otherwise the method returns false).
   *
   * Note that this method allows multiple edges with the same neighbor vertex
   * to exist in the list (it will not check for duplicates).
   *
   * @param client RAMCloud client to use to perform the operation.
   * @param rcTableId The table in which the edge list is (to be) stored.
   * @param keyPrefix Key prefix for the edge list.
   * @param neighborId Remote vertex Id for this edge.
   * @param serializedProperties Pre-serialized properties for this edge.
   * @param segment_size_limit Limit on the max size of segments.
   * @param segment_target_split_point Where to split when splitting is needed.
   *
   * @return True if a new edge list was created, false otherwise.
   */
  public static boolean prepend(
      RAMCloud client,
      long rcTableId,
      byte[] keyPrefix,
      UInt128 neighborId, 
      byte[] serializedProperties,
      int segment_size_limit,
      int segment_target_split_point) {
    /* Read out the head segment. */
    ByteBuffer headSeg;
    byte[] headSegKey = getSegmentKey(keyPrefix, 0);
    boolean newList = false;
    try {
      RAMCloudObject headSegObj = client.read(rcTableId, headSegKey);
      if (headSegObj != null) {
        headSeg = ByteBuffer.allocate(headSegObj.getValueBytes().length)
            .order(ByteOrder.LITTLE_ENDIAN)
            .put(headSegObj.getValueBytes());
        headSeg.flip();
      } else {
        headSeg = ByteBuffer.allocate(Integer.BYTES)
            .order(ByteOrder.LITTLE_ENDIAN).putInt(0);
        headSeg.flip();
        newList = true;
      }
    } catch (ClientException e) {
      throw new RuntimeException(e);
    }

    int serializedEdgeLength =
        UInt128.BYTES + Short.BYTES + serializedProperties.length;

    ByteBuffer serializedEdge = ByteBuffer.allocate(serializedEdgeLength)
        .order(ByteOrder.LITTLE_ENDIAN);
    serializedEdge.put(neighborId.toByteArray());
    serializedEdge.putShort((short) serializedProperties.length);
    serializedEdge.put(serializedProperties);
    serializedEdge.flip();

    /* Prepend edge to head segment. */
    ByteBuffer prependedSeg =
        ByteBuffer.allocate(serializedEdge.capacity() + headSeg.capacity())
        .order(ByteOrder.LITTLE_ENDIAN);
    int majorSegments = headSeg.getInt();
    prependedSeg.putInt(majorSegments);
    prependedSeg.put(serializedEdge);
    prependedSeg.put(headSeg);
    prependedSeg.flip();

    /* Check if we need to split the head segment. */
    if (prependedSeg.capacity() <= segment_size_limit) {
      /* Common case, don't need to split. */
      client.write(rcTableId, headSegKey, prependedSeg.array(), null);
    } else {
      /* Head segment is too big, we need to find a good split point. In some
       * special cases we won't be able to split, like when the segment is just
       * one enormous edge. The following code sets splitIndex to the right
       * point to split the head segment. */
      int splitIndex = prependedSeg.capacity();
      int currentNumTailSegments = prependedSeg.getInt();
      while (prependedSeg.hasRemaining()) {
        int edgeStartPos = prependedSeg.position();
        int nextEdgeStartPos = edgeStartPos + UInt128.BYTES + Short.BYTES 
            + prependedSeg.getShort(edgeStartPos + UInt128.BYTES);

        if (nextEdgeStartPos >= segment_target_split_point) {
          /*
           * The current edge either stradles the split point, or is right up
           * against it.
           *
           *                                       nextEdgeStartPos
           *            <--left-->          <--right-->   V
           * ------|--------------------|-----------------|--------
           *       ^                    ^
           * edgeStartPos     DEFAULT_SEGMENT_TARGET_SPLIT_POINT
           */
          int left = segment_target_split_point - edgeStartPos;
          int right = nextEdgeStartPos - segment_target_split_point;

          if (right < left) {
            /* Target split point is closer to the start of the next edge in
             * the list than the start of this edge. In this case we generally
             * want to split at the start of the next edge, except for a
             * special case handled here. */
            if (nextEdgeStartPos > segment_size_limit) {
              /* Special case, the current edge extends beyond the size limit.
               * To still enforce the size limit policy we choose not to keep
               * this edge in the head segment. */
              splitIndex = edgeStartPos;
              break;
            } else {
              splitIndex = nextEdgeStartPos;
              break;
            }
          } else {
            /* Target split point is closer to the start of this edge than the
             * next. In this case we choose to make this edge part of the newly
             * created segment. */
            splitIndex = edgeStartPos;
            break;
          }
        }

        prependedSeg.position(nextEdgeStartPos);
      }

      prependedSeg.rewind();

      if (splitIndex == prependedSeg.capacity()) {
        /* We have chosen not to split this segment. */
        client.write(rcTableId, headSegKey, prependedSeg.array(), null);
      } else {
        /* Split based on splitIndex. */
        ByteBuffer newHeadSeg = ByteBuffer.allocate(splitIndex)
            .order(ByteOrder.LITTLE_ENDIAN);
        ByteBuffer newTailSeg = ByteBuffer.allocate(prependedSeg.capacity() 
            - splitIndex).order(ByteOrder.LITTLE_ENDIAN);

        int newNumTailSegments = currentNumTailSegments + 1;

        newHeadSeg.put(prependedSeg.array(), 0, splitIndex);
        newHeadSeg.rewind();
        newHeadSeg.putInt(newNumTailSegments);

        newTailSeg.put(prependedSeg.array(), splitIndex,
            prependedSeg.capacity() - splitIndex);

        byte[] newTailSegKey = getSegmentKey(keyPrefix, newNumTailSegments);

        client.write(rcTableId, headSegKey, newHeadSeg.array(), null);
        client.write(rcTableId, newTailSegKey, newTailSeg.array(), null);
      }
    }

    return newList;
  }

  /**
   * This method takes an array of edges and creates the same set of RAMCloud
   * key / value pairs that would be created had the edges been added one by
   * one, starting with the first edge in the array and ending with the last
   * edge in the array, and writes the resulting key/value pairs into the given
   * RAMCloud image file. The resulting key / value pairs written into the
   * image should exactly match the key / value pairs that would be in RAMCloud
   * had the TorcEdgeList.prepend() method been called for each of the edges,
   * starting with the first in the array and ending with the last in the array.
   *
   * @param edgeListTableOS The image file to write to.
   * @param keyPrefix Key prefix for the edge list.
   * @param neighborIds Remote vertex Ids for this edge list. List is in the
   * order these edges would have been added in (0th edge is the first edge
   * added).
   * @param propMaps Property maps for the edges. Same ordering as neighborIds.
   * Can be an empty list, which signals that these edges do not have
   * properties.
   */
  public static void writeListToFile(
      OutputStream edgeListTableOS,
      byte[] keyPrefix,
      List<UInt128> neighborIds, 
      List<byte[]> serializedPropList) {
    /* General strategy here is to simulate the prepending of edges by
     * prepending edge lengths instead of actual edges and split by the sum of
     * the edge lengths in the list, and thus calculate how many edges should go
     * in each segment had they been prepended one by one in that order. Using
     * this information, we can then take the list of edges and directly pack
     * the correct number into the correct segments. 
     */

    // We only need to keep track of the series of edge lengths in the head
    // segment, because once a tail segment is "pinched" off after a split of
    // the head segment, it will remain unchanged, and the only information we
    // need to keep around is the number of edges that made it into the segment.
    LinkedList<Integer> headSegEdgeLengths = new LinkedList<>();
    
    // As we split off tail segments from the head, we record the number of
    // edges that made it into the resulting tail segment in this list. Elements
    // are added to the end of this list as the simulation proceeds, therefore
    // the first element of the list represents the number of edges in the last
    // segment of the edge list, and the last element represents the number of
    // edges in the head segment.
    ArrayList<Integer> edgesPerSegment = new ArrayList<>();
    
    // Here we record the sizes, in bytes, of segments created during the
    // simulation (in the same ordering as the edgesPerSegment list). These data
    // are used after the simulation to allocate appropriately sized ByteBuffers
    // that represent the edge list segments. Although these data could be
    // derived from edgesPerSegment and the argument list of edges
    // post-simulation, this information is calculated already during the
    // simulation and so it is more efficient to simply save it for later use.
    ArrayList<Integer> segmentSizes = new ArrayList<>();
   
    // Head segment starts with an integer field containing the total number of
    // tail segments for this edge list, so this is our starting length for the
    // head segment.
    int headSegLen = Integer.BYTES;

    // Simulate prepending the edges, starting with the first in the argument
    // list and ending with the last in the argument list.
    for (int i = 0; i < neighborIds.size(); i++) {
      int edgeLength;
      if (serializedPropList.size() > 0) {
        edgeLength = UInt128.BYTES + Short.BYTES + 
            serializedPropList.get(i).length;
      } else {
        edgeLength = UInt128.BYTES + Short.BYTES;
      }
      headSegLen += edgeLength;
      headSegEdgeLengths.addFirst(edgeLength);

      if (headSegLen >= DEFAULT_SEGMENT_SIZE_LIMIT) {
        int edgesInNewTailSeg = 0;
        // In the head segment, edges start after the integer field that stores
        // the total number of tail segments for the edge list.
        int edgeStartPos = Integer.BYTES;
        int nextEdgeStartPos = Integer.BYTES;
        for (int j = 0; j < headSegEdgeLengths.size(); j++) {
          edgeStartPos = nextEdgeStartPos;
          nextEdgeStartPos = edgeStartPos + headSegEdgeLengths.get(j);

          if (nextEdgeStartPos >= DEFAULT_SEGMENT_TARGET_SPLIT_POINT) {
            /*
             * The current edge either stradles the split point, or is right up
             * against it.
             *
             *                                       nextEdgeStartPos
             *            <--left-->          <--right-->   V
             * ------|--------------------|-----------------|--------
             *       ^                    ^
             * edgeStartPos     DEFAULT_SEGMENT_TARGET_SPLIT_POINT
             */
            int left = DEFAULT_SEGMENT_TARGET_SPLIT_POINT - edgeStartPos;
            int right = nextEdgeStartPos - DEFAULT_SEGMENT_TARGET_SPLIT_POINT;

            if (right < left) {
              /* Target split point is closer to the start of the next edge in
               * the list than the start of this edge. In this case we
               * generally want to split at the start of the next edge, except
               * for a special case handled here. */
              if (nextEdgeStartPos > DEFAULT_SEGMENT_SIZE_LIMIT) {
                /* Special case, the current edge extends beyond the size limit.
                 * To still enforce the size limit policy we choose not to keep
                 * this edge in the head segment. */
                edgesInNewTailSeg = headSegEdgeLengths.size() - j;
                break;
              } else {
                edgesInNewTailSeg = headSegEdgeLengths.size() - (j + 1);
                break;
              }
            } else {
              /* Target split point is closer to the start of this edge than the
               * next. In this case we choose to make this edge part of the
               * newly created segment. */
              edgesInNewTailSeg = headSegEdgeLengths.size() - j;
              break;
            }
          }
        }

        // At this point we have figured out how many edges go in the new tail
        // segment (which could potentially be zero, which means the edge is
        // actually NOT split. In this case just move on).
        
        if (edgesInNewTailSeg > 0) {
          edgesPerSegment.add(edgesInNewTailSeg);

          int segmentSize = 0;
          for (int j = 0; j < edgesInNewTailSeg; j++) {
            segmentSize += headSegEdgeLengths.getLast();
            headSegEdgeLengths.removeLast();
          }
          headSegLen -= segmentSize;
          
          segmentSizes.add(segmentSize);
        }
      } // if (headSegLen >= DEFAULT_SEGMENT_SIZE_LIMIT) 
    } // for (int i = 0; i < neighborIds.size(); i++) 

    // Whatever is left in headSegEdgeLengths after the simulation is over
    // represents the final state of the head segment.
    edgesPerSegment.add(headSegEdgeLengths.size());
    segmentSizes.add(headSegLen);
  
    // Now edgesPerSegment and segmentSizes contain the metadata for all the
    // segments that represent this edge list in RAMCloud. Time to pack the
    // edges into ByteBuffers and write them out to the edge image file.

    int neighborListSegOffset = 0;
    ByteBuffer keyLen = ByteBuffer.allocate(Integer.BYTES)
        .order(ByteOrder.LITTLE_ENDIAN);
    ByteBuffer valLen = ByteBuffer.allocate(Integer.BYTES)
        .order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < edgesPerSegment.size(); i++) {
      int edgesInSegment = edgesPerSegment.get(i);
      int segmentSize = segmentSizes.get(i);
      ByteBuffer segment = ByteBuffer.allocate(segmentSize)
          .order(ByteOrder.LITTLE_ENDIAN);
      
      byte[] segKey;
      if (i == edgesPerSegment.size() - 1) {
        // This is the head segment.
        segKey = getSegmentKey(keyPrefix, 0);
        // Special field in head segment for total number of tail segments.
        segment.putInt(edgesPerSegment.size() - 1);
      } else {
        // This is a tail segment.
        segKey = getSegmentKey(keyPrefix, i + 1);
      }

      // Remember that the given edges were prepended, so a given segment
      // actually starts with the edges in the end of the range and finishes
      // with the first edge in the range.
      for (int j = edgesInSegment - 1; j >= 0; j--) {
        UInt128 neighborId = neighborIds.get(neighborListSegOffset + j);
        if (serializedPropList.size() > 0) {
          byte[] serializedProps = 
              serializedPropList.get(neighborListSegOffset + j);
          segment.put(neighborId.toByteArray());
          segment.putShort((short) serializedProps.length);
          segment.put(serializedProps);
        } else {
          segment.put(neighborId.toByteArray());
          segment.putShort((short) 0);
        }
      }

      byte[] segVal = segment.array();

      keyLen.rewind();
      keyLen.putInt(segKey.length);
      valLen.rewind();
      valLen.putInt(segVal.length);

      try {
        edgeListTableOS.write(keyLen.array());
        edgeListTableOS.write(segKey);
        edgeListTableOS.write(valLen.array());
        edgeListTableOS.write(segVal);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      neighborListSegOffset += edgesInSegment;
    }
  }

  /**
   * Reads all of the TorcEdges in the edge list.
   *
   * @return List of all the TorcEdges contained in this edge list.
   */
  public static List<TorcEdge> read(
      RAMCloudTransaction rctx,
      long rcTableId,
      byte[] keyPrefix,
      TorcGraph graph, 
      UInt128 baseVertexId,
      String edgeLabel, 
      Direction direction) {
    List<TorcEdge> edgeList = new ArrayList<>();

    byte[] headSegKey = getSegmentKey(keyPrefix, 0);

    RAMCloudObject headSegObj;
    try {
      headSegObj = rctx.read(rcTableId, headSegKey);
      if (headSegObj == null) {
        return edgeList;
      }
    } catch (ClientException e) {
      throw new RuntimeException(e);
    }

    ByteBuffer headSeg =
        ByteBuffer.allocate(headSegObj.getValueBytes().length)
        .order(ByteOrder.LITTLE_ENDIAN);
    headSeg.put(headSegObj.getValueBytes());
    headSeg.flip();

    int numTailSegments = headSeg.getInt();

    while (headSeg.hasRemaining()) {
      byte[] neighborIdBytes = new byte[UInt128.BYTES];
      headSeg.get(neighborIdBytes);

      UInt128 neighborId = new UInt128(neighborIdBytes);

      short propLen = headSeg.getShort();

      byte[] serializedProperties = new byte[propLen];
      headSeg.get(serializedProperties);

      if (direction == Direction.OUT) {
        edgeList.add(new TorcEdge(graph, baseVertexId, neighborId, edgeLabel, 
              serializedProperties));
      } else if (direction == Direction.IN) {
        edgeList.add(new TorcEdge(graph, neighborId, baseVertexId, edgeLabel, 
              serializedProperties));
      } else {
        throw new IllegalArgumentException("Unsupported direction type: " + direction);
      }
    }

    int mark = 0;
    while (mark < numTailSegments) {
      // Read tail segments asynchronously
      List<RAMCloudTransactionReadOp> tailSegReadOps = new ArrayList<>();
      for (int i = mark; i < numTailSegments; i++) {
        byte[] tailSegKey = getSegmentKey(keyPrefix, numTailSegments - i);
        tailSegReadOps.add(new RAMCloudTransactionReadOp(rctx, rcTableId, tailSegKey, true));
        if (tailSegReadOps.size() == DEFAULT_MAX_ASYNC_READS)
          break;
      }

      for (RAMCloudTransactionReadOp readOp : tailSegReadOps) {
        RAMCloudObject tailSegObj;
        try {
          tailSegObj = readOp.getValue();
        } catch (ClientException e) {
          throw new RuntimeException(e);
        } finally {
          readOp.close();
        }

        if (tailSegObj == null) {
          // Object does not exist.
          continue;
        }

        ByteBuffer tailSeg =
            ByteBuffer.allocate(tailSegObj.getValueBytes().length)
            .order(ByteOrder.LITTLE_ENDIAN);
        tailSeg.put(tailSegObj.getValueBytes());
        tailSeg.flip();

        while (tailSeg.hasRemaining()) {
          byte[] neighborIdBytes = new byte[UInt128.BYTES];
          tailSeg.get(neighborIdBytes);

          UInt128 neighborId = new UInt128(neighborIdBytes);

          short propLen = tailSeg.getShort();

          byte[] serializedProperties = new byte[propLen];
          tailSeg.get(serializedProperties);

          if (direction == Direction.OUT) {
            edgeList.add(new TorcEdge(graph, baseVertexId, neighborId, 
                  edgeLabel, serializedProperties));
          } else if (direction == Direction.IN) {
            edgeList.add(new TorcEdge(graph, neighborId, baseVertexId, 
                  edgeLabel, serializedProperties));
          } else {
            throw new IllegalArgumentException("Unsupported direction type: " + direction);
          }
        }
      }

      mark += tailSegReadOps.size();
    }

    return edgeList;
  }

  /**
   * Reads all of the TorcEdges in the edge list. This version performs the
   * operation outside of any transaction context.
   *
   * @return List of all the TorcEdges contained in this edge list.
   */
  public static List<TorcEdge> read(
      RAMCloud client,
      long rcTableId,
      byte[] keyPrefix,
      TorcGraph graph, 
      UInt128 baseVertexId,
      String edgeLabel, 
      Direction direction) {
    List<TorcEdge> edgeList = new ArrayList<>();

    byte[] headSegKey = getSegmentKey(keyPrefix, 0);

    RAMCloudObject headSegObj;
    try {
      headSegObj = client.read(rcTableId, headSegKey);
      if (headSegObj == null) {
        return edgeList;
      }
    } catch (ClientException e) {
      throw new RuntimeException(e);
    }

    ByteBuffer headSeg =
        ByteBuffer.allocate(headSegObj.getValueBytes().length)
        .order(ByteOrder.LITTLE_ENDIAN);
    headSeg.put(headSegObj.getValueBytes());
    headSeg.flip();

    int numTailSegments = headSeg.getInt();

    while (headSeg.hasRemaining()) {
      byte[] neighborIdBytes = new byte[UInt128.BYTES];
      headSeg.get(neighborIdBytes);

      UInt128 neighborId = new UInt128(neighborIdBytes);

      short propLen = headSeg.getShort();

      byte[] serializedProperties = new byte[propLen];
      headSeg.get(serializedProperties);

      if (direction == Direction.OUT) {
        edgeList.add(new TorcEdge(graph, baseVertexId, neighborId, edgeLabel, 
              serializedProperties));
      } else if (direction == Direction.IN) {
        edgeList.add(new TorcEdge(graph, neighborId, baseVertexId, edgeLabel, 
              serializedProperties));
      } else {
        throw new IllegalArgumentException("Unsupported direction type: " + direction);
      }
    }

    int mark = 0;
    while (mark < numTailSegments) {
      // Read tail segments in a MultiRead
      int batchSize = Math.min(numTailSegments - mark, DEFAULT_MAX_MULTIREAD_SIZE);
      MultiReadObject[] tailSegObjs = new MultiReadObject[batchSize];
      for (int i = 0; i < batchSize; i++) {
        byte[] tailSegKey = getSegmentKey(keyPrefix, numTailSegments - i);
        tailSegObjs[i] = new MultiReadObject(rcTableId, tailSegKey);
      }

      client.read(tailSegObjs);

      for (int i = 0; i < batchSize; i++) {
        if (tailSegObjs[i].getStatus() == Status.STATUS_OBJECT_DOESNT_EXIST) {
          // Object does not exist.
          continue;
        }

        ByteBuffer tailSeg =
            ByteBuffer.allocate(tailSegObjs[i].getValueBytes().length)
            .order(ByteOrder.LITTLE_ENDIAN);
        tailSeg.put(tailSegObjs[i].getValueBytes());
        tailSeg.flip();

        while (tailSeg.hasRemaining()) {
          byte[] neighborIdBytes = new byte[UInt128.BYTES];
          tailSeg.get(neighborIdBytes);

          UInt128 neighborId = new UInt128(neighborIdBytes);

          short propLen = tailSeg.getShort();

          byte[] serializedProperties = new byte[propLen];
          tailSeg.get(serializedProperties);

          if (direction == Direction.OUT) {
            edgeList.add(new TorcEdge(graph, baseVertexId, neighborId, 
                  edgeLabel, serializedProperties));
          } else if (direction == Direction.IN) {
            edgeList.add(new TorcEdge(graph, neighborId, baseVertexId, 
                  edgeLabel, serializedProperties));
          } else {
            throw new IllegalArgumentException("Unsupported direction type: " + direction);
          }
        }
      }

      mark += batchSize;
    }

    return edgeList;
  }

  /**
   * Batch reads in parallel all of the TorcEdges for all the given vertices.
   *
   * @param rctx RAMCloud transaction in which to perform the operation.
   * @param rcTableId The table in which the edge list is (to be) stored.
   * @param keyPrefix List of key prefixes for the edge lists.
   * @param graph TorcGraph to which these edges belong. Used for creating
   * TorcEdge objects.
   * @param baseVertexId List of IDs of the vertices.
   * @param label List of the edge labels.
   * @param direction List of edge directions.
   *
   * @return List of all the TorcEdges contained in the edge lists.
   */
  public static Map<byte[], List<TorcEdge>> batchRead(
      RAMCloudTransaction rctx,
      long rcTableId,
      List<byte[]> keyPrefixes,
      TorcGraph graph, 
      List<UInt128> baseVertexIds,
      List<String> edgeLabels, 
      List<Direction> directions) {
    Map<byte[], LinkedList<RAMCloudTransactionReadOp>> readMap = new HashMap<>();
    Map<byte[], List<TorcEdge>> edgeMap = new HashMap<>();

    /* Async. read head segments. */
    for (byte[] kp : keyPrefixes) {
      LinkedList<RAMCloudTransactionReadOp> readOpList = new LinkedList<>();
      byte[] headSegKey = getSegmentKey(kp, 0);
      readOpList.addLast(new RAMCloudTransactionReadOp(rctx, rcTableId,
            headSegKey, true));
      readMap.put(kp, readOpList);
    }

    /* Process returned head segments and async. read tail segments. */
    for (int i = 0; i < keyPrefixes.size(); i++) {
      byte[] kp = keyPrefixes.get(i);
      UInt128 baseVertexId = baseVertexIds.get(i);
      String edgeLabel = edgeLabels.get(i);
      Direction direction = directions.get(i);

      List<TorcEdge> edgeList = new LinkedList<>();
      edgeMap.put(kp, edgeList);

      LinkedList<RAMCloudTransactionReadOp> readOpList = readMap.get(kp);
      RAMCloudTransactionReadOp readOp = readOpList.removeFirst();
      RAMCloudObject headSegObj;
      try {
        headSegObj = readOp.getValue();
        if (headSegObj == null) {
          continue;
        }
      } catch (ClientException e) {
        throw new RuntimeException(e);
      } finally {
        readOp.close();
      }

      if (headSegObj == null) {
        // Object does not exist.
        continue;
      }

      ByteBuffer headSeg =
          ByteBuffer.allocate(headSegObj.getValueBytes().length)
          .order(ByteOrder.LITTLE_ENDIAN);
      headSeg.put(headSegObj.getValueBytes());
      headSeg.flip();

      int numTailSegments = headSeg.getInt();

      while (headSeg.hasRemaining()) {
        byte[] neighborIdBytes = new byte[UInt128.BYTES];
        headSeg.get(neighborIdBytes);

        UInt128 neighborId = new UInt128(neighborIdBytes);

        short propLen = headSeg.getShort();

        byte[] serializedProperties = new byte[propLen];
        headSeg.get(serializedProperties);

        if (direction == Direction.OUT) {
          edgeList.add(new TorcEdge(graph, baseVertexId, neighborId, edgeLabel, 
                serializedProperties));
        } else if (direction == Direction.IN) {
          edgeList.add(new TorcEdge(graph, neighborId, baseVertexId, edgeLabel, 
                serializedProperties));
        } else {
          throw new IllegalArgumentException("Unsupported direction type: " + direction);
        }
      }

      /* Queue up async. reads for tail segments. */
      for (int j = numTailSegments; j > 0; --j) {
        byte[] tailSegKey = getSegmentKey(kp, j);
        readOpList.addLast(new RAMCloudTransactionReadOp(rctx, rcTableId,
              tailSegKey, true));
      }
    }

    /* Process returned tail segments. */
    for (int i = 0; i < keyPrefixes.size(); i++) {
      byte[] kp = keyPrefixes.get(i);
      UInt128 baseVertexId = baseVertexIds.get(i);
      String edgeLabel = edgeLabels.get(i);
      Direction direction = directions.get(i);

      List<TorcEdge> edgeList = edgeMap.get(kp);

      LinkedList<RAMCloudTransactionReadOp> readOpList = readMap.get(kp);

      while (readOpList.size() > 0) {
        RAMCloudTransactionReadOp readOp = readOpList.removeFirst();
        RAMCloudObject tailSegObj = readOp.getValue();
        readOp.close();

        ByteBuffer tailSeg =
            ByteBuffer.allocate(tailSegObj.getValueBytes().length)
            .order(ByteOrder.LITTLE_ENDIAN);
        tailSeg.put(tailSegObj.getValueBytes());
        tailSeg.flip();

        while (tailSeg.hasRemaining()) {
          byte[] neighborIdBytes = new byte[UInt128.BYTES];
          tailSeg.get(neighborIdBytes);

          UInt128 neighborId = new UInt128(neighborIdBytes);

          short propLen = tailSeg.getShort();

          byte[] serializedProperties = new byte[propLen];
          tailSeg.get(serializedProperties);

          if (direction == Direction.OUT) {
            edgeList.add(new TorcEdge(graph, baseVertexId, neighborId, 
                  edgeLabel, serializedProperties));
          } else if (direction == Direction.IN) {
            edgeList.add(new TorcEdge(graph, neighborId, baseVertexId, 
                  edgeLabel, serializedProperties));
          } else {
            throw new IllegalArgumentException("Unsupported direction type: " + direction);
          }
        }
      }
    }

    return edgeMap;
  }

  /**
   * Batch reads in parallel all of the edges for all the given vertices.
   *
   * @param rctx RAMCloud transaction in which to perform the operation.
   * @param rcTableId The table in which the edge list is (to be) stored.
   * @param keyPrefix List of key prefixes for the edge lists.
   *
   * @return List of all the edges contained in the edge lists.
   */
  public static Map<byte[], List<TorcSerializedEdge>> batchRead(
      RAMCloudTransaction rctx,
      long rcTableId,
      List<byte[]> keyPrefixes) {
    Map<byte[], LinkedList<RAMCloudTransactionReadOp>> readMap = new HashMap<>();
    Map<byte[], List<TorcSerializedEdge>> eListMap = new HashMap<>();

    /* Async. read head segments. */
    for (byte[] kp : keyPrefixes) {
      LinkedList<RAMCloudTransactionReadOp> readOpList = new LinkedList<>();
      byte[] headSegKey = getSegmentKey(kp, 0);
      readOpList.addLast(new RAMCloudTransactionReadOp(rctx, rcTableId,
            headSegKey, true));
      readMap.put(kp, readOpList);
    }

    /* Process returned head segments and async. read tail segments. */
    for (int i = 0; i < keyPrefixes.size(); i++) {
      byte[] kp = keyPrefixes.get(i);

      LinkedList<RAMCloudTransactionReadOp> readOpList = readMap.get(kp);
      RAMCloudTransactionReadOp readOp = readOpList.removeFirst();
      RAMCloudObject headSegObj;
      try {
        headSegObj = readOp.getValue();
        if (headSegObj == null) {
          continue;
        }
      } catch (ClientException e) {
        throw new RuntimeException(e);
      } finally {
        readOp.close();
      }

      if (headSegObj == null) {
        // Object does not exist.
        continue;
      }

      ByteBuffer headSeg =
          ByteBuffer.allocate(headSegObj.getValueBytes().length)
          .order(ByteOrder.LITTLE_ENDIAN);
      headSeg.put(headSegObj.getValueBytes());
      headSeg.flip();

      int numTailSegments = headSeg.getInt();

      if (headSeg.hasRemaining()) {
        List<TorcSerializedEdge> eList = new LinkedList<>();
        eListMap.put(kp, eList);

        while (headSeg.hasRemaining()) {
          byte[] neighborIdBytes = new byte[UInt128.BYTES];
          headSeg.get(neighborIdBytes);

          UInt128 neighborId = new UInt128(neighborIdBytes);

          short propLen = headSeg.getShort();

          byte[] serializedProperties = new byte[propLen];
          headSeg.get(serializedProperties);

          eList.add(new TorcSerializedEdge(serializedProperties, neighborId));
        }
      }

      /* Queue up async. reads for tail segments. */
      for (int j = numTailSegments; j > 0; --j) {
        byte[] tailSegKey = getSegmentKey(kp, j);
        readOpList.addLast(new RAMCloudTransactionReadOp(rctx, rcTableId,
              tailSegKey, true));
      }
    }

    /* Process returned tail segments. */
    for (int i = 0; i < keyPrefixes.size(); i++) {
      byte[] kp = keyPrefixes.get(i);

      LinkedList<RAMCloudTransactionReadOp> readOpList = readMap.get(kp);

      if (readOpList.size() > 0) {
        List<TorcSerializedEdge> eList = eListMap.get(kp);

        while (readOpList.size() > 0) {
          RAMCloudTransactionReadOp readOp = readOpList.removeFirst();
          RAMCloudObject tailSegObj = readOp.getValue();
          readOp.close();

          ByteBuffer tailSeg =
              ByteBuffer.allocate(tailSegObj.getValueBytes().length)
              .order(ByteOrder.LITTLE_ENDIAN);
          tailSeg.put(tailSegObj.getValueBytes());
          tailSeg.flip();

          while (tailSeg.hasRemaining()) {
            byte[] neighborIdBytes = new byte[UInt128.BYTES];
            tailSeg.get(neighborIdBytes);

            UInt128 neighborId = new UInt128(neighborIdBytes);

            short propLen = tailSeg.getShort();

            byte[] serializedProperties = new byte[propLen];
            tailSeg.get(serializedProperties);

            eList.add(new TorcSerializedEdge(serializedProperties, neighborId));
          }
        }
      }
    }

    return eListMap;
  }

  /* Metadata we want to keep track of for MutliReadObjects. */
  private static class MultiReadSpec {
    public byte[] keyPrefix;
    public UInt128 baseVertexId;
    public String edgeLabel;
    public Direction direction;
    public boolean isHeadSeg;

    public MultiReadSpec(byte[] keyPrefix, UInt128 baseVertexId, 
        String edgeLabel, Direction direction, boolean isHeadSeg) {
        this.keyPrefix = keyPrefix;
        this.baseVertexId = baseVertexId;
        this.edgeLabel = edgeLabel;
        this.direction = direction;
        this.isHeadSeg = isHeadSeg;
    }
  }

  /**
   * Batch reads in parallel all of the TorcEdges for all the given vertices.
   * This version performs the operation outside of any transaction context.
   *
   * @param client RAMCloud client to use to perform the operation.
   * @param rcTableId The table in which the edge list is (to be) stored.
   * @param keyPrefix List of key prefixes for the edge lists.
   * @param graph TorcGraph to which these edges belong. Used for creating
   * TorcEdge objects.
   * @param baseVertexId List of IDs of the vertices.
   * @param label List of the edge labels.
   * @param direction List of edge directions.
   *
   * @return List of all the TorcEdges contained in the edge lists.
   */
  public static Map<byte[], List<TorcEdge>> batchRead(
      RAMCloud client,
      long rcTableId,
      List<byte[]> keyPrefixes,
      TorcGraph graph, 
      List<UInt128> baseVertexIds,
      List<String> edgeLabels, 
      List<Direction> directions) {
    LinkedList<MultiReadObject> requestQ = new LinkedList<>();
    LinkedList<MultiReadSpec> specQ = new LinkedList<>();
    Map<byte[], List<TorcEdge>> edgeMap = new HashMap<>();

    /* Add head segments to queue and prepare edgeMap. */
    for (int i = 0; i < keyPrefixes.size(); i++) {
      byte[] headSegKey = getSegmentKey(keyPrefixes.get(i), 0);
      requestQ.addLast(new MultiReadObject(rcTableId, headSegKey));
      specQ.addLast(new MultiReadSpec(keyPrefixes.get(i), 
            baseVertexIds.get(i), edgeLabels.get(i), directions.get(i), true));

      List<TorcEdge> edgeList = new LinkedList<>();
      edgeMap.put(keyPrefixes.get(i), edgeList);
    }

    /* Go through request queue and read at most MAX_ASYNC_READS at a time. */
    while (requestQ.size() > 0) {
      int batchSize = Math.min(requestQ.size(), DEFAULT_MAX_MULTIREAD_SIZE);
      MultiReadObject[] requests = new MultiReadObject[batchSize];
      for (int i = 0; i < batchSize; i++) {
        requests[i] = requestQ.removeFirst();
      }

      client.read(requests);

      /* Process this batch, adding more MultiReadObjects to the queue if
       * needed. */
      for (int i = 0; i < batchSize; i++) {
        MultiReadSpec spec = specQ.removeFirst();

        if (requests[i].getStatus() != Status.STATUS_OK) {
          if (requests[i].getStatus() == Status.STATUS_OBJECT_DOESNT_EXIST) {
            continue;
          } else {
            throw new RuntimeException("Segment had status " + 
                requests[i].getStatus());
          }
        }

        List<TorcEdge> edgeList = edgeMap.get(spec.keyPrefix);

        ByteBuffer seg =
            ByteBuffer.allocate(requests[i].getValueBytes().length)
            .order(ByteOrder.LITTLE_ENDIAN);
        seg.put(requests[i].getValueBytes());
        seg.flip();

        if (spec.isHeadSeg) {
          /* Queue up async. reads for tail segments. */
          int numTailSegments = seg.getInt();
          for (int j = numTailSegments; j > 0; --j) {
            byte[] tailSegKey = getSegmentKey(spec.keyPrefix, j);
            requestQ.addLast(new MultiReadObject(rcTableId, tailSegKey));
            spec.isHeadSeg = false;
            specQ.addLast(spec);
          }
        }

        while (seg.hasRemaining()) {
          byte[] neighborIdBytes = new byte[UInt128.BYTES];
          seg.get(neighborIdBytes);

          UInt128 neighborId = new UInt128(neighborIdBytes);

          short propLen = seg.getShort();

          byte[] serializedProperties = new byte[propLen];
          seg.get(serializedProperties);

          if (spec.direction == Direction.OUT) {
            edgeList.add(new TorcEdge(graph, spec.baseVertexId, 
                  neighborId, spec.edgeLabel, 
                  serializedProperties));
          } else if (spec.direction == Direction.IN) {
            edgeList.add(new TorcEdge(graph, neighborId, 
                  spec.baseVertexId, spec.edgeLabel, 
                  serializedProperties));
          } else {
            throw new IllegalArgumentException("Unsupported direction type: " 
                + spec.direction);
          }
        }
      }
    }

    return edgeMap;
  }

  /**
   * Batch reads in parallel all of the edges for all the given vertices.
   * This version performs the operation outside of any transaction context.
   *
   * @param client RAMCloud client to use to perform the operation.
   * @param rcTableId The table in which the edge list is (to be) stored.
   * @param keyPrefix List of key prefixes for the edge lists.
   *
   * @return List of all the TorcEdges contained in the edge lists.
   */ 
  public static Map<byte[], List<TorcSerializedEdge>> batchRead(
      RAMCloud client,
      long rcTableId,
      List<byte[]> keyPrefixes) {
    LinkedList<MultiReadObject> requestQ = new LinkedList<>();
    LinkedList<MultiReadSpec> specQ = new LinkedList<>();
    Map<byte[], List<TorcSerializedEdge>> eListMap = new HashMap<>();

    /* Add head segments to queue and prepare edgeMap. */
    for (int i = 0; i < keyPrefixes.size(); i++) {
      byte[] headSegKey = getSegmentKey(keyPrefixes.get(i), 0);
      requestQ.addLast(new MultiReadObject(rcTableId, headSegKey));
      specQ.addLast(new MultiReadSpec(keyPrefixes.get(i), 
            null, null, null, true));
    }

    /* Go through request queue and read at most MAX_ASYNC_READS at a time. */
    while (requestQ.size() > 0) {
      int batchSize = Math.min(requestQ.size(), DEFAULT_MAX_MULTIREAD_SIZE);
      MultiReadObject[] requests = new MultiReadObject[batchSize];
      for (int i = 0; i < batchSize; i++) {
        requests[i] = requestQ.removeFirst();
      }

      client.read(requests);

      /* Process this batch, adding more MultiReadObjects to the queue if
       * needed. */
      for (int i = 0; i < batchSize; i++) {
        MultiReadSpec spec = specQ.removeFirst();

        if (requests[i].getStatus() != Status.STATUS_OK) {
          if (requests[i].getStatus() == Status.STATUS_OBJECT_DOESNT_EXIST) {
            continue;
          } else {
            throw new RuntimeException("Segment had status " + 
                requests[i].getStatus());
          }
        }

        List<TorcSerializedEdge> eList;
        if (eListMap.containsKey(spec.keyPrefix)) {
          eList = eListMap.get(spec.keyPrefix);
        } else {
          eList = new LinkedList<>();
          eListMap.put(spec.keyPrefix, eList);
        }

        ByteBuffer seg =
            ByteBuffer.allocate(requests[i].getValueBytes().length)
            .order(ByteOrder.LITTLE_ENDIAN);
        seg.put(requests[i].getValueBytes());
        seg.flip();

        if (spec.isHeadSeg) {
          /* Queue up async. reads for tail segments. */
          int numTailSegments = seg.getInt();
          for (int j = numTailSegments; j > 0; --j) {
            byte[] tailSegKey = getSegmentKey(spec.keyPrefix, j);
            requestQ.addLast(new MultiReadObject(rcTableId, tailSegKey));
            spec.isHeadSeg = false;
            specQ.addLast(spec);
          }
        }

        while (seg.hasRemaining()) {
          byte[] neighborIdBytes = new byte[UInt128.BYTES];
          seg.get(neighborIdBytes);

          UInt128 neighborId = new UInt128(neighborIdBytes);

          short propLen = seg.getShort();

          byte[] serializedProperties = new byte[propLen];
          seg.get(serializedProperties);

          eList.add(new TorcSerializedEdge(serializedProperties, neighborId));
        }
      }
    }

    return eListMap;
  }

  /**
   * Creates a RAMCloud key for the given edge list segment.
   *
   * @param keyPrefix RAMCloud key prefix for this list.
   * @param segmentNumber Number of the segment.
   *
   * @return Byte array representing the RAMCloud key.
   */
  private static byte[] getSegmentKey(byte[] keyPrefix, int segmentNumber) {
    ByteBuffer buffer =
        ByteBuffer.allocate(keyPrefix.length + Integer.BYTES)
        .order(ByteOrder.LITTLE_ENDIAN);
    buffer.put(keyPrefix);
    buffer.putInt(segmentNumber);
    return buffer.array();
  }
}

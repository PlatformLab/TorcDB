/* Copyright (c) 2015-2019 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */
package net.ellitron.torc.util;

import static org.junit.Assert.*;

import org.junit.Test;

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
public class ObjectSerializationTest {

  public ObjectSerializationTest() {
  }

  @Test
  public void serdes_int() {
    Integer in = new Integer(0xCAFEBABE);
    byte[] ser = TorcHelper.serializeObject(in);
    Integer out = (Integer)TorcHelper.deserializeObject(ser);
    assertEquals(in, out);
  }

  @Test
  public void serdes_long() {
    Long in = new Long(0xDEADBEEFCAFEBABEL);
    byte[] ser = TorcHelper.serializeObject(in);
    Long out = (Long)TorcHelper.deserializeObject(ser);
    assertEquals(in, out);
  }

  @Test
  public void serdes_string() {
    String in = new String("Dead beef and cafe babes for the win!");
    byte[] ser = TorcHelper.serializeObject(in);
    String out = (String)TorcHelper.deserializeObject(ser);
    assertEquals(in, out);
  }

  @Test
  public void serdes_list() {
    Integer inInt = new Integer(0xCAFEBABE);
    Long inLong = new Long(0xDEADBEEFCAFEBABEL);
    String inString = new String("Dead beef and cafe babes for the win!");
    List<Object> inList = new ArrayList<>();
    inList.add(inInt);
    inList.add(inLong);
    inList.add(inString);
    List<Object> in = new ArrayList<>();
    in.add(inInt);
    in.add(inLong);
    in.add(inString);
    in.add(inList);
    byte[] ser = TorcHelper.serializeObject(in);
    List<Object> out = (List<Object>)TorcHelper.deserializeObject(ser);
    assertEquals(in, out);
  }

  @Test
  public void serdes_map() {
    Integer val1 = new Integer(0xCAFEBABE);
    Long val2 = new Long(0xDEADBEEFCAFEBABEL);
    String val3 = new String("Dead beef and cafe babes for the win!");
    List<Object> val4 = new ArrayList<>();
    val4.add(val1);
    val4.add(val2);
    val4.add(val3);
    Map<Object, Object> in = new HashMap<>();
    in.put(val1, val2);
    in.put(val2, val3);
    in.put(val3, val4);
    byte[] ser = TorcHelper.serializeObject(in);
    Map<Object, Object> out = (Map<Object, Object>)TorcHelper.deserializeObject(ser);
    assertEquals(in, out);
  }
}

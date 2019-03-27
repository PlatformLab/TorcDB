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

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ArrayMap<K,V> extends AbstractMap<K,V> {
  private Object[] keyArray;
  private Object[] valueArray;

  public ArrayMap() {
    this.keyArray = new Object[1];
    this.valueArray = new Object[1];
  }

  public ArrayMap(int capacity) {
    this.keyArray = new Object[capacity];
    this.valueArray = new Object[capacity];
  }

  @Override
  public Set<Map.Entry<K,V>> entrySet() {
    Set<Map.Entry<K,V>> entrySet = new HashSet<>();
    for (int i = 0; i < keyArray.length; i++) {
      if (keyArray[i] != null);
        entrySet.add(new AbstractMap.SimpleEntry<>((K)keyArray[i], (V)valueArray[i]));
    }

    return entrySet;
  }

  @Override
  public V get(Object key) {
    for (int i = 0; i < keyArray.length; i++) {
      if (keyArray[i] != null && keyArray[i].equals(key))
        return (V)valueArray[i];
    }

    return null;
  }

  @Override
  public V put(K key, V value) {
    int firstFree = -1;
    for (int i = 0; i < keyArray.length; i++) {
      if (keyArray[i] != null && keyArray[i].equals(key)) {
        V oldValue = (V)valueArray[i];
        valueArray[i] = value;
        return oldValue;
      } else if (keyArray[i] == null && firstFree == -1) {
        firstFree = i;
      }
    }
   
    if (firstFree != -1) {
      keyArray[firstFree] = key;
      valueArray[firstFree] = value;
    } else {
      int newLength;
      if (keyArray.length == 1)
        newLength = 8;
      else
        newLength = keyArray.length*2;

      Object[] newKeyArray = new Object[newLength];
      Object[] newValueArray = new Object[newLength];
  
      System.arraycopy(keyArray, 0, newKeyArray, 0, keyArray.length);
      System.arraycopy(valueArray, 0, newValueArray, 0, valueArray.length);

      newKeyArray[keyArray.length] = key;
      newValueArray[valueArray.length] = value;

      keyArray = newKeyArray;
      valueArray = newValueArray;
    }

    return null;
  }
}

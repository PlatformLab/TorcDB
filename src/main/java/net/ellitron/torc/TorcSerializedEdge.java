/*
 * Copyright 2019 Apache Software Foundation.
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

import net.ellitron.torc.util.UInt128;

/**
 * Represents a serialized edge stored in a RAMCloud edge list.
 *
 * @author Jonathan Ellithorpe (jde@cs.stanford.edu)
 */
public class TorcSerializedEdge {
  public byte[] serializedProperties;
  public UInt128 vertexId;

  public TorcSerializedEdge(byte[] serializedProperties, UInt128 vertexId) {
    this.serializedProperties = serializedProperties;
    this.vertexId = vertexId;
  }
}
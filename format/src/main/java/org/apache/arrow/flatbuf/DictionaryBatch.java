/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.flatbuf;

import com.google.flatbuffers.BaseVector;
import com.google.flatbuffers.BooleanVector;
import com.google.flatbuffers.ByteVector;
import com.google.flatbuffers.Constants;
import com.google.flatbuffers.DoubleVector;
import com.google.flatbuffers.FlatBufferBuilder;
import com.google.flatbuffers.FloatVector;
import com.google.flatbuffers.IntVector;
import com.google.flatbuffers.LongVector;
import com.google.flatbuffers.ShortVector;
import com.google.flatbuffers.StringVector;
import com.google.flatbuffers.Struct;
import com.google.flatbuffers.Table;
import com.google.flatbuffers.UnionVector;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * For sending dictionary encoding information. Any Field can be
 * dictionary-encoded, but in this case none of its children may be
 * dictionary-encoded.
 * There is one vector / column per dictionary, but that vector / column
 * may be spread across multiple dictionary batches by using the isDelta
 * flag
 */
@SuppressWarnings("unused")
public final class DictionaryBatch extends Table {
  public static void ValidateVersion() { Constants.FLATBUFFERS_25_2_10(); }
  public static DictionaryBatch getRootAsDictionaryBatch(ByteBuffer _bb) { return getRootAsDictionaryBatch(_bb, new DictionaryBatch()); }
  public static DictionaryBatch getRootAsDictionaryBatch(ByteBuffer _bb, DictionaryBatch obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { __reset(_i, _bb); }
  public DictionaryBatch __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  public long id() { int o = __offset(4); return o != 0 ? bb.getLong(o + bb_pos) : 0L; }
  public org.apache.arrow.flatbuf.RecordBatch data() { return data(new org.apache.arrow.flatbuf.RecordBatch()); }
  public org.apache.arrow.flatbuf.RecordBatch data(org.apache.arrow.flatbuf.RecordBatch obj) { int o = __offset(6); return o != 0 ? obj.__assign(__indirect(o + bb_pos), bb) : null; }
  /**
   * If isDelta is true the values in the dictionary are to be appended to a
   * dictionary with the indicated id. If isDelta is false this dictionary
   * should replace the existing dictionary.
   */
  public boolean isDelta() { int o = __offset(8); return o != 0 ? 0!=bb.get(o + bb_pos) : false; }

  public static int createDictionaryBatch(FlatBufferBuilder builder,
      long id,
      int dataOffset,
      boolean isDelta) {
    builder.startTable(3);
    DictionaryBatch.addId(builder, id);
    DictionaryBatch.addData(builder, dataOffset);
    DictionaryBatch.addIsDelta(builder, isDelta);
    return DictionaryBatch.endDictionaryBatch(builder);
  }

  public static void startDictionaryBatch(FlatBufferBuilder builder) { builder.startTable(3); }
  public static void addId(FlatBufferBuilder builder, long id) { builder.addLong(0, id, 0L); }
  public static void addData(FlatBufferBuilder builder, int dataOffset) { builder.addOffset(1, dataOffset, 0); }
  public static void addIsDelta(FlatBufferBuilder builder, boolean isDelta) { builder.addBoolean(2, isDelta, false); }
  public static int endDictionaryBatch(FlatBufferBuilder builder) {
    int o = builder.endTable();
    return o;
  }

  public static final class Vector extends BaseVector {
    public Vector __assign(int _vector, int _element_size, ByteBuffer _bb) { __reset(_vector, _element_size, _bb); return this; }

    public DictionaryBatch get(int j) { return get(new DictionaryBatch(), j); }
    public DictionaryBatch get(DictionaryBatch obj, int j) {  return obj.__assign(__indirect(__element(j), bb), bb); }
  }
}

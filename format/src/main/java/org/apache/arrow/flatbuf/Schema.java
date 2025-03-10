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
 * ----------------------------------------------------------------------
 * A Schema describes the columns in a row batch
 */
@SuppressWarnings("unused")
public final class Schema extends Table {
  public static void ValidateVersion() { Constants.FLATBUFFERS_25_2_10(); }
  public static Schema getRootAsSchema(ByteBuffer _bb) { return getRootAsSchema(_bb, new Schema()); }
  public static Schema getRootAsSchema(ByteBuffer _bb, Schema obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { __reset(_i, _bb); }
  public Schema __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  /**
   * endianness of the buffer
   * it is Little Endian by default
   * if endianness doesn't match the underlying system then the vectors need to be converted
   */
  public short endianness() { int o = __offset(4); return o != 0 ? bb.getShort(o + bb_pos) : 0; }
  public org.apache.arrow.flatbuf.Field fields(int j) { return fields(new org.apache.arrow.flatbuf.Field(), j); }
  public org.apache.arrow.flatbuf.Field fields(org.apache.arrow.flatbuf.Field obj, int j) { int o = __offset(6); return o != 0 ? obj.__assign(__indirect(__vector(o) + j * 4), bb) : null; }
  public int fieldsLength() { int o = __offset(6); return o != 0 ? __vector_len(o) : 0; }
  public org.apache.arrow.flatbuf.Field.Vector fieldsVector() { return fieldsVector(new org.apache.arrow.flatbuf.Field.Vector()); }
  public org.apache.arrow.flatbuf.Field.Vector fieldsVector(org.apache.arrow.flatbuf.Field.Vector obj) { int o = __offset(6); return o != 0 ? obj.__assign(__vector(o), 4, bb) : null; }
  public org.apache.arrow.flatbuf.KeyValue customMetadata(int j) { return customMetadata(new org.apache.arrow.flatbuf.KeyValue(), j); }
  public org.apache.arrow.flatbuf.KeyValue customMetadata(org.apache.arrow.flatbuf.KeyValue obj, int j) { int o = __offset(8); return o != 0 ? obj.__assign(__indirect(__vector(o) + j * 4), bb) : null; }
  public int customMetadataLength() { int o = __offset(8); return o != 0 ? __vector_len(o) : 0; }
  public org.apache.arrow.flatbuf.KeyValue.Vector customMetadataVector() { return customMetadataVector(new org.apache.arrow.flatbuf.KeyValue.Vector()); }
  public org.apache.arrow.flatbuf.KeyValue.Vector customMetadataVector(org.apache.arrow.flatbuf.KeyValue.Vector obj) { int o = __offset(8); return o != 0 ? obj.__assign(__vector(o), 4, bb) : null; }
  /**
   * Features used in the stream/file.
   */
  public long features(int j) { int o = __offset(10); return o != 0 ? bb.getLong(__vector(o) + j * 8) : 0; }
  public int featuresLength() { int o = __offset(10); return o != 0 ? __vector_len(o) : 0; }
  public LongVector featuresVector() { return featuresVector(new LongVector()); }
  public LongVector featuresVector(LongVector obj) { int o = __offset(10); return o != 0 ? obj.__assign(__vector(o), bb) : null; }
  public ByteBuffer featuresAsByteBuffer() { return __vector_as_bytebuffer(10, 8); }
  public ByteBuffer featuresInByteBuffer(ByteBuffer _bb) { return __vector_in_bytebuffer(_bb, 10, 8); }

  public static int createSchema(FlatBufferBuilder builder,
      short endianness,
      int fieldsOffset,
      int customMetadataOffset,
      int featuresOffset) {
    builder.startTable(4);
    Schema.addFeatures(builder, featuresOffset);
    Schema.addCustomMetadata(builder, customMetadataOffset);
    Schema.addFields(builder, fieldsOffset);
    Schema.addEndianness(builder, endianness);
    return Schema.endSchema(builder);
  }

  public static void startSchema(FlatBufferBuilder builder) { builder.startTable(4); }
  public static void addEndianness(FlatBufferBuilder builder, short endianness) { builder.addShort(0, endianness, 0); }
  public static void addFields(FlatBufferBuilder builder, int fieldsOffset) { builder.addOffset(1, fieldsOffset, 0); }
  public static int createFieldsVector(FlatBufferBuilder builder, int[] data) { builder.startVector(4, data.length, 4); for (int i = data.length - 1; i >= 0; i--) builder.addOffset(data[i]); return builder.endVector(); }
  public static void startFieldsVector(FlatBufferBuilder builder, int numElems) { builder.startVector(4, numElems, 4); }
  public static void addCustomMetadata(FlatBufferBuilder builder, int customMetadataOffset) { builder.addOffset(2, customMetadataOffset, 0); }
  public static int createCustomMetadataVector(FlatBufferBuilder builder, int[] data) { builder.startVector(4, data.length, 4); for (int i = data.length - 1; i >= 0; i--) builder.addOffset(data[i]); return builder.endVector(); }
  public static void startCustomMetadataVector(FlatBufferBuilder builder, int numElems) { builder.startVector(4, numElems, 4); }
  public static void addFeatures(FlatBufferBuilder builder, int featuresOffset) { builder.addOffset(3, featuresOffset, 0); }
  public static int createFeaturesVector(FlatBufferBuilder builder, long[] data) { builder.startVector(8, data.length, 8); for (int i = data.length - 1; i >= 0; i--) builder.addLong(data[i]); return builder.endVector(); }
  public static void startFeaturesVector(FlatBufferBuilder builder, int numElems) { builder.startVector(8, numElems, 8); }
  public static int endSchema(FlatBufferBuilder builder) {
    int o = builder.endTable();
    return o;
  }
  public static void finishSchemaBuffer(FlatBufferBuilder builder, int offset) { builder.finish(offset); }
  public static void finishSizePrefixedSchemaBuffer(FlatBufferBuilder builder, int offset) { builder.finishSizePrefixed(offset); }

  public static final class Vector extends BaseVector {
    public Vector __assign(int _vector, int _element_size, ByteBuffer _bb) { __reset(_vector, _element_size, _bb); return this; }

    public Schema get(int j) { return get(new Schema(), j); }
    public Schema get(Schema obj, int j) {  return obj.__assign(__indirect(__element(j), bb), bb); }
  }
}

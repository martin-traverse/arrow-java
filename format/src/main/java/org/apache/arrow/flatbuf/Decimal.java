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
 * Exact decimal value represented as an integer value in two's
 * complement. Currently only 128-bit (16-byte) and 256-bit (32-byte) integers
 * are used. The representation uses the endianness indicated
 * in the Schema.
 */
@SuppressWarnings("unused")
public final class Decimal extends Table {
  public static void ValidateVersion() { Constants.FLATBUFFERS_25_2_10(); }
  public static Decimal getRootAsDecimal(ByteBuffer _bb) { return getRootAsDecimal(_bb, new Decimal()); }
  public static Decimal getRootAsDecimal(ByteBuffer _bb, Decimal obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { __reset(_i, _bb); }
  public Decimal __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  /**
   * Total number of decimal digits
   */
  public int precision() { int o = __offset(4); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  /**
   * Number of digits after the decimal point "."
   */
  public int scale() { int o = __offset(6); return o != 0 ? bb.getInt(o + bb_pos) : 0; }
  /**
   * Number of bits per value. The only accepted widths are 128 and 256.
   * We use bitWidth for consistency with Int::bitWidth.
   */
  public int bitWidth() { int o = __offset(8); return o != 0 ? bb.getInt(o + bb_pos) : 128; }

  public static int createDecimal(FlatBufferBuilder builder,
      int precision,
      int scale,
      int bitWidth) {
    builder.startTable(3);
    Decimal.addBitWidth(builder, bitWidth);
    Decimal.addScale(builder, scale);
    Decimal.addPrecision(builder, precision);
    return Decimal.endDecimal(builder);
  }

  public static void startDecimal(FlatBufferBuilder builder) { builder.startTable(3); }
  public static void addPrecision(FlatBufferBuilder builder, int precision) { builder.addInt(0, precision, 0); }
  public static void addScale(FlatBufferBuilder builder, int scale) { builder.addInt(1, scale, 0); }
  public static void addBitWidth(FlatBufferBuilder builder, int bitWidth) { builder.addInt(2, bitWidth, 128); }
  public static int endDecimal(FlatBufferBuilder builder) {
    int o = builder.endTable();
    return o;
  }

  public static final class Vector extends BaseVector {
    public Vector __assign(int _vector, int _element_size, ByteBuffer _bb) { __reset(_vector, _element_size, _bb); return this; }

    public Decimal get(int j) { return get(new Decimal(), j); }
    public Decimal get(Decimal obj, int j) {  return obj.__assign(__indirect(__element(j), bb), bb); }
  }
}

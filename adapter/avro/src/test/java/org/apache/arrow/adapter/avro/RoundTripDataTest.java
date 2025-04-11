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
package org.apache.arrow.adapter.avro;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.arrow.adapter.avro.producers.CompositeAvroProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class RoundTripDataTest {

  @TempDir public static File TMP;

  private static AvroToArrowConfig basicConfig(BufferAllocator allocator) {
    return new AvroToArrowConfig(allocator, 1000, null, Collections.emptySet(), true);
  }

  private static VectorSchemaRoot readDataFile(
      Schema schema, File dataFile, BufferAllocator allocator) throws Exception {

    try (FileInputStream fis = new FileInputStream(dataFile)) {
      BinaryDecoder decoder = new DecoderFactory().directBinaryDecoder(fis, null);
      return AvroToArrow.avroToArrow(schema, decoder, basicConfig(allocator));
    }
  }

  private static void roundTripTest(
      VectorSchemaRoot root, BufferAllocator allocator, File dataFile, int rowCount)
      throws Exception {

    // Write an AVRO block using the producer classes
    try (FileOutputStream fos = new FileOutputStream(dataFile)) {
      BinaryEncoder encoder = new EncoderFactory().directBinaryEncoder(fos, null);
      CompositeAvroProducer producer =
          ArrowToAvroUtils.createCompositeProducer(root.getFieldVectors());
      for (int row = 0; row < rowCount; row++) {
        producer.produce(encoder);
      }
      encoder.flush();
    }

    // Generate AVRO schema
    Schema schema = ArrowToAvroUtils.createAvroSchema(root.getSchema().getFields());

    // Read back in and compare
    try (VectorSchemaRoot roundTrip = readDataFile(schema, dataFile, allocator)) {

      assertEquals(root.getSchema(), roundTrip.getSchema());
      assertEquals(rowCount, roundTrip.getRowCount());

      // Read and check values
      for (int row = 0; row < rowCount; row++) {
        assertEquals(root.getVector(0).getObject(row), roundTrip.getVector(0).getObject(row));
      }
    }
  }

  private static void roundTripByteArrayTest(
      VectorSchemaRoot root, BufferAllocator allocator, File dataFile, int rowCount)
      throws Exception {

    // Write an AVRO block using the producer classes
    try (FileOutputStream fos = new FileOutputStream(dataFile)) {
      BinaryEncoder encoder = new EncoderFactory().directBinaryEncoder(fos, null);
      CompositeAvroProducer producer =
          ArrowToAvroUtils.createCompositeProducer(root.getFieldVectors());
      for (int row = 0; row < rowCount; row++) {
        producer.produce(encoder);
      }
      encoder.flush();
    }

    // Generate AVRO schema
    Schema schema = ArrowToAvroUtils.createAvroSchema(root.getSchema().getFields());

    // Read back in and compare
    try (VectorSchemaRoot roundTrip = readDataFile(schema, dataFile, allocator)) {

      assertEquals(root.getSchema(), roundTrip.getSchema());
      assertEquals(rowCount, roundTrip.getRowCount());

      // Read and check values
      for (int row = 0; row < rowCount; row++) {
        byte[] rootBytes = (byte[]) root.getVector(0).getObject(row);
        byte[] roundTripBytes = (byte[]) roundTrip.getVector(0).getObject(row);
        assertArrayEquals(rootBytes, roundTripBytes);
      }
    }
  }

  // Data round trip for primitive types, nullable and non-nullable

  @Test
  public void testRoundTripNullColumn() throws Exception {

    // The current read implementation expects EOF, which never happens for a single null vector
    // Include a boolean vector with this test for now, so that EOF exception will be triggered

    // Field definition
    FieldType nullField = new FieldType(false, new ArrowType.Null(), null);
    FieldType booleanField = new FieldType(false, new ArrowType.Bool(), null);

    // Create empty vector
    BufferAllocator allocator = new RootAllocator();
    NullVector nullVector = new NullVector(new Field("nullColumn", nullField, null));
    BitVector booleanVector = new BitVector(new Field("boolean", booleanField, null), allocator);

    int rowCount = 10;

    // Set up VSR
    List<FieldVector> vectors = Arrays.asList(nullVector, booleanVector);

    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors)) {

      root.setRowCount(rowCount);
      root.allocateNew();

      // Set all values to null
      for (int row = 0; row < rowCount; row++) {
        nullVector.setNull(row);
        booleanVector.set(row, 0);
      }

      File dataFile = new File(TMP, "testRoundTripNullColumn.avro");

      roundTripTest(root, allocator, dataFile, rowCount);
    }
  }

  @Test
  public void testRoundTripBooleans() throws Exception {

    // Field definition
    FieldType booleanField = new FieldType(false, new ArrowType.Bool(), null);

    // Create empty vector
    BufferAllocator allocator = new RootAllocator();
    BitVector booleanVector = new BitVector(new Field("boolean", booleanField, null), allocator);

    // Set up VSR
    List<FieldVector> vectors = Arrays.asList(booleanVector);
    int rowCount = 10;

    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors)) {

      root.setRowCount(rowCount);
      root.allocateNew();

      // Set test data
      for (int row = 0; row < rowCount; row++) {
        booleanVector.set(row, row % 2 == 0 ? 1 : 0);
      }

      File dataFile = new File(TMP, "testRoundTripBooleans.avro");

      roundTripTest(root, allocator, dataFile, rowCount);
    }
  }

  @Test
  public void testRoundTripNullableBooleans() throws Exception {

    // Field definition
    FieldType booleanField = new FieldType(true, new ArrowType.Bool(), null);

    // Create empty vector
    BufferAllocator allocator = new RootAllocator();
    BitVector booleanVector = new BitVector(new Field("boolean", booleanField, null), allocator);

    int rowCount = 3;

    // Set up VSR
    List<FieldVector> vectors = Arrays.asList(booleanVector);

    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors)) {

      root.setRowCount(rowCount);
      root.allocateNew();

      // Null value
      booleanVector.setNull(0);

      // False value
      booleanVector.set(1, 0);

      // True value
      booleanVector.set(2, 1);

      File dataFile = new File(TMP, "testRoundTripNullableBooleans.avro");

      roundTripTest(root, allocator, dataFile, rowCount);
    }
  }

  @Test
  public void testRoundTripIntegers() throws Exception {

    // Field definitions
    FieldType int32Field = new FieldType(false, new ArrowType.Int(32, true), null);
    FieldType int64Field = new FieldType(false, new ArrowType.Int(64, true), null);

    // Create empty vectors
    BufferAllocator allocator = new RootAllocator();
    IntVector int32Vector = new IntVector(new Field("int32", int32Field, null), allocator);
    BigIntVector int64Vector = new BigIntVector(new Field("int64", int64Field, null), allocator);

    // Set up VSR
    List<FieldVector> vectors = Arrays.asList(int32Vector, int64Vector);

    int rowCount = 12;

    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors)) {

      root.setRowCount(rowCount);
      root.allocateNew();

      // Set test data
      for (int row = 0; row < 10; row++) {
        int32Vector.set(row, 513 * row * (row % 2 == 0 ? 1 : -1));
        int64Vector.set(row, 3791L * row * (row % 2 == 0 ? 1 : -1));
      }

      // Min values
      int32Vector.set(10, Integer.MIN_VALUE);
      int64Vector.set(10, Long.MIN_VALUE);

      // Max values
      int32Vector.set(11, Integer.MAX_VALUE);
      int64Vector.set(11, Long.MAX_VALUE);

      File dataFile = new File(TMP, "testRoundTripIntegers.avro");

      roundTripTest(root, allocator, dataFile, rowCount);
    }
  }

  @Test
  public void testRoundTripNullableIntegers() throws Exception {

    // Field definitions
    FieldType int32Field = new FieldType(true, new ArrowType.Int(32, true), null);
    FieldType int64Field = new FieldType(true, new ArrowType.Int(64, true), null);

    // Create empty vectors
    BufferAllocator allocator = new RootAllocator();
    IntVector int32Vector = new IntVector(new Field("int32", int32Field, null), allocator);
    BigIntVector int64Vector = new BigIntVector(new Field("int64", int64Field, null), allocator);

    int rowCount = 3;

    // Set up VSR
    List<FieldVector> vectors = Arrays.asList(int32Vector, int64Vector);

    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors)) {

      root.setRowCount(rowCount);
      root.allocateNew();

      // Null values
      int32Vector.setNull(0);
      int64Vector.setNull(0);

      // Zero values
      int32Vector.set(1, 0);
      int64Vector.set(1, 0);

      // Non-zero values
      int32Vector.set(2, Integer.MAX_VALUE);
      int64Vector.set(2, Long.MAX_VALUE);

      File dataFile = new File(TMP, "testRoundTripNullableIntegers.avro");

      roundTripTest(root, allocator, dataFile, rowCount);
    }
  }

  @Test
  public void testRoundTripFloatingPoints() throws Exception {

    // Field definitions
    FieldType float32Field =
        new FieldType(false, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), null);
    FieldType float64Field =
        new FieldType(false, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), null);

    // Create empty vectors
    BufferAllocator allocator = new RootAllocator();
    Float4Vector float32Vector =
        new Float4Vector(new Field("float32", float32Field, null), allocator);
    Float8Vector float64Vector =
        new Float8Vector(new Field("float64", float64Field, null), allocator);

    // Set up VSR
    List<FieldVector> vectors = Arrays.asList(float32Vector, float64Vector);
    int rowCount = 15;

    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors)) {

      root.setRowCount(rowCount);
      root.allocateNew();

      // Set test data
      for (int row = 0; row < 10; row++) {
        float32Vector.set(row, 37.6f * row * (row % 2 == 0 ? 1 : -1));
        float64Vector.set(row, 37.6d * row * (row % 2 == 0 ? 1 : -1));
      }

      float32Vector.set(10, Float.MIN_VALUE);
      float64Vector.set(10, Double.MIN_VALUE);

      float32Vector.set(11, Float.MAX_VALUE);
      float64Vector.set(11, Double.MAX_VALUE);

      float32Vector.set(12, Float.NaN);
      float64Vector.set(12, Double.NaN);

      float32Vector.set(13, Float.POSITIVE_INFINITY);
      float64Vector.set(13, Double.POSITIVE_INFINITY);

      float32Vector.set(14, Float.NEGATIVE_INFINITY);
      float64Vector.set(14, Double.NEGATIVE_INFINITY);

      File dataFile = new File(TMP, "testRoundTripFloatingPoints.avro");

      roundTripTest(root, allocator, dataFile, rowCount);
    }
  }

  @Test
  public void testRoundTripNullableFloatingPoints() throws Exception {

    // Field definitions
    FieldType float32Field =
        new FieldType(true, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), null);
    FieldType float64Field =
        new FieldType(true, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), null);

    // Create empty vectors
    BufferAllocator allocator = new RootAllocator();
    Float4Vector float32Vector =
        new Float4Vector(new Field("float32", float32Field, null), allocator);
    Float8Vector float64Vector =
        new Float8Vector(new Field("float64", float64Field, null), allocator);

    int rowCount = 3;

    // Set up VSR
    List<FieldVector> vectors = Arrays.asList(float32Vector, float64Vector);

    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors)) {

      root.setRowCount(rowCount);
      root.allocateNew();

      // Null values
      float32Vector.setNull(0);
      float64Vector.setNull(0);

      // Zero values
      float32Vector.set(1, 0.0f);
      float64Vector.set(1, 0.0);

      // Non-zero values
      float32Vector.set(2, 1.0f);
      float64Vector.set(2, 1.0);

      File dataFile = new File(TMP, "testRoundTripNullableFloatingPoints.avro");

      roundTripTest(root, allocator, dataFile, rowCount);
    }
  }

  @Test
  public void testRoundTripStrings() throws Exception {

    // Field definition
    FieldType stringField = new FieldType(false, new ArrowType.Utf8(), null);

    // Create empty vector
    BufferAllocator allocator = new RootAllocator();
    VarCharVector stringVector =
        new VarCharVector(new Field("string", stringField, null), allocator);

    // Set up VSR
    List<FieldVector> vectors = Arrays.asList(stringVector);
    int rowCount = 5;

    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors)) {

      root.setRowCount(rowCount);
      root.allocateNew();

      // Set test data
      stringVector.setSafe(0, "Hello world!".getBytes());
      stringVector.setSafe(1, "<%**\r\n\t\\abc\0$$>".getBytes());
      stringVector.setSafe(2, "你好世界".getBytes());
      stringVector.setSafe(3, "مرحبا بالعالم".getBytes());
      stringVector.setSafe(4, "(P ∧ P ⇒ Q) ⇒ Q".getBytes());

      File dataFile = new File(TMP, "testRoundTripStrings.avro");

      roundTripTest(root, allocator, dataFile, rowCount);
    }
  }

  @Test
  public void testRoundTripNullableStrings() throws Exception {

    // Field definition
    FieldType stringField = new FieldType(true, new ArrowType.Utf8(), null);

    // Create empty vector
    BufferAllocator allocator = new RootAllocator();
    VarCharVector stringVector =
        new VarCharVector(new Field("string", stringField, null), allocator);

    int rowCount = 3;

    // Set up VSR
    List<FieldVector> vectors = Arrays.asList(stringVector);

    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors)) {

      root.setRowCount(rowCount);
      root.allocateNew();

      // Set test data
      stringVector.setNull(0);
      stringVector.setSafe(1, "".getBytes());
      stringVector.setSafe(2, "not empty".getBytes());

      File dataFile = new File(TMP, "testRoundTripNullableStrings.avro");

      roundTripTest(root, allocator, dataFile, rowCount);
    }
  }

  @Test
  public void testRoundTripBinary() throws Exception {

    // Field definition
    FieldType binaryField = new FieldType(false, new ArrowType.Binary(), null);
    FieldType fixedField = new FieldType(false, new ArrowType.FixedSizeBinary(5), null);

    // Create empty vector
    BufferAllocator allocator = new RootAllocator();
    VarBinaryVector binaryVector =
        new VarBinaryVector(new Field("binary", binaryField, null), allocator);
    FixedSizeBinaryVector fixedVector =
        new FixedSizeBinaryVector(new Field("fixed", fixedField, null), allocator);

    // Set up VSR
    List<FieldVector> vectors = Arrays.asList(binaryVector, fixedVector);
    int rowCount = 3;

    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors)) {

      root.setRowCount(rowCount);
      root.allocateNew();

      // Set test data
      binaryVector.setSafe(0, new byte[] {1, 2, 3});
      binaryVector.setSafe(1, new byte[] {4, 5, 6, 7});
      binaryVector.setSafe(2, new byte[] {8, 9});

      fixedVector.setSafe(0, new byte[] {1, 2, 3, 4, 5});
      fixedVector.setSafe(1, new byte[] {4, 5, 6, 7, 8, 9});
      fixedVector.setSafe(2, new byte[] {8, 9, 10, 11, 12});

      File dataFile = new File(TMP, "testRoundTripBinary.avro");

      roundTripByteArrayTest(root, allocator, dataFile, rowCount);
    }
  }

  @Test
  public void testRoundTripNullableBinary() throws Exception {

    // Field definition
    FieldType binaryField = new FieldType(true, new ArrowType.Binary(), null);
    FieldType fixedField = new FieldType(true, new ArrowType.FixedSizeBinary(5), null);

    // Create empty vector
    BufferAllocator allocator = new RootAllocator();
    VarBinaryVector binaryVector =
        new VarBinaryVector(new Field("binary", binaryField, null), allocator);
    FixedSizeBinaryVector fixedVector =
        new FixedSizeBinaryVector(new Field("fixed", fixedField, null), allocator);

    int rowCount = 3;

    // Set up VSR
    List<FieldVector> vectors = Arrays.asList(binaryVector, fixedVector);

    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors)) {

      root.setRowCount(rowCount);
      root.allocateNew();

      // Set test data
      binaryVector.setNull(0);
      binaryVector.setSafe(1, new byte[] {});
      binaryVector.setSafe(2, new byte[] {10, 11, 12});

      fixedVector.setNull(0);
      fixedVector.setSafe(1, new byte[] {0, 0, 0, 0, 0});
      fixedVector.setSafe(2, new byte[] {10, 11, 12, 13, 14});

      File dataFile = new File(TMP, "testRoundTripNullableBinary.avro");

      roundTripByteArrayTest(root, allocator, dataFile, rowCount);
    }
  }
}

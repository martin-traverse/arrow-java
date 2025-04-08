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

import org.apache.arrow.adapter.avro.producers.CompositeAvroProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.avro.Schema;
import org.apache.avro.io.*;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RoundTripDataTest {

  @TempDir
  public static File TMP;

  private static AvroToArrowConfig basicConfig(BufferAllocator allocator) {
    return new AvroToArrowConfig(allocator, 1000, null, Collections.emptySet(), true);
  }

  private static VectorSchemaRoot readDataFile(
      Schema schema, File dataFile, BufferAllocator allocator)
      throws IOException {

    try (FileInputStream fis = new FileInputStream(dataFile)) {
      BinaryDecoder decoder = new DecoderFactory().directBinaryDecoder(fis, null);
      return AvroToArrow.avroToArrow(schema, decoder, basicConfig(allocator));
    }
  }

  @Test
  @Disabled
  public void testRoundTripNullColumn() throws Exception {

    // TODO: Raw block with single null field has no size, will read forever

    // Field definition
    FieldType nullField = new FieldType(false, new ArrowType.Null(), null);

    // Create empty vector
    BufferAllocator allocator = new RootAllocator();
    NullVector nullVector = new NullVector(new Field("nullColumn", nullField, null));

    int rowCount = 10;

    // Set up VSR
    List<FieldVector> vectors = Arrays.asList(nullVector);

    try (VectorSchemaRoot root = new VectorSchemaRoot(vectors)) {

      root.setRowCount(rowCount);
      root.allocateNew();

      // Set all values to null
      for (int row = 0; row < rowCount; row++) {
        nullVector.setNull(row);
      }

      File dataFile = new File(TMP, "testWriteNullColumn.avro");

      // Write an AVRO block using the producer classes
      try (FileOutputStream fos = new FileOutputStream(dataFile)) {
        BinaryEncoder encoder = new EncoderFactory().directBinaryEncoder(fos, null);
        CompositeAvroProducer producer = ArrowToAvroUtils.createCompositeProducer(vectors);
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

      // Write an AVRO block using the producer classes
      try (FileOutputStream fos = new FileOutputStream(dataFile)) {
        BinaryEncoder encoder = new EncoderFactory().directBinaryEncoder(fos, null);
        CompositeAvroProducer producer = ArrowToAvroUtils.createCompositeProducer(vectors);
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

      // Write an AVRO block using the producer classes
      try (FileOutputStream fos = new FileOutputStream(dataFile)) {
        BinaryEncoder encoder = new EncoderFactory().directBinaryEncoder(fos, null);
        CompositeAvroProducer producer = ArrowToAvroUtils.createCompositeProducer(vectors);
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
  }
}

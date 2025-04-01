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

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RoundTripSchemaTest {

  // Schema round trip for primitive types, nullable and non-nullable

  @Test
  public void testRoundTripNullType() {

    AvroToArrowConfig config = new AvroToArrowConfig(null, 1, null, Collections.emptySet(), true);

    List<Field> fields =
        Arrays.asList(new Field("nullType", FieldType.notNullable(new ArrowType.Null()), null));

    Schema avroSchema = ArrowToAvroUtils.createAvroSchema(fields, "TestRecord");
    org.apache.arrow.vector.types.pojo.Schema arrowSchema = AvroToArrowUtils.createArrowSchema(avroSchema, config);

    // Exact match on fields after round trip
    assertEquals(fields, arrowSchema.getFields());
  }

  @Test
  public void testRoundTripBooleanType() {

    AvroToArrowConfig config = new AvroToArrowConfig(null, 1, null, Collections.emptySet(), true);

    List<Field> fields =
        Arrays.asList(
            new Field("nullableBool", FieldType.nullable(new ArrowType.Bool()), null),
            new Field("nonNullableBool", FieldType.notNullable(new ArrowType.Bool()), null));

    Schema avroSchema = ArrowToAvroUtils.createAvroSchema(fields, "TestRecord");
    org.apache.arrow.vector.types.pojo.Schema arrowSchema = AvroToArrowUtils.createArrowSchema(avroSchema, config);

    // Exact match on fields after round trip
    assertEquals(fields, arrowSchema.getFields());
  }

  @Test
  public void testRoundTripIntegerTypes() {

    AvroToArrowConfig config = new AvroToArrowConfig(null, 1, null, Collections.emptySet(), true);

    // Only round trip types with direct equivalent in Avro

    List<Field> fields =
        Arrays.asList(
            new Field("nullableInt32", FieldType.nullable(new ArrowType.Int(32, true)), null),
            new Field("nonNullableInt32", FieldType.notNullable(new ArrowType.Int(32, true)), null),
            new Field("nullableInt64", FieldType.nullable(new ArrowType.Int(64, true)), null),
            new Field("nonNullableInt64", FieldType.notNullable(new ArrowType.Int(64, true)), null));

    Schema avroSchema = ArrowToAvroUtils.createAvroSchema(fields, "TestRecord");
    org.apache.arrow.vector.types.pojo.Schema arrowSchema = AvroToArrowUtils.createArrowSchema(avroSchema, config);

    // Exact match on fields after round trip
    assertEquals(fields, arrowSchema.getFields());
  }

  @Test
  public void testRoundTripFloatingPointTypes() {

    AvroToArrowConfig config = new AvroToArrowConfig(null, 1, null, Collections.emptySet(), true);

    // Only round trip types with direct equivalent in Avro

    List<Field> fields =
        Arrays.asList(
            new Field(
                "nullableFloat32",
                FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
                null),
            new Field(
                "nonNullableFloat32",
                FieldType.notNullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
                null),
            new Field(
                "nullableFloat64",
                FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                null),
            new Field(
                "nonNullableFloat64",
                FieldType.notNullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                null));

    Schema avroSchema = ArrowToAvroUtils.createAvroSchema(fields, "TestRecord");
    org.apache.arrow.vector.types.pojo.Schema arrowSchema = AvroToArrowUtils.createArrowSchema(avroSchema, config);

    // Exact match on fields after round trip
    assertEquals(fields, arrowSchema.getFields());
  }

  @Test
  public void testRoundTripStringTypes() {

    AvroToArrowConfig config = new AvroToArrowConfig(null, 1, null, Collections.emptySet(), true);

    List<Field> fields =
        Arrays.asList(
            new Field("nullableUtf8", FieldType.nullable(new ArrowType.Utf8()), null),
            new Field("nonNullableUtf8", FieldType.notNullable(new ArrowType.Utf8()), null));

    Schema avroSchema = ArrowToAvroUtils.createAvroSchema(fields, "TestRecord");
    org.apache.arrow.vector.types.pojo.Schema arrowSchema = AvroToArrowUtils.createArrowSchema(avroSchema, config);

    // Exact match on fields after round trip
    assertEquals(fields, arrowSchema.getFields());
  }

  @Test
  public void testRoundTripBinaryTypes() {

    AvroToArrowConfig config = new AvroToArrowConfig(null, 1, null, Collections.emptySet(), true);

    List<Field> fields =
        Arrays.asList(
            new Field("nullableBinary", FieldType.nullable(new ArrowType.Binary()), null),
            new Field("nonNullableBinary", FieldType.notNullable(new ArrowType.Binary()), null));

    Schema avroSchema = ArrowToAvroUtils.createAvroSchema(fields, "TestRecord");
    org.apache.arrow.vector.types.pojo.Schema arrowSchema = AvroToArrowUtils.createArrowSchema(avroSchema, config);

    // Exact match on fields after round trip
    assertEquals(fields, arrowSchema.getFields());
  }

  @Test
  public void testRoundTripFixedSizeBinaryTypes() {

    AvroToArrowConfig config = new AvroToArrowConfig(null, 1, null, Collections.emptySet(), true);

    List<Field> fields =
        Arrays.asList(
            new Field(
                "nullableFixedSizeBinary",
                FieldType.nullable(new ArrowType.FixedSizeBinary(10)),
                null),
            new Field(
                "nonNullableFixedSizeBinary",
                FieldType.notNullable(new ArrowType.FixedSizeBinary(10)),
                null));

    Schema avroSchema = ArrowToAvroUtils.createAvroSchema(fields, "TestRecord");
    org.apache.arrow.vector.types.pojo.Schema arrowSchema = AvroToArrowUtils.createArrowSchema(avroSchema, config);

    // Exact match on fields after round trip
    assertEquals(fields, arrowSchema.getFields());
  }
}

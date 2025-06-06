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
package org.apache.arrow.c;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.table.Table;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Functions for working with the C data interface.
 *
 * <p>This API is EXPERIMENTAL. Note that currently only 64bit systems are supported.
 */
public final class Data {

  private Data() {}

  /**
   * Export Java Field using the C data interface format.
   *
   * @param allocator Buffer allocator for allocating C data interface fields
   * @param field Field object to export
   * @param provider Dictionary provider for dictionary encoded fields (optional)
   * @param out C struct where to export the field
   */
  public static void exportField(
      BufferAllocator allocator, Field field, DictionaryProvider provider, ArrowSchema out) {
    SchemaExporter exporter = new SchemaExporter(allocator);
    exporter.export(out, field, provider);
  }

  /**
   * Export Java Schema using the C data interface format.
   *
   * @param allocator Buffer allocator for allocating C data interface fields
   * @param schema Schema object to export
   * @param provider Dictionary provider for dictionary encoded fields (optional)
   * @param out C struct where to export the field
   */
  public static void exportSchema(
      BufferAllocator allocator, Schema schema, DictionaryProvider provider, ArrowSchema out) {
    // Convert to a struct field equivalent to the input schema
    FieldType fieldType =
        new FieldType(false, new ArrowType.Struct(), null, schema.getCustomMetadata());
    Field field = new Field("", fieldType, schema.getFields());
    exportField(allocator, field, provider, out);
  }

  /**
   * Export Java FieldVector using the C data interface format.
   *
   * <p>The resulting ArrowArray struct keeps the array data and buffers alive until its release
   * callback is called by the consumer.
   *
   * @param allocator Buffer allocator for allocating C data interface fields
   * @param vector Vector object to export
   * @param provider Dictionary provider for dictionary encoded vectors (optional)
   * @param out C struct where to export the array
   */
  public static void exportVector(
      BufferAllocator allocator, FieldVector vector, DictionaryProvider provider, ArrowArray out) {
    exportVector(allocator, vector, provider, out, null);
  }

  /**
   * Export Java FieldVector using the C data interface format.
   *
   * <p>The resulting ArrowArray struct keeps the array data and buffers alive until its release
   * callback is called by the consumer.
   *
   * @param allocator Buffer allocator for allocating C data interface fields
   * @param vector Vector object to export
   * @param provider Dictionary provider for dictionary encoded vectors (optional)
   * @param out C struct where to export the array
   * @param outSchema C struct where to export the array type (optional)
   */
  public static void exportVector(
      BufferAllocator allocator,
      FieldVector vector,
      DictionaryProvider provider,
      ArrowArray out,
      ArrowSchema outSchema) {
    if (outSchema != null) {
      exportField(allocator, vector.getField(), provider, outSchema);
    }

    ArrayExporter exporter = new ArrayExporter(allocator);
    exporter.export(out, vector, provider);
  }

  /**
   * Export the current contents of a Java Table using the C data interface format.
   *
   * <p>The table is exported as if it were a struct array. The resulting ArrowArray struct keeps
   * the record batch data and buffers alive until its release callback is called by the consumer.
   *
   * @param allocator Buffer allocator for allocating C data interface fields
   * @param table Table to export
   * @param out C struct where to export the record batch
   */
  public static void exportTable(BufferAllocator allocator, Table table, ArrowArray out) {
    exportTable(allocator, table, table.getDictionaryProvider(), out, null);
  }

  /**
   * Export the current contents of a Java Table using the C data interface format.
   *
   * <p>The table is exported as if it were a struct array. The resulting ArrowArray struct keeps
   * the record batch data and buffers alive until its release callback is called by the consumer.
   *
   * @param allocator Buffer allocator for allocating C data interface fields
   * @param table Table to export
   * @param provider Dictionary provider for dictionary encoded vectors (optional)
   * @param out C struct where to export the record batch
   */
  public static void exportTable(
      BufferAllocator allocator, Table table, DictionaryProvider provider, ArrowArray out) {
    exportTable(allocator, table, provider, out, null);
  }

  /**
   * Export the current contents of a Java Table using the C data interface format.
   *
   * <p>The table is exported as if it were a struct array. The resulting ArrowArray struct keeps
   * the record batch data and buffers alive until its release callback is called by the consumer.
   *
   * @param allocator Buffer allocator for allocating C data interface fields
   * @param table Table to export
   * @param provider Dictionary provider for dictionary encoded vectors (optional)
   * @param out C struct where to export the record batch
   * @param outSchema C struct where to export the record batch schema (optional)
   */
  public static void exportTable(
      BufferAllocator allocator,
      Table table,
      DictionaryProvider provider,
      ArrowArray out,
      ArrowSchema outSchema) {
    try (VectorSchemaRoot root = table.toVectorSchemaRoot()) {
      exportVectorSchemaRoot(allocator, root, provider, out, outSchema);
    }
  }

  /**
   * Export the current contents of a Java VectorSchemaRoot using the C data interface format.
   *
   * <p>The vector schema root is exported as if it were a struct array. The resulting ArrowArray
   * struct keeps the record batch data and buffers alive until its release callback is called by
   * the consumer.
   *
   * @param allocator Buffer allocator for allocating C data interface fields
   * @param vsr Vector schema root to export
   * @param provider Dictionary provider for dictionary encoded vectors (optional)
   * @param out C struct where to export the record batch
   */
  public static void exportVectorSchemaRoot(
      BufferAllocator allocator,
      VectorSchemaRoot vsr,
      DictionaryProvider provider,
      ArrowArray out) {
    exportVectorSchemaRoot(allocator, vsr, provider, out, null);
  }

  /**
   * Export the current contents of a Java VectorSchemaRoot using the C data interface format.
   *
   * <p>The vector schema root is exported as if it were a struct array. The resulting ArrowArray
   * struct keeps the record batch data and buffers alive until its release callback is called by
   * the consumer.
   *
   * @param allocator Buffer allocator for allocating C data interface fields
   * @param vsr Vector schema root to export
   * @param provider Dictionary provider for dictionary encoded vectors (optional)
   * @param out C struct where to export the record batch
   * @param outSchema C struct where to export the record batch schema (optional)
   */
  public static void exportVectorSchemaRoot(
      BufferAllocator allocator,
      VectorSchemaRoot vsr,
      DictionaryProvider provider,
      ArrowArray out,
      ArrowSchema outSchema) {
    if (outSchema != null) {
      exportSchema(allocator, vsr.getSchema(), provider, outSchema);
    }

    VectorUnloader unloader = new VectorUnloader(vsr);
    try (ArrowRecordBatch recordBatch = unloader.getRecordBatch()) {
      StructVectorLoader loader = new StructVectorLoader(vsr.getSchema());
      try (StructVector vector = loader.load(allocator, recordBatch)) {
        exportVector(allocator, vector, provider, out);
      }
    }
  }

  /**
   * Export a reader as an ArrowArrayStream using the C Stream Interface.
   *
   * @param allocator Buffer allocator for allocating C data interface fields
   * @param reader Reader to export
   * @param out C struct to export the stream
   */
  public static void exportArrayStream(
      BufferAllocator allocator, ArrowReader reader, ArrowArrayStream out) {
    new ArrayStreamExporter(allocator).export(out, reader);
  }

  /**
   * Equivalent to calling {@link #importField(BufferAllocator, ArrowSchema,
   * CDataDictionaryProvider, boolean) importField(allocator, schema, provider, true)}.
   *
   * @param allocator Buffer allocator for allocating dictionary vectors
   * @param schema C data interface struct representing the field [inout]
   * @param provider A dictionary provider will be initialized with empty dictionary vectors
   *     (optional)
   * @return Imported field object
   * @see #importField(BufferAllocator, ArrowSchema, CDataDictionaryProvider, boolean)
   */
  public static Field importField(
      BufferAllocator allocator, ArrowSchema schema, CDataDictionaryProvider provider) {
    return importField(allocator, schema, provider, true);
  }

  /**
   * Import Java Field from the C data interface.
   *
   * <p>The given ArrowSchema struct is released (as per the C data interface specification), even
   * if this function fails.
   *
   * @param allocator Buffer allocator for allocating dictionary vectors
   * @param schema C data interface struct representing the field [inout]
   * @param provider A dictionary provider will be initialized with empty dictionary vectors
   *     (optional)
   * @param closeImportedStructs if true, the ArrowSchema struct will be closed when this method
   *     completes.
   * @return Imported field object
   */
  public static Field importField(
      BufferAllocator allocator,
      ArrowSchema schema,
      CDataDictionaryProvider provider,
      boolean closeImportedStructs) {
    try {
      SchemaImporter importer = new SchemaImporter(allocator);
      return importer.importField(schema, provider);
    } finally {
      schema.release();
      if (closeImportedStructs) {
        schema.close();
      }
    }
  }

  /**
   * Equivalent to calling {@link #importSchema(BufferAllocator, ArrowSchema,
   * CDataDictionaryProvider, boolean) importSchema(allocator, schema, provider, true)}.
   *
   * @param allocator Buffer allocator for allocating dictionary vectors
   * @param schema C data interface struct representing the field
   * @param provider A dictionary provider will be initialized with empty dictionary vectors
   *     (optional)
   * @return Imported schema object
   * @see #importSchema(BufferAllocator, ArrowSchema, CDataDictionaryProvider, boolean)
   */
  public static Schema importSchema(
      BufferAllocator allocator, ArrowSchema schema, CDataDictionaryProvider provider) {
    return importSchema(allocator, schema, provider, true);
  }

  /**
   * Import Java Schema from the C data interface.
   *
   * <p>The given ArrowSchema struct is released (as per the C data interface specification), even
   * if this function fails.
   *
   * @param allocator Buffer allocator for allocating dictionary vectors
   * @param schema C data interface struct representing the field
   * @param provider A dictionary provider will be initialized with empty dictionary vectors
   *     (optional)
   * @param closeImportedStructs if true, the ArrowSchema struct will be closed when this method
   *     completes.
   * @return Imported schema object
   */
  public static Schema importSchema(
      BufferAllocator allocator,
      ArrowSchema schema,
      CDataDictionaryProvider provider,
      boolean closeImportedStructs) {
    Field structField = importField(allocator, schema, provider, closeImportedStructs);
    if (structField.getType().getTypeID() != ArrowTypeID.Struct) {
      throw new IllegalArgumentException(
          "Cannot import schema: ArrowSchema describes non-struct type");
    }
    return new Schema(structField.getChildren(), structField.getMetadata());
  }

  /**
   * Equivalent to calling {@link #importIntoVector(BufferAllocator, ArrowArray, FieldVector,
   * DictionaryProvider, boolean)} importIntoVector(allocator, array, vector, provider, true)}.
   *
   * @param allocator Buffer allocator
   * @param array C data interface struct holding the array data
   * @param vector Imported vector object [out]
   * @param provider Dictionary provider to load dictionary vectors to (optional)
   * @see #importIntoVector(BufferAllocator, ArrowArray, FieldVector, DictionaryProvider, boolean)
   */
  public static void importIntoVector(
      BufferAllocator allocator,
      ArrowArray array,
      FieldVector vector,
      DictionaryProvider provider) {
    importIntoVector(allocator, array, vector, provider, true);
  }

  /**
   * Import Java vector from the C data interface.
   *
   * <p>On successful completion, the ArrowArray struct will have been moved (as per the C data
   * interface specification) to a private object held alive by the resulting array.
   *
   * @param allocator Buffer allocator
   * @param array C data interface struct holding the array data
   * @param vector Imported vector object [out]
   * @param provider Dictionary provider to load dictionary vectors to (optional)
   * @param closeImportedStructs if true, the ArrowArray struct will be closed when this method
   *     completes successfully.
   */
  public static void importIntoVector(
      BufferAllocator allocator,
      ArrowArray array,
      FieldVector vector,
      DictionaryProvider provider,
      boolean closeImportedStructs) {
    ArrayImporter importer = new ArrayImporter(allocator, vector, provider);
    importer.importArray(array);
    if (closeImportedStructs) {
      array.close();
    }
  }

  /**
   * Equivalent to calling {@link #importVector(BufferAllocator, ArrowArray, ArrowSchema,
   * CDataDictionaryProvider, boolean) importVector(allocator, array, schema, provider, true)}.
   *
   * @param allocator Buffer allocator for allocating the output FieldVector
   * @param array C data interface struct holding the array data
   * @param schema C data interface struct holding the array type
   * @param provider Dictionary provider to load dictionary vectors to (optional)
   * @return Imported vector object
   * @see #importVector(BufferAllocator, ArrowArray, ArrowSchema, CDataDictionaryProvider, boolean)
   */
  public static FieldVector importVector(
      BufferAllocator allocator,
      ArrowArray array,
      ArrowSchema schema,
      CDataDictionaryProvider provider) {
    return importVector(allocator, array, schema, provider, true);
  }

  /**
   * Import Java vector and its type from the C data interface.
   *
   * <p>The ArrowArray struct has its contents moved (as per the C data interface specification) to
   * a private object held alive by the resulting vector. The ArrowSchema struct is released, even
   * if this function fails.
   *
   * @param allocator Buffer allocator for allocating the output FieldVector
   * @param array C data interface struct holding the array data
   * @param schema C data interface struct holding the array type
   * @param provider Dictionary provider to load dictionary vectors to (optional)
   * @param closeImportedStructs if true, the ArrowArray struct will be closed when this method
   *     completes successfully and the ArrowSchema struct will be always be closed.
   * @return Imported vector object
   */
  public static FieldVector importVector(
      BufferAllocator allocator,
      ArrowArray array,
      ArrowSchema schema,
      CDataDictionaryProvider provider,
      boolean closeImportedStructs) {
    Field field = importField(allocator, schema, provider, closeImportedStructs);
    FieldVector vector = field.createVector(allocator);
    importIntoVector(allocator, array, vector, provider, closeImportedStructs);
    return vector;
  }

  /**
   * Equivalent to calling {@link #importIntoVectorSchemaRoot(BufferAllocator, ArrowArray,
   * VectorSchemaRoot, DictionaryProvider, boolean) importIntoVectorSchemaRoot(allocator, array,
   * root, provider, true)}.
   *
   * @param allocator Buffer allocator
   * @param array C data interface struct holding the record batch data
   * @param root vector schema root to load into
   * @param provider Dictionary provider to load dictionary vectors to (optional)
   * @see #importIntoVectorSchemaRoot(BufferAllocator, ArrowArray, VectorSchemaRoot,
   *     DictionaryProvider, boolean)
   */
  public static void importIntoVectorSchemaRoot(
      BufferAllocator allocator,
      ArrowArray array,
      VectorSchemaRoot root,
      DictionaryProvider provider) {
    importIntoVectorSchemaRoot(allocator, array, root, provider, true);
  }

  /**
   * Import record batch from the C data interface into vector schema root.
   *
   * <p>The ArrowArray struct has its contents moved (as per the C data interface specification) to
   * a private object held alive by the resulting vector schema root.
   *
   * <p>The schema of the vector schema root must match the input array (undefined behavior
   * otherwise).
   *
   * @param allocator Buffer allocator
   * @param array C data interface struct holding the record batch data
   * @param root vector schema root to load into
   * @param provider Dictionary provider to load dictionary vectors to (optional)
   * @param closeImportedStructs if true, the ArrowArray struct will be closed when this method
   *     completes successfully
   */
  public static void importIntoVectorSchemaRoot(
      BufferAllocator allocator,
      ArrowArray array,
      VectorSchemaRoot root,
      DictionaryProvider provider,
      boolean closeImportedStructs) {
    try (StructVector structVector = StructVector.emptyWithDuplicates("", allocator)) {
      structVector.initializeChildrenFromFields(root.getSchema().getFields());
      importIntoVector(allocator, array, structVector, provider, closeImportedStructs);
      StructVectorUnloader unloader = new StructVectorUnloader(structVector);
      VectorLoader loader = new VectorLoader(root);
      try (ArrowRecordBatch recordBatch = unloader.getRecordBatch()) {
        loader.load(recordBatch);
      }
    }
  }

  /**
   * Equivalent to calling {@link #importVectorSchemaRoot(BufferAllocator, ArrowSchema,
   * CDataDictionaryProvider, boolean) importVectorSchemaRoot(allocator, schema, provider, true)}.
   *
   * @param allocator Buffer allocator for allocating the output VectorSchemaRoot
   * @param schema C data interface struct holding the record batch schema
   * @param provider Dictionary provider to load dictionary vectors to (optional)
   * @return Imported vector schema root
   * @see #importVectorSchemaRoot(BufferAllocator, ArrowSchema, CDataDictionaryProvider, boolean)
   */
  public static VectorSchemaRoot importVectorSchemaRoot(
      BufferAllocator allocator, ArrowSchema schema, CDataDictionaryProvider provider) {
    return importVectorSchemaRoot(allocator, schema, provider, true);
  }

  /**
   * Import Java vector schema root from a C data interface Schema.
   *
   * <p>The type represented by the ArrowSchema struct must be a struct type array.
   *
   * <p>The ArrowSchema struct is released, even if this function fails.
   *
   * @param allocator Buffer allocator for allocating the output VectorSchemaRoot
   * @param schema C data interface struct holding the record batch schema
   * @param provider Dictionary provider to load dictionary vectors to (optional)
   * @param closeImportedStructs if true, the ArrowSchema struct will be closed when this method
   *     completes
   * @return Imported vector schema root
   */
  public static VectorSchemaRoot importVectorSchemaRoot(
      BufferAllocator allocator,
      ArrowSchema schema,
      CDataDictionaryProvider provider,
      boolean closeImportedStructs) {
    return importVectorSchemaRoot(allocator, null, schema, provider, closeImportedStructs);
  }

  /**
   * Equivalent to calling {@link #importVectorSchemaRoot(BufferAllocator, ArrowArray, ArrowSchema,
   * CDataDictionaryProvider, boolean) importVectorSchemaRoot(allocator, array, schema, provider,
   * true)}.
   *
   * @param allocator Buffer allocator for allocating the output VectorSchemaRoot
   * @param array C data interface struct holding the record batch data (optional)
   * @param schema C data interface struct holding the record batch schema
   * @param provider Dictionary provider to load dictionary vectors to (optional)
   * @return Imported vector schema root
   * @see #importVectorSchemaRoot(BufferAllocator, ArrowArray, ArrowSchema, CDataDictionaryProvider,
   *     boolean)
   */
  public static VectorSchemaRoot importVectorSchemaRoot(
      BufferAllocator allocator,
      ArrowArray array,
      ArrowSchema schema,
      CDataDictionaryProvider provider) {
    return importVectorSchemaRoot(allocator, array, schema, provider, true);
  }

  /**
   * Import Java vector schema root from the C data interface.
   *
   * <p>The type represented by the ArrowSchema struct must be a struct type array.
   *
   * <p>The ArrowArray struct has its contents moved (as per the C data interface specification) to
   * a private object held alive by the resulting record batch. The ArrowSchema struct is released,
   * even if this function fails.
   *
   * <p>Prefer {@link #importIntoVectorSchemaRoot} for loading array data while reusing the same
   * vector schema root.
   *
   * @param allocator Buffer allocator for allocating the output VectorSchemaRoot
   * @param array C data interface struct holding the record batch data (optional)
   * @param schema C data interface struct holding the record batch schema
   * @param provider Dictionary provider to load dictionary vectors to (optional)
   * @param closeImportedStructs if true, the ArrowArray struct will be closed when this method
   *     completes successfully and the ArrowSchema struct will be always be closed.
   * @return Imported vector schema root
   */
  public static VectorSchemaRoot importVectorSchemaRoot(
      BufferAllocator allocator,
      ArrowArray array,
      ArrowSchema schema,
      CDataDictionaryProvider provider,
      boolean closeImportedStructs) {
    VectorSchemaRoot vsr =
        VectorSchemaRoot.create(
            importSchema(allocator, schema, provider, closeImportedStructs), allocator);
    if (array != null) {
      importIntoVectorSchemaRoot(allocator, array, vsr, provider, closeImportedStructs);
    }
    return vsr;
  }

  /**
   * Equivalent to calling {@link #importArrayStream(BufferAllocator, ArrowArrayStream, boolean)
   * importArrayStream(allocator, stream, true)}.
   *
   * @param allocator Buffer allocator for allocating the output data.
   * @param stream C stream interface struct to import.
   * @return Imported reader
   * @see #importArrayStream(BufferAllocator, ArrowArrayStream, boolean)
   */
  public static ArrowReader importArrayStream(BufferAllocator allocator, ArrowArrayStream stream) {
    return importArrayStream(allocator, stream, true);
  }

  /**
   * Import an ArrowArrayStream as an {@link ArrowReader}.
   *
   * <p>On successful completion, the ArrowArrayStream struct will have been moved (as per the C
   * data interface specification) to a private object held alive by the resulting ArrowReader.
   *
   * @param allocator Buffer allocator for allocating the output data.
   * @param stream C stream interface struct to import.
   * @param closeImportedStructs if true, the ArrowArrayStream struct will be closed when this
   *     method completes successfully
   * @return Imported reader
   */
  public static ArrowReader importArrayStream(
      BufferAllocator allocator, ArrowArrayStream stream, boolean closeImportedStructs) {
    ArrowArrayStreamReader reader = new ArrowArrayStreamReader(allocator, stream);
    if (closeImportedStructs) {
      stream.close();
    }
    return reader;
  }
}

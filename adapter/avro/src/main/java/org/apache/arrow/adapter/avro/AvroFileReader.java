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


import org.apache.arrow.adapter.avro.consumers.CompositeAvroConsumer;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Set;

class AvroFileReader implements DictionaryProvider {

  // Writer owns a channel / decoder and will close them
  // Schema / VSR / dictionaries are created when header is read
  // VSR / dictionaries are cleaned up on close
  // Dictionaries accessible through DictionaryProvider iface

  // Use magic from Avro's own constants
  private static final byte[] AVRO_MAGIC = DataFileConstants.MAGIC;

  private static final String codecName = "zstandard";
  private static final CompressionUtil.CodecType codecType = CompressionUtil.CodecType.ZSTD;

  private final InputStream stream;
  private final BinaryDecoder decoder;

  private final BufferAllocator allocator;
  private final ArrowBuf batchBuffer;
  private BinaryDecoder batchDecoder;

  private final CompressionCodec compressionCodec;
  private CompositeAvroConsumer recordConsumer;

  private org.apache.avro.Schema avroSchema;
  private Schema arrowSchema;
  private VectorSchemaRoot batch;


  // Avro decoder configured externally
  public AvroFileReader(
      InputStream stream,
      BufferAllocator allocator) {

    this.stream = new PushbackInputStream(stream);
    this.decoder = DecoderFactory.get().directBinaryDecoder(stream, null);

    this.allocator = allocator;

    this.compressionCodec = CompressionCodec.Factory.INSTANCE.createCodec(codecType);
  }

  // Sets up a defaulr binary deocder for the channel
  // Avro read sequentially so seekable channel not needed
  public AvroFileReader(
      ReadableByteChannel channel,
      BufferAllocator allocator) {

    this(Channels.newInputStream(channel), allocator);
  }

  // Schema and VSR available after readHeader()
  Schema getSchema() {

  }

  VectorSchemaRoot getVectorSchemaRoot() {

  }

  @Override
  public Set<Long> getDictionaryIds() {
    return Set.of();
  }

  @Override
  public Dictionary lookup(long id) {
    return null;
  }

  // Read the Avro header and set up schema / VSR / dictionaries
  void readHeader() {

  }

  // Read the next Avro block and load it into the VSR
  // Return true if successful, false if EOS
  // Also false in non-blocking mode if need more data
  boolean readBatch() throws IOException {

    if (decoder.isEnd() || decoder.inputStream().available() <= 8) {
      return false;
    }

    long nRows = decoder.readLong();
    long nBytes = decoder.readLong();

    if (nRows > Integer.MAX_VALUE || nBytes > Integer.MAX_VALUE) {
      throw new RuntimeException(); // TODO error
    }

    long blockSize = nBytes + 24;

    if (decoder.inputStream().available() <= blockSize - 8) {
      return false;
    }

    decoder.skipFixed((int) nBytes);

    byte[] blockSyncMarker = new byte[16];
    decoder.readFixed(blockSyncMarker);

    // TODO: Check sync marker

    ArrowBuf encodedData = null;

    // Decompressed buffer is newly allocated and needs to be released
    try (ArrowBuf rawData = compressionCodec.decompress(allocator, encodedData)) {

      // Set up stream and decoder to read from the decompressed buffer
      InputStream batchStream = new BufferInputStream(rawData);
      batchDecoder = DecoderFactory.get().directBinaryDecoder(batchStream, batchDecoder);

      // Consume a batch
      batch.getFieldVectors().forEach(vector -> ensureCapacity(vector, (int) nRows));

      for (int row = 0; row < nRows; row++) {
        recordConsumer.consume(batchDecoder);
      }

      batch.setRowCount((int) nRows);

      // Batch is ready
      return true;
    }
  }

  private void ensureCapacity(FieldVector vector, int capacity) {
    if (vector.getValueCapacity() < capacity) {
      vector.setInitialCapacity(capacity);
    }
  }

  // Check for position and size of the next Avro data block
  // Provides a mechanism for non-blocking / reactive styles
  boolean hasNextBatch() throws IOException {
    return ! decoder.isEnd();
  }

  long nextBatchPosition() {

  }

  long nextBatchSize() {

  }

  // Closes encoder and / or channel
  // Also closes VSR and dictionary vectors
  void close() throws IOException {
    stream.close();
    batchBuffer.close();
    batch.close();
    // TODO: Close dictionaries
  }

}

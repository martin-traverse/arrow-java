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
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;


class AvroFileWriter {

  // Use magic from Avro's own constants
  private static final byte[] AVRO_MAGIC = DataFileConstants.MAGIC;

  private static final String codecName = "zstandard";
  private static final CompressionUtil.CodecType codecType = CompressionUtil.CodecType.ZSTD;

  private final OutputStream stream;
  private final Encoder encoder;

  private final BufferAllocator allocator;
  private final BufferOutputStream batchBuffer;
  private BinaryEncoder batchEncoder;
  private VectorSchemaRoot batch;

  private final Schema avroSchema;
  private final byte[] syncMarker;

  private final CompositeAvroProducer recordProducer;
  private final CompressionCodec compressionCodec;


  public AvroFileWriter(
      OutputStream stream,
      VectorSchemaRoot firstBatch,
      DictionaryProvider dictionaries)
      throws IOException {

      EncoderFactory encoderFactory = EncoderFactory.get();

      this.stream = stream;
      this.encoder = encoderFactory.binaryEncoder(stream, null);

      this.allocator = firstBatch.getVector(0).getAllocator();
      this.batchBuffer = new BufferOutputStream(allocator);
      this.batchEncoder = encoderFactory.binaryEncoder(stream, null);
      this.batch = firstBatch;

    try {

      this.avroSchema = ArrowToAvroUtils.createAvroSchema(
          firstBatch.getSchema().getFields(),
          dictionaries);

      this.recordProducer = ArrowToAvroUtils.createCompositeProducer(
          firstBatch.getFieldVectors(),
          dictionaries);

      this.compressionCodec = CompressionCodec.Factory.INSTANCE.createCodec(codecType);

      // Generate a random sync marker
      var random = new Random();
      this.syncMarker = new byte[16];
      random.nextBytes(this.syncMarker);
    }
    catch (Throwable e) {
        // Do not leak the batch buffer if there are problems during setup
        batchBuffer.close();
        throw e;
    }
  }

  // Sets up a defaulr binary encoder for the channel
  public AvroFileWriter(
      WritableByteChannel channel,
      VectorSchemaRoot firstBatch,
      DictionaryProvider dictionaries)
      throws IOException {

    this(Channels.newOutputStream(channel), firstBatch, dictionaries);
  }

  // Write the Avro header (throws if already written)
  public void writeHeader() throws IOException {

    // Prepare the metadata map
    Map<String, byte[]> metadata = new HashMap<>();
    metadata.put("avro.schema", avroSchema.toString().getBytes(StandardCharsets.UTF_8));
    metadata.put("avro.codec", codecName.getBytes(StandardCharsets.UTF_8));

    // Avro magic
    encoder.writeFixed(AVRO_MAGIC);

    // Write the metadata map
    encoder.writeMapStart(); // write metadata
    encoder.setItemCount(metadata.size());
    for (Map.Entry<String, byte[]> entry : metadata.entrySet()) {
      encoder.startItem();
      encoder.writeString(entry.getKey());
      encoder.writeBytes(entry.getValue());
    }
    encoder.writeMapEnd();

    // Sync marker denotes end of the header
    encoder.writeFixed(this.syncMarker);
    encoder.flush();
  }

  // Write the contents of the VSR as an Avro data block
  // Writes header if not yet written
  // Expects new data to be in the batch (i.e. VSR can be recycled)
  public void writeBatch() throws IOException {

    // Reset batch buffer and encoder
    batchBuffer.reset();
    batchEncoder = EncoderFactory.get().directBinaryEncoder(batchBuffer, batchEncoder);

    // Reset producers
    recordProducer.getProducers().forEach(producer -> producer.setPosition(0));

    // Produce a batch
    for (int row = 0; row < batch.getRowCount(); row++) {
      recordProducer.produce(batchEncoder);
    }

    batchEncoder.flush();

    // Raw buffer is a view onto the stream backing buffer - do not release
    ArrowBuf rawBuffer = batchBuffer.getBuffer();

    // Compressed buffer is newly allocated and needs to be released
    try (ArrowBuf compressedBuffer = compressionCodec.compress(allocator, rawBuffer)) {

      // Write Avro block to the main encoder
      encoder.writeLong(batch.getRowCount());
      encoder.writeBytes(compressedBuffer.nioBuffer());
      encoder.writeFixed(syncMarker);
    }
  }

  // Reset vectors in all the producders
  // Supports a stream of VSRs if source VSR is not recycled
  void resetBatch(VectorSchemaRoot batch) {
    recordProducer.resetProducerVectors(batch);
    this.batch = batch;
  }

  public void flush() throws IOException {
    encoder.flush();
  }

  // Closes encoder and / or channel
  // Does not close VSR or dictionary vectors
  public void close() throws IOException {
    encoder.flush();
    stream.close();
    batchBuffer.close();
  }
}

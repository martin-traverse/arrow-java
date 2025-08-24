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

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.arrow.adapter.avro.consumers.CompositeAvroConsumer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.InvalidAvroMagicException;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.io.BinaryData;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;


class AvroFileReader implements DictionaryProvider {

  // Writer owns a channel / decoder and will close them
  // Schema / VSR / dictionaries are created when header is read
  // VSR / dictionaries are cleaned up on close
  // Dictionaries accessible through DictionaryProvider iface

  // Use magic from Avro's own constants
  private static final byte[] AVRO_MAGIC = DataFileConstants.MAGIC;
  private static final int SYNC_MARKER_SIZE = 16;

  private final InputStream stream;
  private final BinaryDecoder decoder;
  private final BufferAllocator allocator;
  private final boolean lookAhead;

  private org.apache.avro.Schema avroSchema;
  private String avroCodec;
  private final byte[] syncMarker;

  private CompositeAvroConsumer recordConsumer;
  private VectorSchemaRoot arrowBatch;
  private Schema arrowSchema;
  private DictionaryProvider.MapDictionaryProvider dictionaries;

  private boolean readHeaderDone;
  private long headerSize;
  private long nextBatchPosition;
  private ByteBuffer batchBuffer;
  private BinaryDecoder batchDecoder;
  private final byte[] batchSyncMarker;

  // Create a new AvroFileReader for the input stream
  // In order to support non-blocking mode, the stream must support mark / reset
  public AvroFileReader(InputStream stream, BufferAllocator allocator, boolean lookAhead) {

    Preconditions.checkNotNull(stream, "Input stream cannot be null");
    Preconditions.checkNotNull(allocator, "Buffer allocator cannot be null");

    if (lookAhead) {
      Preconditions.checkArgument(stream.markSupported(), "Input stream does not support lookAhead (requires markSupporter() == true)");
    }

    this.stream = stream;
    this.allocator = allocator;
    this.lookAhead = lookAhead;

    if (lookAhead) {
      this.decoder = DecoderFactory.get().directBinaryDecoder(stream, null);
    } else {
      this.decoder = DecoderFactory.get().binaryDecoder(stream, null);
    }

    this.syncMarker = new byte[SYNC_MARKER_SIZE];
    this.batchSyncMarker = new byte[SYNC_MARKER_SIZE];
  }

  // Read the Avro header and set up schema / VSR / dictionaries
  public void readHeader() throws IOException {

    Preconditions.checkState(!readHeaderDone, "readHeader() has already been called");

    // Keep track of the header size
    long headerSize = 0;

    // Read Avro magic
    byte[] magic = new byte[AVRO_MAGIC.length];
    decoder.readFixed(magic);
    headerSize += magic.length;

    // Validate Avro magic
    int validateMagic =
        BinaryData.compareBytes(AVRO_MAGIC, 0, AVRO_MAGIC.length, magic, 0, AVRO_MAGIC.length);

    if (validateMagic != 0) {
      throw new InvalidAvroMagicException("Invalid AVRO data file: The file is not an Avro file");
    }

    // Read the metadata map
    for (long count = decoder.readMapStart(); count != 0; count = decoder.mapNext()) {

      headerSize += zigzagSize(count);

      for (long i = 0; i < count; i++) {

        ByteBuffer keyBuffer = decoder.readBytes(null);
        ByteBuffer valueBuffer = decoder.readBytes(null);

        headerSize += zigzagSize(keyBuffer.remaining()) + keyBuffer.remaining();
        headerSize += zigzagSize(valueBuffer.remaining()) + valueBuffer.remaining();

        String key = new String(keyBuffer.array(), StandardCharsets.UTF_8);

        // Handle header entries for schema and codec
        if ("avro.schema".equals(key)) {
          avroSchema = processSchema(valueBuffer);
        } else if ("avro.codec".equals(key)) {
          avroCodec = processCodec(valueBuffer);
          // Check codec is available before continuing
          AvroCompression.getAvroCodec(avroCodec);
        }
      }
    }

    // End of map marker
    headerSize += 1;

    // Sync marker denotes end of the header
    decoder.readFixed(syncMarker);
    headerSize += syncMarker.length;

    // Schema must always be present
    if (avroSchema == null) {
      throw new RuntimeException("Invalid AVRO data file: Schema missing in file header");
    }

    // Prepare read config
    this.dictionaries = new DictionaryProvider.MapDictionaryProvider();
    AvroToArrowConfig config = new AvroToArrowConfig(allocator, 0, dictionaries, Set.of(), false);

    // Calling this method will also populate the dictionary map
    this.recordConsumer = AvroToArrowUtils.createCompositeConsumer(avroSchema, config);

    // Initialize data vectors
    List<FieldVector> vectors = new ArrayList<>(arrowSchema.getFields().size());
    for (int i = 0; i < arrowSchema.getFields().size(); i++) {
      FieldVector vector = recordConsumer.getConsumers().get(i).getVector();
      vectors.add(vector);
    }

    // Initialize batch and schema
    this.arrowBatch = new VectorSchemaRoot(vectors);
    this.arrowSchema = arrowBatch.getSchema();

    // First batch starts after the header
    this.readHeaderDone = true;
    this.headerSize = headerSize;
    this.nextBatchPosition = headerSize;
  }

  private org.apache.avro.Schema processSchema(ByteBuffer buffer) throws IOException {

    org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();

    try (InputStream schemaStream = new ByteArrayInputStream(buffer.array())) {
      return parser.parse(schemaStream);
    }
  }

  private String processCodec(ByteBuffer buffer) {

    if (buffer != null && buffer.remaining() > 0) {
      return new String(buffer.array(), StandardCharsets.UTF_8);
    } else {
      return DataFileConstants.NULL_CODEC;
    }
  }

  // Schema and VSR available after readHeader()
  public Schema getSchema() {
    Preconditions.checkState(readHeaderDone, "readHeader() has not been called");
    return arrowSchema;
  }

  public VectorSchemaRoot getVectorSchemaRoot() {
    Preconditions.checkState(readHeaderDone, "readHeader() has not been called");
    return arrowBatch;
  }

  @Override
  public Set<Long> getDictionaryIds() {
    Preconditions.checkState(readHeaderDone, "readHeader() has not been called");
    return dictionaries.getDictionaryIds();
  }

  @Override
  public Dictionary lookup(long id) {
    Preconditions.checkState(readHeaderDone, "readHeader() has not been called");
    return dictionaries.lookup(id);
  }

  // Read the next Avro block and load it into the VSR
  // Return true if successful, false if EOS
  // Also false in non-blocking mode if need more data
  public boolean readBatch() throws IOException {

    Preconditions.checkState(readHeaderDone, "readHeader() has not been called");

    if (!hasNextBatch())
      return false;

    // Read Avro block from the main encoder
    long nRows = decoder.readLong();
    batchBuffer = decoder.readBytes(batchBuffer);
    decoder.readFixed(batchSyncMarker);

    long batchSize =
        zigzagSize(nRows)
            + zigzagSize(batchBuffer.remaining())
            + batchBuffer.remaining()
            + SYNC_MARKER_SIZE;

    // Validate sync marker - mismatch indicates a corrupt file
    int validateMarker =
        BinaryData.compareBytes(
            syncMarker, 0, SYNC_MARKER_SIZE, batchSyncMarker, 0, SYNC_MARKER_SIZE);

    if (validateMarker != 0) {
      throw new AvroRuntimeException("Invalid AVRO data file: The file is corrupted");
    }

    // Reset producers
    recordConsumer
        .getConsumers()
        .forEach(consumer -> ensureCapacity(consumer.getVector(), (int) nRows));
    recordConsumer.getConsumers().forEach(consumer -> consumer.setPosition(0));

    // Decompress the batch buffer using Avro's codecs
    var codec = AvroCompression.getAvroCodec(avroCodec);
    var batchDecompressed = codec.decompress(batchBuffer);

    // Prepare batch stream and decoder
    try (InputStream batchStream = new ByteArrayInputStream(batchDecompressed.array())) {

      batchDecoder = DecoderFactory.get().directBinaryDecoder(batchStream, batchDecoder);

      // Consume a batch, reading from the batch stream (buffer)
      for (int row = 0; row < nRows; row++) {
        recordConsumer.consume(batchDecoder);
      }

      arrowBatch.setRowCount((int) nRows);

      // Update next batch position
      nextBatchPosition += batchSize;

      // Batch is ready
      return true;
    }
  }

  public boolean skipBatch() throws IOException {

    Preconditions.checkState(readHeaderDone, "readHeader() has not been called");

    if (!hasNextBatch())
      return false;

    // Read Avro block from the main encoder
    long nRows = decoder.readLong();
    long nBytes = decoder.readLong();

    int chunk;
    for(long remaining = nBytes; remaining > 0; remaining -= chunk) {
      chunk = (int) Math.min(remaining, Integer.MAX_VALUE);
      decoder.skipFixed(chunk);
    }

    decoder.skipFixed(SYNC_MARKER_SIZE);

    // Validate sync marker - mismatch indicates a corrupt file
    int validateMarker =
        BinaryData.compareBytes(
            syncMarker, 0, SYNC_MARKER_SIZE, batchSyncMarker, 0, SYNC_MARKER_SIZE);

    if (validateMarker != 0) {
      throw new AvroRuntimeException("Invalid AVRO data file: The file is corrupted");
    }

    long batchSize =
        zigzagSize(nRows)
            + zigzagSize(batchBuffer.remaining())
            + batchBuffer.remaining()
            + SYNC_MARKER_SIZE;

    nextBatchPosition += batchSize;
    return true;
  }

  // Check for position and size of the next Avro data block
  // Provides a mechanism for non-blocking / reactive styles
  public boolean hasNextBatch() throws IOException {

    Preconditions.checkState(readHeaderDone, "readHeader() has not been called");

    if (lookAhead) {
      return decoder.inputStream().available() > 0;
    } else {
      return !decoder.isEnd();
    }
  }

  public long headerSize() throws IOException {

    Preconditions.checkState(readHeaderDone || lookAhead, "readHeader() has not been called and look-ahead is not enabled");

    // Do not run header size logic if header has already been read
    if (readHeaderDone) {
      return headerSize;
    }

    InputStream in = decoder.inputStream();
    long headerSize = AVRO_MAGIC.length + zigzagSize(0) + SYNC_MARKER_SIZE;

    if (in.available() < headerSize) {
      return headerSize - in.available();
    }

    try {

      int bytesRead = 0;
      int mark = headerSizeMark(headerSize, bytesRead, 0);

      byte[] magic = new byte[AVRO_MAGIC.length];
      decoder.readFixed(magic);
      bytesRead += AVRO_MAGIC.length;

      // Validate Avro magic
      // A wrong file type will cause unexpected errors, even though the header is not fully parsed
      int validateMagic =
          BinaryData.compareBytes(AVRO_MAGIC, 0, AVRO_MAGIC.length, magic, 0, AVRO_MAGIC.length);

      if (validateMagic != 0) {
        throw new InvalidAvroMagicException("Invalid AVRO data file: The file is not an Avro file");
      }

      for (long count = decoder.readMapStart(); count != 0; count = decoder.mapNext()) {

        headerSize += zigzagSize(count);
        bytesRead += zigzagSize(count);

        for (long i = 0; i < count; i++) {

          int keySize = decoder.readInt();
          bytesRead += zigzagSize(keySize);

          if (in.available() < keySize + SYNC_MARKER_SIZE) {
            return keySize + SYNC_MARKER_SIZE - in.available();
          }

          mark = headerSizeMark(headerSize, bytesRead, mark);
          decoder.skipFixed(keySize);
          bytesRead += keySize;

          int valueSize = decoder.readInt();
          bytesRead += zigzagSize(valueSize);

          if (in.available() < valueSize + SYNC_MARKER_SIZE) {
            return valueSize + SYNC_MARKER_SIZE - in.available();
          }

          mark = headerSizeMark(headerSize, bytesRead, mark);
          decoder.skipFixed(valueSize);
          bytesRead += valueSize;

          headerSize +=
              zigzagSize(keySize) + keySize +
              zigzagSize(valueSize) + valueSize;

          mark = headerSizeMark(headerSize, bytesRead, mark);
        }
      }

      return headerSize;
    }
    finally {
      in.reset();
    }
  }

  private int headerSizeMark(long headerSize, int bytesRead, int mark) throws IOException {
    if (headerSize > mark) {
      int newMark = Math.max((int) headerSize, mark * 2);
      InputStream in = decoder.inputStream();
      in.reset();
      in.mark(newMark);
      if (bytesRead > 0) {
        decoder.skipFixed(bytesRead);
      }
      return newMark;
    } else {
      return mark;
    }
  }

  public long nextBatchPosition() {
    Preconditions.checkState(readHeaderDone, "readHeader() has not been called");
    return nextBatchPosition;
  }

  public long nextBatchSize() throws IOException {

    Preconditions.checkState(readHeaderDone, "readHeader() has not been called");
    Preconditions.checkState(lookAhead, "nextBatchSize() is not available because look-ahead is not enabled");

    InputStream in = decoder.inputStream();
    int bytesRequired = 20;

    if (in.available() < bytesRequired) {
      return bytesRequired -  in.available();
    }

    in.mark(bytesRequired);
    long nRows = decoder.readLong();
    long nBytes = decoder.readLong();
    in.reset();

    return zigzagSize(nRows) + zigzagSize(nBytes) + nBytes + SYNC_MARKER_SIZE;
  }

  public long nextBatchRowCount() throws IOException {

    Preconditions.checkState(readHeaderDone, "readHeader() has not been called");
    Preconditions.checkState(lookAhead, "nextBatchSize() is not available because look-ahead is not enabled");

    InputStream in = decoder.inputStream();
    int bytesRequired = 10;

    if (in.available() < bytesRequired) {
      return bytesRequired - in.available();
    }

    in.mark(bytesRequired);
    long nRows = decoder.readLong();
    in.reset();

    return nRows;
  }

  private int zigzagSize(long n) {

    long val = (n << 1) ^ (n >> 63); // move sign to low-order bit
    int bytes = 1;

    while ((val & ~0x7F) != 0) {
      bytes += 1;
      val >>>= 7;
    }

    return bytes;
  }

  private void ensureCapacity(FieldVector vector, int capacity) {
    if (vector.getValueCapacity() < capacity) {
      vector.setInitialCapacity(capacity);
    }
  }

  // Closes encoder and / or channel
  // Also closes VSR and dictionary vectors
  public void close() throws IOException {

    stream.close();

    if (arrowBatch != null) {
      arrowBatch.close();
    }

    if (dictionaries != null) {
      for (long dictionaryId : dictionaries.getDictionaryIds()) {
        Dictionary dictionary = dictionaries.lookup(dictionaryId);
        dictionary.getVector().close();
      }
    }
  }
}

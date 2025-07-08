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

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.MemoryUtil;

import java.io.IOException;
import java.io.OutputStream;

public class BufferOutputStream extends OutputStream {

  private static final int INITIAL_BUFFER_SIZE = 4096;

  private ArrowBuf buffer;

  public BufferOutputStream(BufferAllocator allocator) {
    buffer = allocator.buffer(INITIAL_BUFFER_SIZE);
  }

  public ArrowBuf getBuffer() {
    return buffer.slice(0, buffer.writerIndex());
  }

  @Override
  public void write(int b) throws IOException {
    ensureCapacity(buffer.capacity() + 1);
    buffer.writeByte(b);
  }

  @Override
  public void write(byte[] b) throws IOException {
    ensureCapacity(buffer.capacity() + b.length);
    buffer.writeBytes(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    ensureCapacity(buffer.capacity() + len);
    buffer.writeBytes(b, off, len);
  }

  @Override
  public void flush() {
    // no-op
  }

  public void reset() {
    buffer.clear();
  }

  @Override
  public void close() throws IOException {
    buffer.close();
  }

  private void ensureCapacity(long capacity) {

    if (capacity > buffer.capacity()) {

      long newCapacity = Math.max(capacity, buffer.capacity() * 2);

      BufferAllocator allocator = buffer.getReferenceManager().getAllocator();
      ArrowBuf newBuffer = allocator.buffer(newCapacity);

      try {

        MemoryUtil.copyMemory(buffer.memoryAddress(), newBuffer.memoryAddress(), buffer.writerIndex());

        buffer.close();
        buffer = newBuffer;
      }
      catch (Throwable t) {
        newBuffer.close();
        throw t;
      }
    }
  }
}

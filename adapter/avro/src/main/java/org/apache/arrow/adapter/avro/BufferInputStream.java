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

import java.io.IOException;
import java.io.InputStream;

public class BufferInputStream extends InputStream {

  private final ArrowBuf buffer;

  public BufferInputStream(ArrowBuf buffer) {

    if (buffer.writerIndex() > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("Buffer is too large");
    }

    this.buffer = buffer;
  }

  @Override
  public int read() throws IOException {
    if (buffer.readableBytes() > 0) {
      return buffer.readByte() & 0xff;
    } else {
      return -1;
    }
  }

  @Override
  public int read(byte[] b) throws IOException {
    if (buffer.readableBytes() > 0) {
      int nBytes = Math.min(b.length, (int) buffer.readableBytes());
      buffer.readBytes(b);
      return b.length;
    }
    else {
      return -1;
    }
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return super.read(b, off, len);
  }

  @Override
  public byte[] readAllBytes() throws IOException {
    return readNBytes((int) buffer.readableBytes());
  }

  @Override
  public byte[] readNBytes(int len) throws IOException {
    int nBytes = Math.min(len,(int)  buffer.readableBytes());
    byte[] b = new byte[nBytes];
    buffer.readBytes(b);
    return b;
  }

  @Override
  public int readNBytes(byte[] b, int off, int len) throws IOException {
    return super.readNBytes(b, off, len);
  }

  @Override
  public long skip(long n) throws IOException {
    return super.skip(n);
  }

  @Override
  public void skipNBytes(long n) throws IOException {
    buffer.readerIndex(buffer.readerIndex() + n);
  }

  @Override
  public int available() throws IOException {
    return (int) buffer.readableBytes();
  }

  @Override
  public void close() throws IOException {
    buffer.close();
  }
}

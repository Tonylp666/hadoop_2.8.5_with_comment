/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.io.compress;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class DecompressorStream extends CompressionInputStream {
  /**
   * The maximum input buffer size.
   */
  private static final int MAX_INPUT_BUFFER_SIZE = 512;
  /**
   * MAX_SKIP_BUFFER_SIZE is used to determine the maximum buffer size to
   * use when skipping. See {@link java.io.InputStream}.
   */
  private static final int MAX_SKIP_BUFFER_SIZE = 2048;

  private byte[] skipBytes;
  private byte[] oneByte = new byte[1];

  protected Decompressor decompressor = null;
  protected byte[] buffer;
  protected boolean eof = false;
  protected boolean closed = false;
  private int lastBytesSent = 0;

  @VisibleForTesting
  DecompressorStream(InputStream in, Decompressor decompressor,
                            int bufferSize, int skipBufferSize)
      throws IOException {
    super(in);

    if (decompressor == null) {
      throw new NullPointerException();
    } else if (bufferSize <= 0) {
      throw new IllegalArgumentException("Illegal bufferSize");
    }

    this.decompressor = decompressor;
    buffer = new byte[bufferSize];
    skipBytes = new byte[skipBufferSize];
  }

  public DecompressorStream(InputStream in, Decompressor decompressor,
                            int bufferSize)
      throws IOException {
    this(in, decompressor, bufferSize, MAX_SKIP_BUFFER_SIZE);
  }

  public DecompressorStream(InputStream in, Decompressor decompressor)
      throws IOException {
    this(in, decompressor, MAX_INPUT_BUFFER_SIZE);
  }

  /**
   * Allow derived classes to directly set the underlying stream.
   * lipng
   *      protected:当前类、同一包下、子孙类。
   * 
   * @param in Underlying input stream.
   * @throws IOException
   */
  protected DecompressorStream(InputStream in) throws IOException {
    super(in);
  }

  @Override
  public int read() throws IOException {
    checkStream();
    return (read(oneByte, 0, oneByte.length) == -1) ? -1 : (oneByte[0] & 0xff);
  }

  /**
   *  liping
   *        这个方法调用decompress(),在调用Decompressor#decompre() 实现压缩功能
   * @param b   liping:解压缓冲区，存的是压缩的数据
   * @param off
   * @param len
   * @return
   * @throws IOException
   */
  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    checkStream();
    
    if ((off | len | (off + len) | (b.length - (off + len))) < 0) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return 0;
    }

    return decompress(b, off, len);
  }

  /**
   * liping
   *      getCompressedData(); 返回的是本次往decompressor里面送的数据大小
   *
   *      如果上一次压缩的数据已经消费完全，则进入循环。
   *       接着判断 if (decompressor.finished())，即有没有到达已解压缩输出流的末端(uncompressedDirectBuf)，
   *       当uncompressedDirectBuf到达末端，在快式压缩/解压中，此时会往decompressor中塞数据，塞数据的数量由getCompressedData()确定。
   *       Ⅱ，若uncompressedDirectBuf到达末端，在流式压缩/解压中
   *
   *       Ⅲ，在若uncompressedDirectBuf未到达末端，且needInput()为true时，会往decompressor中塞数据，塞数据的数量由getCompressedData()确定。
   *
   *
   * @param b liping：compressed data
   * @param off
   * @param len
   * @return  liping:从decompresssor 中拿到的数据量
   * @throws IOException
   */
  protected int decompress(byte[] b, int off, int len) throws IOException {
    int n;

    //判断上一次解压后的数据是不是被全部消费，如果没有消费完全，在这个语句里面就会进行消费，并返回消费数据的数量。
    while ((n = decompressor.decompress(b, off, len)) == 0) {
      if (decompressor.needsDictionary()) {
        eof = true;
        return -1;
      }
      //liping  是否已到达已解压数据的末端
      if (decompressor.finished()) {
        // First see if there was any leftover buffered input from previous
        // stream; if not, attempt to refill buffer.  If refill -> EOF, we're
        // all done; else reset, fix up input buffer, and get ready for next
        // concatenated substream/"member".
        int nRemaining = decompressor.getRemaining();
        if (nRemaining == 0) {
          // liping: decompressor.getRemaining(); compressedDirectBuf 中剩余的字节数，快式压缩时为0，因为一次将整块数据解压。
          // m = getCompressedData(); 本次往 buffer 中塞进的数据量，然后重置解压缩器decompressor,并将本次输入的数据（buffer）塞到解压缩器---->compressedDirectBuf 。并记录
          // -1 说明到达了压缩文件的末端。
          //

          int m = getCompressedData();
          if (m == -1) {
            // apparently the previous end-of-stream was also end-of-file:
            // return success, as if we had never called getCompressedData()
            eof = true;
            return -1;
          }
          decompressor.reset();
          decompressor.setInput(buffer, 0, m);
          lastBytesSent = m;
        } else {
          // looks like it's a concatenated stream:  reset low-level zlib (or
          // other engine) and buffers, then "resend" remaining input data
          decompressor.reset();
          int leftoverOffset = lastBytesSent - nRemaining;
          assert (leftoverOffset >= 0);
          // this recopies userBuf -> direct buffer if using native libraries:
          decompressor.setInput(buffer, leftoverOffset, nRemaining);
          // NOTE:  this is the one place we do NOT want to save the number
          // of bytes sent (nRemaining here) into lastBytesSent:  since we
          // are resending what we've already sent before, offset is nonzero
          // in general (only way it could be zero is if it already equals
          // nRemaining), which would then screw up the offset calculation
          // _next_ time around.  IOW, getRemaining() is in terms of the
          // original, zero-offset bufferload, so lastBytesSent must be as
          // well.  Cheesy ASCII art:
          //
          //          <------------ m, lastBytesSent ----------->
          //          +===============================================+
          // buffer:  |1111111111|22222222222222222|333333333333|     |
          //          +===============================================+
          //     #1:  <-- off -->|<-------- nRemaining --------->
          //     #2:  <----------- off ----------->|<-- nRem. -->
          //     #3:  (final substream:  nRemaining == 0; eof = true)
          //
          // If lastBytesSent is anything other than m, as shown, then "off"
          // will be calculated incorrectly.
        }
      } else if (decompressor.needsInput()) {
        int m = getCompressedData();
        if (m == -1) {
          throw new EOFException("Unexpected end of input stream");
        }
        decompressor.setInput(buffer, 0, m);
        lastBytesSent = m;
      }
    }

    return n;
  }

  protected int getCompressedData() throws IOException {
    checkStream();
  
    // note that the _caller_ is now required to call setInput() or throw
    return in.read(buffer, 0, buffer.length);
  }

  protected void checkStream() throws IOException {
    if (closed) {
      throw new IOException("Stream closed");
    }
  }
  
  @Override
  public void resetState() throws IOException {
    decompressor.reset();
  }

  @Override
  public long skip(long n) throws IOException {
    // Sanity checks
    if (n < 0) {
      throw new IllegalArgumentException("negative skip length");
    }
    checkStream();

    // Read 'n' bytes
    int skipped = 0;
    while (skipped < n) {
      int len = Math.min(((int)n - skipped), skipBytes.length);
      len = read(skipBytes, 0, len);
      if (len == -1) {
        eof = true;
        break;
      }
      skipped += len;
    }
    return skipped;
  }

  @Override
  public int available() throws IOException {
    checkStream();
    return (eof) ? 0 : 1;
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      try {
        super.close();
      } finally {
        closed = true;
      }
    }
  }

  @Override
  public boolean markSupported() {
    return false;
  }

  @Override
  public synchronized void mark(int readlimit) {
  }

  @Override
  public synchronized void reset() throws IOException {
    throw new IOException("mark/reset not supported");
  }

}

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

package org.apache.hadoop.io.compress.snappy;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.util.NativeCodeLoader;

/**
 * A {@link Compressor} based on the snappy compression algorithm.
 * http://code.google.com/p/snappy/
 */
public class SnappyCompressor implements Compressor {
  private static final Log LOG =
      LogFactory.getLog(SnappyCompressor.class.getName());
  private static final int DEFAULT_DIRECT_BUFFER_SIZE = 64 * 1024;

  private int directBufferSize;
  private Buffer compressedDirectBuf = null;
  private int uncompressedDirectBufLen;
  private Buffer uncompressedDirectBuf = null;
  private byte[] userBuf = null;
  private int userBufOff = 0, userBufLen = 0;
  private boolean finish, finished;

  private long bytesRead = 0L;
  private long bytesWritten = 0L;

  private static boolean nativeSnappyLoaded = false;


  /**
   *  判断是否加载本地库以及是否支持 snappy 压缩
   */
  static {
    if (NativeCodeLoader.isNativeCodeLoaded() &&
        NativeCodeLoader.buildSupportsSnappy()) {
      try {
        initIDs();
        nativeSnappyLoaded = true;
      } catch (Throwable t) {
        LOG.error("failed to load SnappyCompressor", t);
      }
    }
  }
  
  public static boolean isNativeCodeLoaded() {
    return nativeSnappyLoaded;
  }
  
  /**
   * Creates a new compressor.
   *
   * @param directBufferSize size of the direct buffer to be used.
   */
  public SnappyCompressor(int directBufferSize) {
    this.directBufferSize = directBufferSize;

    //liping  创建一个direct的ByteBuffer，这样的ByteBuffer在参与IO操作时性能会更好
    uncompressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize);
    compressedDirectBuf = ByteBuffer.allocateDirect(directBufferSize);
    //liping   当前读取的位置。
    compressedDirectBuf.position(directBufferSize);
  }

  /**
   * Creates a new compressor with the default buffer size.
   */
  public SnappyCompressor() {
    this(DEFAULT_DIRECT_BUFFER_SIZE);
  }

  /**
   * Sets input data for compression.
   * This should be called whenever #needsInput() returns
   * <code>true</code> indicating that more input data is required.
   *
   * @param b   Input data
   * @param off Start offset
   * @param len Length
   */
  @Override
  public void setInput(byte[] b, int off, int len) {
    if (b == null) {
      throw new NullPointerException();
    }
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new ArrayIndexOutOfBoundsException();
    }
    //liping 当往压缩器中灌数据时，说明还没有结束。
    finished = false;

    /**
     * liping 当uncompressedDirectBuf中剩余的数据空间小于本次要给他的数据大小时，
     * 将上述参数的数值赋值到userBuf暂存起来
     * 否则，将数据灌到 uncompressedDirectBuf
     */
    if (len > uncompressedDirectBuf.remaining()) {
      // save data; now !needsInput
      this.userBuf = b;
      this.userBufOff = off;
      this.userBufLen = len;
    } else {
      ((ByteBuffer) uncompressedDirectBuf).put(b, off, len);
      uncompressedDirectBufLen = uncompressedDirectBuf.position();
    }

    bytesRead += len;
  }

  /**
   * If a write would exceed the capacity of the direct buffers, it is set
   * aside to be loaded by this function while the compressed data are
   * consumed.
   */
  void setInputFromSavedData() {
    if (0 >= userBufLen) {
      return;
    }
    finished = false;

    uncompressedDirectBufLen = Math.min(userBufLen, directBufferSize);
    ((ByteBuffer) uncompressedDirectBuf).put(userBuf, userBufOff,
        uncompressedDirectBufLen);

    // Note how much data is being fed to snappy
    userBufOff += uncompressedDirectBufLen;
    userBufLen -= uncompressedDirectBufLen;
  }

  /**
   * Does nothing.
   */
  @Override
  public void setDictionary(byte[] b, int off, int len) {
    // do nothing
  }

  /**
   * liping
   * 满足下面一种情况 needsInput() 方法就返回false
   * 输出缓冲区（即保持压缩结果的缓冲区）有未读取的数据、
   * 输入缓冲区没有空间，以及压缩器已经借用外部缓冲区。
   * 这时，用户需要通过compress()方法取走已经压缩的数据，
   * 直到needsInput()返回true，才可再次通过setInput()方法添加待压缩数据。
   *
   * Returns true if the input data buffer is empty and
   * #setInput() should be called to provide more input.
   *
   * @return <code>true</code> if the input data buffer is empty and
   *         #setInput() should be called in order to provide more input.
   */
  @Override
  public boolean needsInput() {
    return !(compressedDirectBuf.remaining() > 0
        || uncompressedDirectBuf.remaining() == 0 || userBufLen > 0);
  }

  /**
   * When called, indicates that compression should end
   * with the current contents of the input buffer.
   */
  @Override
  public void finish() {
    finish = true;
  }

  /**
   * Returns true if the end of the compressed
   * data output stream has been reached.
   * //liping
   *        如果已到达压缩数据输出流的末尾，则返回true。
   *
   * @return <code>true</code> if the end of the compressed
   *         data output stream has been reached.
   */
  @Override
  public boolean finished() {
    // Check if all uncompressed data has been consumed
    return (finish && finished && compressedDirectBuf.remaining() == 0);
  }

  /**
   * Fills specified buffer with compressed data. Returns actual number
   * of bytes of compressed data. A return value of 0 indicates that
   * needsInput() should be called in order to determine if more input
   * data is required.
   *
   * 用压缩数据填充指定的缓冲区。返回压缩数据的实际字节数。返回值0表示应该调用needsInput()，以确定是否需要更多的输入数据。
   *
   * liping
   *      Compressor通过setInput()方法接收数据到内部缓冲区，自然可以多次调用setInput()方法，但内部缓冲区总是会被写满。
   *      如何判断压缩器内部缓冲区是否已满呢？可以通过needsInput()的返回值，如果是false，
   *      表明缓冲区已经满，这时必须通过compress()方法获取压缩后的数据，释放缓冲区空间。
   *
   * @param b   Buffer for the compressed data---->里面存的是已经压缩成功的数据。
   * @param off Start offset of the data
   * @param len Size of the buffer
   * @return The actual number of bytes of compressed data.
   */
  @Override
  public int compress(byte[] b, int off, int len)
      throws IOException {
    if (b == null) {
      throw new NullPointerException();
    }
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new ArrayIndexOutOfBoundsException();
    }

    // Check if there is compressed data
    int n = compressedDirectBuf.remaining();
    if (n > 0) {
      n = Math.min(n, len);
      /**
       * liping  get(byte[] dst, int offset, int length)
       *          从position位置开始相对读，读length个byte，并写入dst下标从offset到offset+length的区域。
       */
      ((ByteBuffer) compressedDirectBuf).get(b, off, n);
      bytesWritten += n;
      //返回的是已经压缩的实际字节数。
      return n;
    }

    // Re-initialize the snappy's output direct-buffer
    /**
     *  liping 2020.07.10
     *  Invariants: mark <= position <= limit <= capacity
     *  因为前面为了数据安全问题，在compressor初始化的时候 compressedDirectBuf 时的position = limit = capacity。
     *  因为compressedDirectBuf是存放压缩完的数据的， 此时同样为了数据安全，在压缩未开始前，将limit设置为0。
     */
    compressedDirectBuf.clear();
    compressedDirectBuf.limit(0);
    /**
     * liping 07.29
     *
     */
    if (0 == uncompressedDirectBuf.position()) {
      // No compressed data, so we should have !needsInput or !finished
      setInputFromSavedData();
      if (0 == uncompressedDirectBuf.position()) {
        // Called without data; write nothing
        finished = true;
        return 0;
      }
    }

    // Compress data
    /**
     * https://blog.csdn.net/wqy1200/article/details/106541090
     *
     * 接上面的注释，当compressor 中数据塞满时，开始执行压缩操作。
     *  具体的压缩操作是通过Java native 来调用C代码执行的，
     *   n = compressBytesDirect(); 即snappy底层代码的返回值是压缩后数据的实际大小，此时仍然为饿了数据安全
     *   将compressedDirectBuf.limit设置为压缩后的返回值。
     *
     *   此时compressor已经将本次压缩的 uncompressedDirectBuf 中未压数据消费完了，因此将 uncompressedDirectBuf重置，
     *   以方便后续操作
     *
     */
    n = compressBytesDirect();
    compressedDirectBuf.limit(n);
    uncompressedDirectBuf.clear(); // snappy consumes all buffer input

    // Set 'finished' if snapy has consumed all user-data
    if (0 == userBufLen) {
      finished = true;
    }

    // Get atmost 'len' bytes
    n = Math.min(n, len);
    bytesWritten += n;
    //liping 将已压的数据按 n 的长度写出到b--->输出流
    ((ByteBuffer) compressedDirectBuf).get(b, off, n);

    return n;
  }

  /**
   * Resets compressor so that a new set of input data can be processed.
   */
  @Override
  public void reset() {
    finish = false;
    finished = false;
    uncompressedDirectBuf.clear();
    uncompressedDirectBufLen = 0;
    compressedDirectBuf.clear();
    compressedDirectBuf.limit(0);
    userBufOff = userBufLen = 0;
    bytesRead = bytesWritten = 0L;
  }

  /**
   * Prepare the compressor to be used in a new stream with settings defined in
   * the given Configuration
   *
   * @param conf Configuration from which new setting are fetched
   */
  @Override
  public void reinit(Configuration conf) {
    reset();
  }

  /**
   * Return number of bytes given to this compressor since last reset.
   */
  @Override
  public long getBytesRead() {
    return bytesRead;
  }

  /**
   * Return number of bytes consumed by callers of compress since last reset.
   */
  @Override
  public long getBytesWritten() {
    return bytesWritten;
  }

  /**
   * Closes the compressor and discards any unprocessed input.
   */
  @Override
  public void end() {
  }

  private native static void initIDs();

  /**
   * https://blog.csdn.net/wqy1200/article/details/106541090
   * @return
   */
  private native int compressBytesDirect();

  public native static String getLibraryName();
}

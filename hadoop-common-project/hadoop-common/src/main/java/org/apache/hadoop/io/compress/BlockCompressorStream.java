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

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * A {@link org.apache.hadoop.io.compress.CompressorStream} which works
 * with 'block-based' based compression algorithms, as opposed to 
 * 'stream-based' compression algorithms.
 *
 * It should be noted that this wrapper does not guarantee that blocks will
 * be sized for the compressor. If the
 * {@link org.apache.hadoop.io.compress.Compressor} requires buffering to
 * effect meaningful compression, it is responsible for it.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class BlockCompressorStream extends CompressorStream {

  // The 'maximum' size of input data to be compressed, to account
  // for the overhead of the compression algorithm.
  private final int MAX_INPUT_SIZE;

  /**
   * Create a {@link BlockCompressorStream}.
   * 
   * @param out stream
   * @param compressor compressor to be used
   * @param bufferSize size of buffer
   * @param compressionOverhead maximum 'overhead' of the compression 
   *                            algorithm with given bufferSize
   */
  public BlockCompressorStream(OutputStream out, Compressor compressor, 
                               int bufferSize, int compressionOverhead) {
    super(out, compressor, bufferSize);
    MAX_INPUT_SIZE = bufferSize - compressionOverhead;
  }

  /**
   * Create a {@link BlockCompressorStream} with given output-stream and 
   * compressor.
   * Use default of 512 as bufferSize and compressionOverhead of 
   * (1% of bufferSize + 12 bytes) =  18 bytes (zlib algorithm).
   * 
   * @param out stream
   * @param compressor compressor to be used
   */
  public BlockCompressorStream(OutputStream out, Compressor compressor) {
    this(out, compressor, 512, 18);
  }

  /**
   * Write the data provided to the compression codec, compressing no more
   * than the buffer size less the compression overhead as specified during
   * construction for each block.
   *
   * Each block contains the uncompressed length for the block, followed by
   * one or more length-prefixed blocks of compressed data.
   *
   * 压缩后：//==|=++++++++++|=++++++++++|=++++++++++|=++++++++++|...|=++++++++++|//
   *        //原文|本快压缩后数据大小+压缩后数据|本快压缩后数据大小+压缩后数据|。。。。。。|//
   */
  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    // Sanity checks
    if (compressor.finished()) {
      throw new IOException("write beyond end of stream");
    }
    if (b == null) {
      throw new NullPointerException();
    } else if ((off < 0) || (off > b.length) || (len < 0) ||
               ((off + len) > b.length)) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return;
    }

    //返回的是未压缩的字节数量
    long limlen = compressor.getBytesRead();
    /**
     * len + limlen > MAX_INPUT_SIZE && limlen > 0
     * 调用finish(),此时如果compressor没有结束，则将 limlen 写入压缩输出流中，
     * 然后停止往compressor中灌数据，然后查看compressor 是否结束，
     * 若没有结束则调用compress(),在压缩结束时充值压缩器。
     *
     *
     *  解释：
     *  压缩输出格式：原文数据大小+压缩后的数据大小+压缩后的原文。
     *      把上面这些东西写进去是为了校验，看解压后数据大小是不是和原文数据大小一样，如果不一样就说明压缩或解压异常。
     *
     */
    if (len + limlen > MAX_INPUT_SIZE && limlen > 0) {
      // Adding this segment would exceed the maximum size.
      // Flush data if we have it.
      finish();
      compressor.reset();
    }

    /**
     * 如果输入的数据过长，首先将本次的原文数据大小写出去。
     * 会循环调用setInput()和压缩compress()。
     */
    if (len > MAX_INPUT_SIZE) {
      // The data we're given exceeds the maximum size. Any data
      // we had have been flushed, so we write out this chunk in segments
      // not exceeding the maximum size until it is exhausted.
      rawWriteInt(len);
      do {
        int bufLen = Math.min(len, MAX_INPUT_SIZE);
        
        compressor.setInput(b, off, bufLen);
        compressor.finish();/*停止往compressor 里面灌数据*/
        while (!compressor.finished()) {
          compress();
        }
        compressor.reset();
        off += bufLen;
        len -= bufLen;
      } while (len > 0);
      return;
    }

    /**
     * liping
     *    当 len > MAX_INPUT_SIZE 或者 len + limlen > MAX_INPUT_SIZE && limlen > 0 都不满足时，
     *    说明压缩器还没有满，此时为了效率还可以塞数据到压缩器。因此调用 compressor.setInput()将len长度的数据塞进compressor
     *    再下次调用compressor.getBytesRead()时得到。
     */
    // Give data to the compressor
    compressor.setInput(b, off, len);
    if (!compressor.needsInput()) {
      // compressor buffer size might be smaller than the maximum
      // size, so we permit it to flush if required.
      rawWriteInt((int)compressor.getBytesRead());
      do {
        compress();
      } while (!compressor.needsInput());
    }
  }

  /**
   * liping:当往compressor里面塞数据结束时（compressor里面满了/不能装下一次buffer len），
   *        调用finish() ，首先将本次压缩的原文大小写出去。
   *        然后调用 compressor.finish() 表示不能往compressor 里面塞数据了。
   *        接着当没有到达压缩输出流的结尾就循环调用compress().
   *
   *        在compress()里面，首先获取本次已经压缩的数据的长度，并将其写出，再将本次compressed写出。
   * @throws IOException
   */
  @Override
  public void finish() throws IOException {
    if (!compressor.finished()) {
      rawWriteInt((int)compressor.getBytesRead());
      compressor.finish();
      while (!compressor.finished()) {
        compress();
      }
    }
  }

  /**
   * 首先获取本次已经压缩的数据的长度，并将其写出，再将本次compressed写出。
   * @throws IOException
   * buffer：用于存已经压缩成功的数据的buffer。
   */
  @Override
  protected void compress() throws IOException {
    int len = compressor.compress(buffer, 0, buffer.length);
    if (len > 0) {
      // Write out the compressed chunk
      rawWriteInt(len);
      out.write(buffer, 0, len);
    }
  }

  /**
   * liping
   * @param v
   * @throws IOException
   */
  private void rawWriteInt(int v) throws IOException {
    out.write((v >>> 24) & 0xFF);
    out.write((v >>> 16) & 0xFF);
    out.write((v >>>  8) & 0xFF);
    out.write((v >>>  0) & 0xFF);
  }

}

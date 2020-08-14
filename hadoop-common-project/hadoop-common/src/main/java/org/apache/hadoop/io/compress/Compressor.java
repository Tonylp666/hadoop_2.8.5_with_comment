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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

/**
 * Specification of a stream-based 'compressor' which can be  
 * plugged into a {@link CompressionOutputStream} to compress data.
 * This is modelled after {@link java.util.zip.Deflater}
 * 
 */

/**
 * liping 2020.07.07
 * Compressor通过setInput()方法接收数据到内部缓冲区，自然可以多次调用setInput()方法，但内部缓冲区总是会被写满。
 * 如何判断压缩器内部缓冲区是否已满呢？可以通过needsInput()的返回值，如果是false，
 * 表明缓冲区已经满，这时必须通过compress()方法获取压缩后的数据，释放缓冲区空间。
 *
 * 为了提高压缩效率，并不是每次用户调用setInput()方法，压缩器就会立即工作，所以，为了通知压缩器所有数据已经写入，必须使用finish()方法。
 * finish()调用结束后，压缩器缓冲区中保持的已经压缩的数据，可以继续通过compress()方法获得。
 * 至于要判断压缩器中是否还有未读取的压缩数据，则需要利用finished()方法来判断。
 *
 * 注意　finished()和finish()的作用不同，finish()结束数据输入的过程，
 * 而finished()返回false，表明压缩器中还有未读取的压缩数据，可以继续通过compress()方法读取。
 *
 */
@InterfaceAudience.Public //liping  在任务项目或应用中可使用
@InterfaceStability.Evolving  //liping  不断变化，不同次版本间可能不兼容
public interface Compressor {
  /**
   * Sets input data for compression. 
   * This should be called whenever #needsInput() returns 
   * <code>true</code> indicating that more input data is required.
   * liping
   * 输入要压缩的数据.
   * 根据#needsInput() 返回的值，判断是否执行
   * 如果返回为true，表示需要更多的数据.
   *
   * @param b Input data
   * @param off Start offset
   * @param len Length
   */
  public void setInput(byte[] b, int off, int len);
  
  /**
   * Returns true if the input data buffer is empty and 
   * #setInput() should be called to provide more input.
   * liping 如果输入数据缓冲区为空，则返回true
   * liping 应该调用#setInput()来提供更多的输入。
   * 
   * @return <code>true</code> if the input data buffer is empty and 
   * #setInput() should be called in order to provide more input.
   */
  public boolean needsInput();
  
  /**
   * Sets preset dictionary for compression. A preset dictionary 
   * is used when the history buffer can be predetermined.
   * liping 设置预设字典进行压缩。 当可以预先确定历史缓冲器时使用预设字典。
   *
   * @param b Dictionary data bytes
   * @param off Start offset
   * @param len Length
   */
  public void setDictionary(byte[] b, int off, int len);

  /**
   * Return number of uncompressed bytes input so far.
   * liping 返回已经输入到compressor里未压缩的字节。
   */
  public long getBytesRead();

  /**
   * Return number of compressed bytes output so far.
   * liping 返回输出数据以压缩的字节
   */
  public long getBytesWritten();

  /**
   * When called, indicates that compression should end
   * with the current contents of the input buffer.
   * liping 当调用的时候，表示当前输入缓存的内容的压缩操作应该结束
   *
   */
  public void finish();
  
  /**
   * Returns true if the end of the compressed 
   * data output stream has been reached.
   * @return <code>true</code> if the end of the compressed
   * data output stream has been reached.
   * liping 是否已达到压缩数据输出流的结尾，则返回true。
   *    finish() 和 finished()方法的作用不同，finish() 结束数据的输入过程
   *    finished() 用来判断压缩器中是否用于判断压缩器中是否还有未读取的压缩数据。
   */
  public boolean finished();
  
  /**
   * Fills specified buffer with compressed data. Returns actual number
   * of bytes of compressed data. A return value of 0 indicates that
   * needsInput() should be called in order to determine if more input
   * data is required.
   *  liping
   * 向缓存中填充压缩的数据. 返回实际的压缩的字节数. 返回0表明 needsInput()被调用
   * 来判断是否需要更多的输入数据.
   * 
   * @param b Buffer for the compressed data
   * @param off Start offset of the data
   * @param len Size of the buffer
   * @return The actual number of bytes of compressed data.
   */
  public int compress(byte[] b, int off, int len) throws IOException;
  
  /**
   * Resets compressor so that a new set of input data can be processed.
   * liping 重置 compressor， 预备一些新的输入数据.
   */
  public void reset();
  
  /**
   * Closes the compressor and discards any unprocessed input.
   * liping 关闭 compressor 并丢弃未处理的输入。
   */
  public void end();

  /**
   * Prepare the compressor to be used in a new stream with settings defined in
   * the given Configuration
   * liping reinit()方法更进一步允许使用Hadoop的配置系统，重置并重新配置压缩器。
   *
   * @param conf Configuration from which new setting are fetched
   */
  public void reinit(Configuration conf);
}

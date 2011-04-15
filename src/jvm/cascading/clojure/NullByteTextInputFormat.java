/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cascading.clojure;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.commons.logging.Log; 
import org.apache.commons.logging.LogFactory;

public class NullByteTextInputFormat extends TextInputFormat {
  private static final Log log = LogFactory.getLog(NullByteTextInputFormat.class);
  
  @Override
  public RecordReader<LongWritable,Text> getRecordReader(InputSplit inputSplit,
                                                         JobConf jobConf,
                                                         Reporter reporter) throws IOException {
    return new NullByteRecordReader((FileSplit) inputSplit, jobConf);
  }

  public static class NullByteRecordReader implements RecordReader<LongWritable,Text> {
    private final long start;
    private final long end;
    private final FSDataInputStream fsin;
    private final DataOutputBuffer buffer = new DataOutputBuffer();

    public NullByteRecordReader(FileSplit split, JobConf jobConf) throws IOException {
      log.info(String.format("Creating null terminating record reader"));

      // open the file and seek to the start of the split
      start = split.getStart();
      end = start + split.getLength();
      Path file = split.getPath();
      FileSystem fs = file.getFileSystem(jobConf);
      fsin = fs.open(split.getPath());
      fsin.seek(start);
    }

    @Override
    public boolean next(LongWritable key, Text value) throws IOException {
      if (fsin.getPos() < end) {
        if (readUntilMatch(false)) {
          try {
            if (readUntilMatch(true)) {
              key.set(fsin.getPos());
              value.set(buffer.getData(), 0, buffer.getLength());
              return true;
            }
          } finally {
            buffer.reset();
          }
        }
      }
      return false;
    }

    @Override
    public LongWritable createKey() {
      return new LongWritable();
    }

    @Override
    public Text createValue() {
      return new Text();
    }

    @Override
    public long getPos() throws IOException {
      return fsin.getPos();
    }

    @Override
    public void close() throws IOException {
      fsin.close();
    }

    @Override
    public float getProgress() throws IOException {
      return (fsin.getPos() - start) / (float) (end - start);
    }

    private boolean readUntilMatch(boolean withinBlock) throws IOException {
      while (true) {
        int b = fsin.read();
        // end of file:
        if (b == -1) return false;
        // save to buffer:
        if (withinBlock) {
					if (b == 0) {
						return true;
					}
					buffer.write(b);
				}

        // we've found our null byte
        if (b == 0) {
					return true;
        }

        // see if we've passed the stop point:
        if (!withinBlock && fsin.getPos() >= end) return false;
      }
    }
  }
}
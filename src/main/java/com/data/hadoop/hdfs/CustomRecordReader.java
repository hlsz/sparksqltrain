package com.data.hadoop.hdfs;

import com.data.utils.HadoopUtil;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;

/**
 * 自定义记录文件读取类
 *
 */
public class CustomRecordReader extends RecordReader<PairOfStringLong, Text> {
    private PairOfStringLong key;
    private Text value;

    // define pos and offsets
    private long startOffset;
    private long endOffset;
    private long pos;

    private FileSystem fs;
    private Path path;
    private FSDataInputStream fileIn;
    private LineReader reader;

    public CustomRecordReader(CombineFileSplit split, TaskAttemptContext context, Integer index) throws IOException {
        path = split.getPath(index);
        fs = path.getFileSystem(context.getConfiguration());
        startOffset = split.getOffset(index);
        endOffset = startOffset + split.getLength(index);
        fileIn = fs.open(path);
        reader = new LineReader(fileIn);
        pos = startOffset;
    }

    @Override
    public void initialize(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
        // This will not be called, use custom Constructor
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public float getProgress() throws IOException {
        if (startOffset == endOffset) {
            return 0;
        }
        return Math.min(1.0f, (pos - startOffset) / (float) (endOffset - startOffset));
    }

    @Override
    public PairOfStringLong getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        if (key == null) {
            // key.filename = path.getName()
            // key.offset = pos
            key = new PairOfStringLong(path.getName(), pos);
        }
        if (value == null) {
            value = new Text();
        }
        int newSize = 0;
        if (pos < endOffset) {
            newSize = reader.readLine(value);
            pos += newSize;
        }
        if (newSize == 0) {
            key = null;
            value = null;
            return false;
        } else {
            return true;
        }
    }
}

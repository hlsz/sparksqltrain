package com.data.hadoop.hdfs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import java.io.IOException;
import java.util.List;

/**
  * 自定义文件输入格式，将较小的文件合并到控制大小为MAX_SPLIT_SIZE_128MB的文件中
  */
public class CustomCFIF extends CombineFileInputFormat<PairOfStringLong, Text> {
    final static long MAX_SPLIT_SIZE_128MB = 134217728; // 128 MB = 128*1024*1024


    public CustomCFIF() {
        super();
        setMaxSplitSize(MAX_SPLIT_SIZE_128MB);
    }


    public RecordReader<PairOfStringLong, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException {
        return new CombineFileRecordReader<PairOfStringLong, Text>((CombineFileSplit) split, context,
                CustomRecordReader.class);
    }


    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        return false;
    }
}

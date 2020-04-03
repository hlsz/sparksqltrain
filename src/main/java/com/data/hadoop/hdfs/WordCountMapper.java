package com.data.hadoop.hdfs;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;
import java.io.IOException;

/**
  * WordCount Mapper
  *
  */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {


    private int ignoredLength = 3;
    private static final IntWritable one = new IntWritable(1);
    private Text reducerKey = new Text();


    @Override
    protected void setup(Mapper.Context context) throws IOException, InterruptedException {
        this.ignoredLength = context.getConfiguration().getInt("word.count.ignored.length", 3);
    }


    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        if ((line == null) || (line.length() < ignoredLength)) {
            return;
        }


        String[] words = StringUtils.split(line);
        if (words == null) {
            return;
        }


        for (String word : words) {
            if (word.length() < this.ignoredLength) {
                continue;
            }
            if (word.matches(".*[,.;]$")) {
                word = word.substring(0, word.length() - 1);
            }
            reducerKey.set(word);
            context.write(reducerKey, one);
        }
    }


}
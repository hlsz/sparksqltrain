package com.data.hadoop.demo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ChainMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        String year = line.substring(15, 19);

        int temp = Integer.parseInt(line.substring(87, 92));

        context.write(new Text(year), new IntWritable(temp));
    }
}

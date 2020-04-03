package com.data.hadoop.demo;

import au.com.bytecode.opencsv.CSVParser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class EarthQuakeLocationMapper extends
        Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (key.get() > 0) {
            String[] lines = new CSVParser().parseLine(value.toString());
            context.write(new Text(lines[9]), new IntWritable(1));
        }
    }
}


class EarthQuakeLocationReducer extends
        Reducer<Text, IntWritable, Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
         int count = 0;
         for(IntWritable value :values){
             count++;
         }
         if(count >= 10) {
             context.write(key, new IntWritable(count));
         }
    }
}
package com.data.hadoop.demo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ChainReducer2 extends Mapper<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
        int temp = value.get();
        //取奇数气温
        if(temp % 2 == 1){
            context.write(key, new IntWritable(temp));
        }
    }
}

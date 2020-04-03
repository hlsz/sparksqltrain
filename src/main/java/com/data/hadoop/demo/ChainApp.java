package com.data.hadoop.demo;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;

public class ChainApp {

    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///user/analyai/");

        Job job = Job.getInstance(conf);

        FileSystem fs = FileSystem.get(conf);
        job.setJobName("Chain");
        job.setJarByClass(ChainApp.class);

        ChainMapper.addMapper(job, ChainMapper1.class, LongWritable.class, Text.class,
                Text.class, IntWritable.class, conf);

        ChainMapper.addMapper(job, ChainMapper2.class, Text.class, IntWritable.class,
                CompKey.class, NullWritable.class, conf);

        ChainReducer.setReducer(job, ChainReducer1.class, CompKey.class, NullWritable.class,
                Text.class, IntWritable.class, conf);

        ChainReducer.addMapper(job, ChainReducer2.class, Text.class, IntWritable.class,
                Text.class, IntWritable.class, conf);



    }


}

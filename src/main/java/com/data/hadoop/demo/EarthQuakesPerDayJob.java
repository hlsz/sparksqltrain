package com.data.hadoop.demo;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class EarthQuakesPerDayJob {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Job job = new Job();

        job.setJarByClass(EarthQuakesPerDayJob.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(EarthQuakesPerDateMapper.class);
        job.setReducerClass(EarthQuakesPerDateReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}

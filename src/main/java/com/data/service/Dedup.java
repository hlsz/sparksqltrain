package com.data.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class Dedup {

    public static class Map extends Mapper<Object, Text, Text, Text> {

        private static Text line = new Text();

        public void map(Object key, Text value, Context context)
            throws IOException,InterruptedException {
            line = value;
            context.write(line, new Text(""));
        }
    }

    // reduce
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        //实现reduce函数

        public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException,InterruptedException {
            context.write(key, new Text(""));
        }
    }

    public static void main(String[] args)  throws IOException{
        Configuration conf = new Configuration();
        conf.set("mapred.job.tracker", "192.18.1.1.");

        String[] ioArgs = new String[]{"dedup_in", "dedup_out"};
        String[] otherArgs = new GenericOptionsParser(conf, ioArgs).getRemainingArgs();

        if (otherArgs.length != 2) {
            System.err.println("Usage: Data deduplication <in> <out>");
            System.exit(2);
        }

        Job job = new Job(conf, "data Dedupliction");
        job.setJarByClass(Dedup.class);

        //设置 map, combine, reduce处理类
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);

        //设置输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputKeyClass(Text.class);

        //设置输入和输出目录
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(0);

    }

}


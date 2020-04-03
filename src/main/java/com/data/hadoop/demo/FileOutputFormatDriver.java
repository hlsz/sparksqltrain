package com.data.hadoop.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FileOutputFormatDriver extends Configured implements Tool {


    public static void main(String[] args) throws  Exception{
        ToolRunner.run(new Configuration(), new FileOutputFormatDriver(), args);
    }

    @Override
    public int run(String[] arg0) throws Exception {
        if(arg0.length != 3){
            System.err.println("Usage:");
            return -1;
        }
        Configuration conf = getConf();

        Path in = new Path(arg0[0]);
        Path out = new Path(arg0[1]);

        boolean delete = out.getFileSystem(conf)
                .delete(out, true);

        System.out.println("deleted " + out + "?" + delete);
        Job job = Job.getInstance(conf, "fileoutputformat test job");
        job.setJarByClass(getClass());

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(CustomOutputFormat.class);

        job.setMapperClass(Mapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(Integer.parseInt(arg0[2]));
        job.setReducerClass(Reducer.class);
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);

        return job.waitForCompletion(true)?0:-1;

    }
}

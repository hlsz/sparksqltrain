package com.data.hadoop.hdfs;

import com.data.utils.HadoopUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 将小文件合并到大文件的单词计数驱动程序类。
 *
 */
public class CombineSmallFilesDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        long beginTime = System.currentTimeMillis();
        System.exit(ToolRunner.run(new Configuration(), new CombineSmallFilesDriver(), args));
        long elapsedTime = System.currentTimeMillis() - beginTime;
        System.out.println("elapsed time(millis): " + elapsedTime);
    }

    @Override
    public int run(String[] args) throws Exception {
        System.out.println("input path = " + args[0]);
        System.out.println("output path = " + args[1]);

        Configuration conf = getConf();
        Job job = new Job(conf);
        job.setJobName("CombineSmallFilesDriver");

        // 将所有jar文件添加到HDFS的分布式缓存中
        HadoopUtil.addJarsToDistributedCache(job, "/lib/");

        // 定义文件数据格式化
        job.setInputFormatClass(CustomCFIF.class);

        // 定义Output的Key和Value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 定义map和reduce的函数类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        // job.setNumReduceTasks(13);

        // 定义输入/输出路径
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // 提交作业等待完成
        job.submit();
        job.waitForCompletion(true);
        return 0;
    }
}

/**
 * Wordcount Mapper
 */
//public class WordCountMapper extends Mapper<PairOfStringLong, Text, Text, IntWritable> {
//
//    final static IntWritable one = new IntWritable(1);
//    private Text word = new Text();
//
//    public void map(PairOfStringLong key, Text value, Reducer.Context context) throws IOException, InterruptedException {
//        String line = value.toString().trim();
//        String[] tokens = StringUtils.split(line, " ".charAt(0));
//        for (String tok : tokens) {
//            word.set(tok);
//            context.write(word, one);
//        }
//    }
//}

/**
 * Wordcount Reduce
 */
//public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
//
//    public void reduce(Text key, Iterable<IntWritable> values, Context context)
//            throws IOException, InterruptedException {
//        int sum = 0;
//        for (IntWritable val : values) {
//            sum += val.get();
//        }
//        context.write(key, new IntWritable(sum));
//    }
//}
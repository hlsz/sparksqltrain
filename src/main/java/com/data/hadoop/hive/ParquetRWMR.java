package com.data.hadoop.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;

import java.io.IOException;
import java.util.StringTokenizer;


public class ParquetRWMR extends Configured implements Tool {

    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();;
        String writeSchema = "message example {\n" +
                "required binary id;\n" +
                "required binary name;\n" +
                "required binary des;\n" +
                "}";
        conf.set("parquet.example.schema",writeSchema);

        Job job = Job.getInstance(conf);
        job.setJarByClass(ParquetRWMR.class);
        job.setJobName("parquet");

        String in = "/tmp/test/parquet_test";
        String out = "/tmp/test/parquet_test_mr";

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputValueClass(Group.class);

        job.setMapperClass(WordCountMap.class);
        job.setReducerClass(WordCountReduce.class);

        job.setInputFormatClass(ParquetInputFormat.class);
        job.setOutputFormatClass(ParquetOutputFormat.class);

        ParquetInputFormat.setInputPaths(job,new Path(in));
        ParquetInputFormat.setReadSupportClass(job, GroupReadSupport.class);

        ParquetOutputFormat.setOutputPath(job, new Path(out));
        ParquetOutputFormat.setWriteSupportClass(job, GroupWriteSupport.class);

        boolean rt =job.waitForCompletion(true);
        return rt?0:1;
    }

    public static class WordCountMap extends
            Mapper<Void, Group, Text, Text> {

        private Text word = new Text();

        public void map(Void key, Group value, Context context)
                throws IOException, InterruptedException {
            Long first = value.getLong("0",0); //value.getLong方法第一个参数是字段名，如果该参数是key-value类型的，第二个参数传0即可。因为根据key返回的值是一个list，0即是取第一个
            String sec = value.getString("1",0);
            String third = value.getString("2",0);
            word.set(first.toString());
            context.write(word, new Text(sec + "\t" + third));
        }
    }

    public static class WordCountReduce extends
            Reducer<Text, Text, Void, Group> {
        private SimpleGroupFactory factory;

        public void reduce(Text key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            int sum = 0;
            StringBuilder str = new StringBuilder();
            for (Text val : values) {
                String tmp_file[] = val.toString().split("\t");
                Group group = factory.newGroup()
                        .append("id",  key.toString())
                        .append("name", tmp_file[0])
                        .append("des",tmp_file[1]);
                context.write(null,group);
                break;
            }


        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            factory = new SimpleGroupFactory(GroupWriteSupport.getSchema(context.getConfiguration()));

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int retnum = ToolRunner.run(conf,new ParquetRWMR(),args);
    }
}

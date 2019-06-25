package com.data.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MultipleReducer extends
        Reducer<LongWritable, Text, LongWritable, Text> {

    private MultipleOutputs<LongWritable, Text> out;

    @Override
    protected void setup(Context cxt) throws IOException, InterruptedException {
        out = new MultipleOutputs<LongWritable, Text>(cxt);
    }

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context cxt) throws IOException, InterruptedException {
        for(Text v:values){
            if(v.toString().startsWith("ignore")){
                out.write("ignore", key, v, "ign");
            }else{
                out.write("other",key,v,"oth");
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        out.close();
    }
}

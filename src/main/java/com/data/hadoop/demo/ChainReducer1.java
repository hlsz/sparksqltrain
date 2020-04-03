package com.data.hadoop.demo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class ChainReducer1 extends Reducer<CompKey, NullWritable, Text, IntWritable> {

    @Override
    //由于分组对比器设定，相同的year放在一个分组,因此，在一个reduce循环中，得到的数据均为同一年份的数据
    protected void reduce(CompKey key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        String year = key.getYear();
        Iterator<NullWritable> it = values.iterator();
        int i = 0;
        while(it.hasNext()){
            System.out.println(key.toString());
            int temp = key.getTemp();
            context.write(new Text(year), new IntWritable(temp));
            it.next();
            i++;
            if(i>=10){
                break;
            }
        }
    }
}

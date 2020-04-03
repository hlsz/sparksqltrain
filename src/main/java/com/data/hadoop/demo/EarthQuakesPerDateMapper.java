package com.data.hadoop.demo;

import au.com.bytecode.opencsv.CSVParser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;


public class EarthQuakesPerDateMapper extends Mapper<LongWritable,
        Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
       if (key.get() > 0 ) {
           try{
               CSVParser parser = new CSVParser();
               String[] lines = parser.parseLine(value.toString());
               SimpleDateFormat format =
                       new SimpleDateFormat("EEEEE, MMMM dd, yyyy HH:mm:ss Z");
               Date dt = format.parse(lines[3]);
               format.applyPattern("dd-MM-yyyy");

               String dtstr = format.format(dt);
               context.write(new Text(dtstr), new IntWritable(1));

           } catch (ParseException e) {
               e.printStackTrace();
           }
       }
    }

    private final String LINE = "ci,14897012,2,\"Monday, December 13, 2010 " +
            "14:10:32 UTC\",33.0290,-115." +
            "5388,1.9,15.70,41,\"Southern California\"";


}

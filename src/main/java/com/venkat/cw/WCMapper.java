package com.venkat.cw;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WCMapper extends Mapper<LongWritable, Text, WCWritable, IntWritable> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

        String line = value.toString();

        String[] words=line.split(",");

        for(String word: words ){

            WCWritable outputKey = new WCWritable(word.toUpperCase().trim());

            IntWritable outputValue = new IntWritable(1);

            context.write(outputKey, outputValue);
        }
    }

}

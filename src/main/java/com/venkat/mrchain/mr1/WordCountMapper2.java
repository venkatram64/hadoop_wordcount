package com.venkat.mrchain.mr1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper2 extends Mapper<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
        String lowerCaseWrod = key.toString().toLowerCase();
        context.write(new Text(lowerCaseWrod),value);
    }
}

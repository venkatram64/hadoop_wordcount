package com.venkat.mrchain.mr2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Job2Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] words = value.toString().split("\\s+");
        String firstCharacter = words[0].substring(0,1);
        context.write(new Text(firstCharacter), new IntWritable(Integer.parseInt(words[1])));
    }
}

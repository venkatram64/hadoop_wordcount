package com.venkat.mf;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CommaSeparatorMapper extends Mapper<Text, Text,Text, IntWritable> {

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

        context.write(value, new IntWritable(1));
    }
}

package com.venkat.mfo;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MultifileOutMapper extends Mapper<LongWritable, Text, Text,Text> {

    private Text empId = new Text();
    private Text empData = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] words = line.split(",");

        empId.set(words[0]);
        empData.set(words[1] + "," + words[2] + "," + words[3]);
        context.write(empId, empData);
    }
}

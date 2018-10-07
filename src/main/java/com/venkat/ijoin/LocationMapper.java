package com.venkat.ijoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;

import java.io.IOException;


public class LocationMapper extends Mapper<LongWritable,Text,Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString().trim();
        String[] address = line.split(",");
        context.write(new Text(address[0]), new Text("Address,"+address[1]));
    }
}

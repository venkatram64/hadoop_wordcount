package com.venkat.fb;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FBMapper extends Mapper<LongWritable, Text, Text, Text> {
    //FKLY490998LB,2010-01-29 06:12:17,Mumbai,Ecommerce,39,13,25-35
    public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {

        String line = values.toString();
        String[] words = line.split(",");

        String category = words[3];
        String info = words[2] + "," + words[4] + "," + words[5];

        context.write(new Text(category), new Text(info));
    }
}

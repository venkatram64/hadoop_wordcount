package com.venkat.ojoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class DeptMapper extends Mapper<LongWritable,Text,Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString().trim();
        String[] dept = line.split(",");
        context.write(new Text(dept[0]), new Text("Department,"+dept[1] +" " + dept[2]));
    }
}

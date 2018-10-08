package com.venkat.ojoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class EmpMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString().trim();
        String[] emp = line.split(",");
        context.write(new Text(emp[5]),new Text("Employee," + emp[0] + "  " + emp[1] + "  " + emp[2] + "  " + emp[3] + "  " + emp[4]));
    }
}

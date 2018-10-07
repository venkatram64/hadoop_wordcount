package com.venkat.sum;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SumMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
        String[] data = values.toString().split(",");

        for(String num: data){
            int number = Integer.parseInt(num);
            if(number%2 == 0){
                context.write(new Text("Even"), new IntWritable(number));
            }else{
                context.write(new Text("Odd"), new IntWritable(number));
            }
        }
    }
}

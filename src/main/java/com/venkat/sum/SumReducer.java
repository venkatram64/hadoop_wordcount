package com.venkat.sum;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{

        int sum = 0;

        if(key.equals("Even")){
            for(IntWritable value: values){
                sum += value.get();
            }
        }else{
            for(IntWritable value: values){
                sum += value.get();
            }
        }

        context.write(key, new IntWritable(sum));
    }
}

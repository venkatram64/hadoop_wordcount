package com.venkat.wc2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text word, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{

        int sum = 0;

        for(IntWritable value : values){
            sum += value.get();
        }
        context.write(word, new IntWritable(sum));
    }
}

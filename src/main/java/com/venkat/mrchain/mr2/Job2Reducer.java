package com.venkat.mrchain.mr2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Job2Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int characterCount = 0;
        for(IntWritable count : values){
            characterCount += count.get();
        }
        context.write(key, new IntWritable(characterCount));
    }
}

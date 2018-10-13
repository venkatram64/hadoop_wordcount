package com.venkat.chain;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text, IntWritable,Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int totalWordFrequency = 0;

        for(IntWritable count : values){
            totalWordFrequency += count.get();
        }

        context.write(key, new IntWritable(totalWordFrequency));
    }
}

package com.venkat.seq;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class SeqReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0;
        if(key.equals("ODD")){
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

package com.venkat.cw;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WCReducer extends Reducer<WCWritable, IntWritable, WCWritable, IntWritable> {

    public void reduce(WCWritable word, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{

        int sum = 0;

        for(IntWritable value : values){
            sum += value.get();
        }
        context.write(word, new IntWritable(sum));
    }
}

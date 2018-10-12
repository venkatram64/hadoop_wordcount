package com.venkat.counters;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class CounterReducer extends Reducer<Text, Text, Text, IntWritable> {

    //Hyderabad [{39,3} {54.13} {9.5} {39,6} .....]

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //Hyderabad [{39,3} {54.13} {9.5} {39,6} ...]
        int totalRevenue = 0;
        Iterator<Text> iterator = values.iterator();

        while(iterator.hasNext()){
            String[] data = iterator.next().toString().split(",");

            int price = Integer.parseInt(data[0]);
            int sales = Integer.parseInt(data[1]);

            totalRevenue += price * sales;
        }
        context.write(key, new IntWritable(totalRevenue));
    }
}

package com.venkat.mjoin;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class MapJoinReducer extends Reducer<Text, IntWritable,Text,IntWritable> {

    //STR_1 Banglore [{280} {560} {456} .......]
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int totalRevenue = 0;
        Iterator<IntWritable> iterator = values.iterator();
        while(iterator.hasNext()){
            int revenue = iterator.next().get();
            totalRevenue += revenue;
        }
        context.write(key, new IntWritable(totalRevenue));
    }
}

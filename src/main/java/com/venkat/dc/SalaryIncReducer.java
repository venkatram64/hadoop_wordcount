package com.venkat.dc;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class SalaryIncReducer extends Reducer<Text, DoubleWritable,Text,DoubleWritable> {

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

        double totalIncrement = 0;
        int count = 0;
        Iterator<DoubleWritable> iterator = values.iterator();
        while(iterator.hasNext()){
            count++;
            totalIncrement += iterator.next().get();
        }

        double avgInc = totalIncrement/count;
        context.write(key, new DoubleWritable(avgInc));
    }
}

package com.venkat.seq;

import com.google.common.primitives.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.yarn.proto.ApplicationClientProtocol;

import java.io.IOException;

public class SeqMapper extends Mapper<LongWritable, BytesWritable, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {

        String str = new String(value.getBytes(), "UTF-8");
        String[] data = str.toString().split(",");

        for(String num : data){
            int number = Integer.parseInt(num.trim());
            if(number % 2 == 1){
                context.write(new Text("ODD"), new IntWritable(number));
            }else{
                context.write(new Text("EVEN"), new IntWritable(number));
            }
        }

    }
}

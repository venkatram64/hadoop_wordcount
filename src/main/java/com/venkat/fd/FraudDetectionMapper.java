package com.venkat.fd;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FraudDetectionMapper extends Mapper<LongWritable, Text, Text, FraudDetectionWritable> {

    private Text custId = new Text();
    private FraudDetectionWritable data = new FraudDetectionWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] words = line.split(",");
        custId.set(words[0]);
        data.set(words[1], words[5], words[6], words[7]);
        context.write(custId, data);
    }
}

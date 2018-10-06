package com.venkat.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Mapper;

import java.io.IOException;

public class WordCountMapper extends MapReduceBase implements Mapper<LongWritable,Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value,
                       OutputCollector<Text,IntWritable> output, Reporter r) throws IOException {
        String s = value.toString();
        for(String word: s.split(" ")){
            output.collect(new Text(word), new IntWritable(1));
        }
    }
}

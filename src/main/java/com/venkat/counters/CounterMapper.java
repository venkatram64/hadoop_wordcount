package com.venkat.counters;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//static counters
enum Location {
    TOTAL, BANGALORE, CHENNAI, HYDERABAD
}

public class CounterMapper extends Mapper<LongWritable, Text,Text,Text> {

    private Text storeLocation = new Text();
    private Text data = new Text();
    //12,sofa,39,3,hyderabad

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        context.getCounter(Location.TOTAL).increment(1);

        String line = value.toString();
        String[] words = line.split(",");

        if(words[4].equalsIgnoreCase("Bangalore")){
            context.getCounter(Location.BANGALORE).increment(1);
        }else if(words[4].equalsIgnoreCase("Chennai")){
            context.getCounter(Location.CHENNAI).increment(1);
        }else if(words[4].equalsIgnoreCase("Hyderabad")){
            context.getCounter(Location.HYDERABAD).increment(1);
        }else{
            throw new RuntimeException("No such city");
        }

        int salesCount = Integer.parseInt(words[3]);
        if(salesCount < 10){
            context.getCounter("SALES","LOW_SALES").increment(1);
        }

        int price = Integer.parseInt(words[2]);
        if(salesCount*price > 500){
            context.getCounter("SALES","HIGH_REVENUE").increment(1);
        }

        storeLocation.set(words[4]);
        data.set(words[2] + "," + words[3]);
        context.write(storeLocation, data);
    }
}

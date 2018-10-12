package com.venkat.counters;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CounterDriver {

    //bin/hdfs dfs -mkdir /user/venkat/mc
    //bin/hdfs dfs -put /home/venkatram/IdeaProjects/wordcount/counters.txt /user/venkat/mc/
    //bin/hdfs dfs -ls  /user/venkat/mc/
    //bin/hadoop jar /home/venkatram/IdeaProjects/wordcount/target/wordcount-1.0-SNAPSHOT.jar com.venkat.counter.CounterDriver
    //bin/hdfs dfs -cat /user/venkat/mc/output/*

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();

        Path input = new Path("hdfs://localhost:9000/user/venkat/mc/counters.txt");

        Path output = new Path("hdfs://localhost:9000/user/venkat/mc/output");

        Job job = new Job(conf, "CounterDriver Analysis");

        job.setJarByClass(CounterDriver.class);
        job.setMapperClass(CounterMapper.class);
        job.setReducerClass(CounterReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job,input);
        FileOutputFormat.setOutputPath(job, output);

        output.getFileSystem(job.getConfiguration()).delete(output, true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

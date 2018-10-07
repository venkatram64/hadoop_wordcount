package com.venkat.mfo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class MultifileOutDriver {

    //bin/hdfs dfs -mkdir /user/venkat/mfo
    //bin/hdfs dfs -put /home/venkatram/IdeaProjects/wordcount/mfo.txt /user/venkat/mfo/
    //bin/hdfs dfs -ls  /user/venkat/mfo/
    //bin/hadoop jar /home/venkatram/IdeaProjects/wordcount/target/wordcount-1.0-SNAPSHOT.jar com.venkat.mfo.MultifileOutDriver
    //bin/hdfs dfs -cat /user/venkat/mfo/output/*

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();

        Path input = new Path("hdfs://localhost:9000/user/venkat/mfo/mfo.txt");

        Path output = new Path("hdfs://localhost:9000/user/venkat/mfo/output");

        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");

        Job job=new Job(conf,"MultifileOut Analysis");

        job.setJarByClass(MultifileOutDriver.class);
        job.setMapperClass(MultifileOutMapper.class);
        job.setReducerClass(MultifileOutReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, input);

        MultipleOutputs.addNamedOutput(job,"HR", TextOutputFormat.class, Text.class, LongWritable.class);

        MultipleOutputs.addNamedOutput(job,"Accounts", TextOutputFormat.class, Text.class, LongWritable.class);

        FileOutputFormat.setOutputPath(job, output);

        output.getFileSystem(job.getConfiguration()).delete(output,true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

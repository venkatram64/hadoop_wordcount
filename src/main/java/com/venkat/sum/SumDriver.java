package com.venkat.sum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class SumDriver {

    //bin/hdfs dfs -mkdir  /user/venkat/odd_even
    //bin/hdfs dfs -put /home/venkatram/IdeaProjects/wordcount/odd_even.txt /user/venkat/odd_even/odd_even.txt
    //bin/hdfs dfs -ls  /user/venkat/odd_even/
    //bin/hadoop jar /home/venkatram/IdeaProjects/wordcount/target/wordcount-1.0-SNAPSHOT.jar com.venkat.sum.SumDriver /user/venkat/odd_even/ /user/venkat/odd_even/output
    //bin/hdfs dfs -cat /user/venkat/odd_even/output/*

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        String[] files = new GenericOptionsParser(conf,args).getRemainingArgs();

        if(files.length < 2){
            System.out.println("give input and output directories.");
            return ;
        }

        Path input=new Path(files[0]);
        Path output=new Path(files[1]);

        Job job=new Job(conf,"summingNumbers");

        job.setJarByClass(SumDriver.class);
        job.setMapperClass(SumMapper.class);
        job.setReducerClass(SumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}

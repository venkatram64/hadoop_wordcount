package com.venkat.fb;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class FBDriver {

    //bin/hdfs dfs -mkdir  /user/venkat/fb
    //bin/hdfs dfs -put /home/venkatram/IdeaProjects/wordcount/odd_even.txt /user/venkat/fb/fb.txt
    //bin/hdfs dfs -ls  /user/venkat/fb/
    //bin/hadoop jar /home/venkatram/IdeaProjects/wordcount/target/wordcount-1.0-SNAPSHOT.jar com.venkat.fb.FBDriver /user/venkat/fb/ /user/venkat/fb/output
    //bin/hdfs dfs -cat /user/venkat/fb/output/*

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        String[] files = new GenericOptionsParser(conf,args).getRemainingArgs();

        if(files.length < 2){
            System.out.println("give input and output directories.");
            return ;
        }

        Path input=new Path(files[0]);
        Path output=new Path(files[1]);

        Job job=new Job(conf,"FB");

        job.setJarByClass(FBDriver.class);
        job.setMapperClass(FBMapper.class);
        job.setReducerClass(FBReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

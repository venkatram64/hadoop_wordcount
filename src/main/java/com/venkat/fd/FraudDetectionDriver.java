package com.venkat.fd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class FraudDetectionDriver {

    //bin/hdfs dfs -put /home/venkatram/IdeaProjects/wordcount/wc_v2.txt /user/venkat/fd/
    //bin/hdfs dfs -ls  /user/venkat/fd/
    //bin/hadoop jar /home/venkatram/IdeaProjects/wordcount/target/wordcount-1.0-SNAPSHOT.jar com.venkat.fd.FraudDetectionDriver /user/venkat/fd/ /user/venkat/fd/output
    //bin/hdfs dfs -cat /user/venkat/fd/output/*

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        String[] files = new GenericOptionsParser(conf,args).getRemainingArgs();

        if(files.length < 2){
            System.out.println("give input and output directories.");
            return ;
        }

        Path input=new Path(files[0]);

        Path output=new Path(files[1]);

        Job job=new Job(conf,"Fraud Analysis");

        job.setJarByClass(FraudDetectionDriver.class);
        job.setMapperClass(FraudDetectionMapper.class);
        job.setReducerClass(FraudDetectionReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FraudDetectionWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        output.getFileSystem(job.getConfiguration()).delete(output,true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

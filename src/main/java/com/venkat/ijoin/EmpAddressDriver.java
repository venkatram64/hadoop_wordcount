package com.venkat.ijoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class EmpAddressDriver {

    //bin/hdfs dfs -mkdir /user/venkat/ij
    //bin/hdfs dfs -put /home/venkatram/IdeaProjects/wordcount/ij.txt /user/venkat/ij/
    //bin/hdfs dfs -put /home/venkatram/IdeaProjects/wordcount/ij2.txt /user/venkat/ij/
    //bin/hdfs dfs -ls  /user/venkat/ij/
    //bin/hadoop jar /home/venkatram/IdeaProjects/wordcount/target/wordcount-1.0-SNAPSHOT.jar com.venkat.ij.EmpAddressDriver
    //bin/hdfs dfs -cat /user/venkat/ij/output/*

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();

        Path input = new Path("hdfs://localhost:9000/user/venkat/ij/ij.txt");
        Path input2 = new Path("hdfs://localhost:9000/user/venkat/ij/ij2.txt");

        Path output = new Path("hdfs://localhost:9000/user/venkat/ij/output");

        Job job=new Job(conf,"EmpAddressDriver Analysis");

        job.setJarByClass(EmpAddressDriver.class);
        job.setMapperClass(LocationMapper.class);
        job.setMapperClass(EmpMapper.class);
        job.setReducerClass(EmpAddressReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job,input, TextInputFormat.class, EmpMapper.class);
        MultipleInputs.addInputPath(job,input2, TextInputFormat.class, LocationMapper.class);

        FileOutputFormat.setOutputPath(job, output);
        output.getFileSystem(job.getConfiguration()).delete(output,true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

package com.venkat.mf;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MultifileDriver {

    //bin/hdfs dfs -mkdir /user/venkat/mf
    //bin/hdfs dfs -put /home/venkatram/IdeaProjects/wordcount/word1.txt /user/venkat/mf/
    //bin/hdfs dfs -put /home/venkatram/IdeaProjects/wordcount/word2.txt /user/venkat/mf/
    //bin/hdfs dfs -ls  /user/venkat/mf/
    //bin/hadoop jar /home/venkatram/IdeaProjects/wordcount/target/wordcount-1.0-SNAPSHOT.jar com.venkat.mf.MultfileDriver
    //bin/hdfs dfs -cat /user/venkat/mf/output/*

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();

        Path input = new Path("hdfs://localhost:9000/user/venkat/mf/word1.txt");

        Path input2 = new Path("hdfs://localhost:9000/user/venkat/mf/word2.txt");

        Path output = new Path("hdfs://localhost:9000/user/venkat/mf/output");

        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");

        Job job=new Job(conf,"Multifile Analysis");

        job.setJarByClass(MultifileDriver.class);
        job.setMapperClass(SpaceSeparatorMapper.class);
        job.setMapperClass(CommaSeparatorMapper.class);
        job.setReducerClass(TextReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        MultipleInputs.addInputPath(job, input, TextInputFormat.class, SpaceSeparatorMapper.class);
        MultipleInputs.addInputPath(job, input2, KeyValueTextInputFormat.class,CommaSeparatorMapper.class);

        FileOutputFormat.setOutputPath(job, output);

        output.getFileSystem(job.getConfiguration()).delete(output,true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

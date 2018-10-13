package com.venkat.chain;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {

    //bin/hdfs dfs -mkdir /user/venkat/chain
    //bin/hdfs dfs -put /home/venkatram/IdeaProjects/wordcount/counters.txt /user/venkat/chain/
    //bin/hdfs dfs -ls  /user/venkat/chain/
    //bin/hadoop jar /home/venkatram/IdeaProjects/wordcount/target/wordcount-1.0-SNAPSHOT.jar com.venkat.chain.WordCountDriver
    //bin/hdfs dfs -cat /user/venkat/chain/output/*

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();

        Path input = new Path("hdfs://localhost:9000/user/venkat/chain/word2.txt");

        Path output = new Path("hdfs://localhost:9000/user/venkat/chain/output");

        Job job = new Job(conf, "WordCountDriver Analysis");

        job.setJarByClass(WordCountDriver.class);

        ChainMapper.addMapper(job,
                WordCountMapper.class,
                LongWritable.class,
                Text.class,
                Text.class,
                IntWritable.class,
                conf
                );

        ChainMapper.addMapper(job,
                WordCountMapper2.class,
                Text.class,
                IntWritable.class,
                Text.class,
                IntWritable.class,
                conf
        );

        ChainReducer.setReducer(job,
                WordCountReducer.class,
                Text.class,
                IntWritable.class,
                Text.class,
                IntWritable.class,
                conf
        );

        FileInputFormat.addInputPath(job,input);
        FileOutputFormat.setOutputPath(job, output);

        output.getFileSystem(job.getConfiguration()).delete(output, true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

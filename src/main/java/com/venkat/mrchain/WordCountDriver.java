package com.venkat.mrchain;


import com.venkat.mrchain.mr1.WordCountMapper;
import com.venkat.mrchain.mr1.WordCountMapper2;
import com.venkat.mrchain.mr1.WordCountReducer;
import com.venkat.mrchain.mr2.Job2Mapper;
import com.venkat.mrchain.mr2.Job2Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {

    //bin/hdfs dfs -mkdir /user/venkat/chain
    //bin/hdfs dfs -put /home/venkatram/IdeaProjects/wordcount/counters.txt /user/venkat/chain/
    //bin/hdfs dfs -ls  /user/venkat/chain/
    //bin/hadoop jar /home/venkatram/IdeaProjects/wordcount/target/wordcount-1.0-SNAPSHOT.jar com.venkat.mrchain.WordCountDriver
    //bin/hdfs dfs -cat /user/venkat/chain/output_job1/*
    //bin/hdfs dfs -cat /user/venkat/chain/output/*

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();

        Path input = new Path("hdfs://localhost:9000/user/venkat/chain/word2.txt");

        Path output = new Path("hdfs://localhost:9000/user/venkat/chain/output");

        Job job = new Job(conf, "first Job");

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
        FileOutputFormat.setOutputPath(job, new Path(output +"_job1"));

        output.getFileSystem(job.getConfiguration()).delete(new Path(output+"_job1"), true);

        if(!job.waitForCompletion(true) ){
            System.out.println("Error completing first job ");
            System.exit(1);
        }

        //Second Job

        Configuration conf2 = new Configuration();
        Job job2 = new Job(conf2, "Chain job2");
        job2.setJarByClass(WordCountDriver.class);

        job2.setMapperClass(Job2Mapper.class);
        job2.setReducerClass(Job2Reducer.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        /* Set input path: Read from from first one's output path*/

        FileInputFormat.addInputPath(job2, new Path(output +"_job1/part-r-00000"));
        FileOutputFormat.setOutputPath(job2, output);
        output.getFileSystem(job2.getConfiguration()).delete(output,true);

        job2.waitForCompletion(true);

    }
}

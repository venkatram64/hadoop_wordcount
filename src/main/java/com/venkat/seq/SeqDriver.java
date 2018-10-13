package com.venkat.seq;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class SeqDriver {

    //bin/hdfs dfs -mkdir /user/venkat/seq
    //bin/hdfs dfs -put /home/venkatram/IdeaProjects/wordcount/counters.txt /user/venkat/seq/
    //bin/hdfs dfs -ls  /user/venkat/seq/
    //bin/hadoop jar /home/venkatram/IdeaProjects/wordcount/target/wordcount-1.0-SNAPSHOT.jar com.venkat.seq.CounterDriver
    //bin/hdfs dfs -cat /user/venkat/seq/output/*

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();

        Path input = new Path("hdfs://localhost:9000/user/venkat/seq/SeqDriver.txt");

        Path output = new Path("hdfs://localhost:9000/user/venkat/seq/output");

        Job job = new Job(conf, "SeqDriver Analysis");

        job.setJarByClass(SeqDriver.class);
        job.setMapperClass(SeqMapper.class);
        job.setReducerClass(SeqReducer.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        //optimizations
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        //optimization, compress sequence file
        FileOutputFormat.setCompressOutput(job,true);
        FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

        FileInputFormat.addInputPath(job,input);
        FileOutputFormat.setOutputPath(job, output);

        output.getFileSystem(job.getConfiguration()).delete(output, true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

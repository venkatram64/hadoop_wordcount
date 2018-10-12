package com.venkat.ciw;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class XMLRecordDriver {

    //bin/hdfs dfs -mkdir /user/venkat/ciw
    //bin/hdfs dfs -put /home/venkatram/IdeaProjects/wordcount/counters.txt /user/venkat/ciw/
    //bin/hdfs dfs -ls  /user/venkat/ciw/
    //bin/hadoop jar /home/venkatram/IdeaProjects/wordcount/target/wordcount-1.0-SNAPSHOT.jar com.venkat.ciw.XMLRecordDriver
    //bin/hdfs dfs -cat /user/venkat/ciw/output/*

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();

        Path input = new Path("hdfs://localhost:9000/user/venkat/ciw/xinput.xml");

        Path output = new Path("hdfs://localhost:9000/user/venkat/ciw/output");

        Job job = new Job(conf, "CounterDriver Analysis");

        job.setInputFormatClass(XMLInputFormat.class);
        job.setJarByClass(XMLRecordDriver.class);
        job.setMapperClass(XMLRecordMapper.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job,input);
        FileOutputFormat.setOutputPath(job, output);

        output.getFileSystem(job.getConfiguration()).delete(output, true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

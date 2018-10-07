package com.venkat.dc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class SalaryIncDriver {

    //bin/hdfs dfs -put /home/venkatram/IdeaProjects/wordcount/emp.txt /user/venkat/emp/
    //bin/hdfs dfs -put /home/venkatram/IdeaProjects/wordcount/designation.txt /user/venkat/emp/
    //bin/hdfs dfs -ls  /user/venkat/emp/
    //bin/hadoop jar /home/venkatram/IdeaProjects/wordcount/target/wordcount-1.0-SNAPSHOT.jar com.venkat.dc.SalaryIncDriver
    //bin/hdfs dfs -cat /user/venkat/emp/output/*

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {

        Configuration conf = new Configuration();

        Path input=new Path("hdfs://localhost:9000/user/venkat/emp/emp.txt");

        Path output=new Path("hdfs://localhost:9000/user/venkat/emp/output");

        DistributedCache.addCacheFile(new URI("hdfs://localhost:9000/user/venkat/emp/designation.txt"),conf);

        Job job=new Job(conf,"Employe Salary Analysis");

        job.setJarByClass(SalaryIncDriver.class);
        job.setMapperClass(SalaryIncMapper.class);
        job.setReducerClass(SalaryIncReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        output.getFileSystem(job.getConfiguration()).delete(output,true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

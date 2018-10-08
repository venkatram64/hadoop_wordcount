package com.venkat.mjoin;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class MapJoinDriver {

    //bin/hdfs dfs -mkdir /user/venkat/mj
    //bin/hdfs dfs -put /home/venkatram/IdeaProjects/wordcount/sales.txt /user/venkat/mj/
    //bin/hdfs dfs -put /home/venkatram/IdeaProjects/wordcount/store.txt /user/venkat/mj/
    //bin/hdfs dfs -put /home/venkatram/IdeaProjects/wordcount/products.txt /user/venkat/mj/
    //bin/hdfs dfs -ls  /user/venkat/mj/
    //bin/hadoop jar /home/venkatram/IdeaProjects/wordcount/target/wordcount-1.0-SNAPSHOT.jar com.venkat.mjoin.MapJoinDriver
    //bin/hdfs dfs -cat /user/venkat/mj/output/*

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {

        Configuration conf = new Configuration();

        Path input = new Path("hdfs://localhost:9000/user/venkat/mj/sales.txt");
        Path output = new Path("hdfs://localhost:9000/user/venkat/mj/output");

        DistributedCache.addCacheFile(new URI("hdfs://localhost:9000/user/venkat/mj/store.txt"),conf);
        DistributedCache.addCacheFile(new URI("hdfs://localhost:9000/user/venkat/mj/products.txt"),conf);

        Job job=new Job(conf,"MapJoinDriver Analysis");

        job.setJarByClass(MapJoinDriver.class);
        job.setMapperClass(MapJoinMapper.class);
        job.setReducerClass(MapJoinReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        output.getFileSystem(job.getConfiguration()).delete(output,true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

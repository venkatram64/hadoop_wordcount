package com.venkat.ojoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class EmpDeptDriver {

    //bin/hdfs dfs -mkdir /user/venkat/oj
    //bin/hdfs dfs -put /home/venkatram/IdeaProjects/wordcount/emp_oj.txt /user/venkat/oj/
    //bin/hdfs dfs -put /home/venkatram/IdeaProjects/wordcount/dept_oj.txt /user/venkat/oj/
    //bin/hdfs dfs -ls  /user/venkat/oj/
    //bin/hadoop jar /home/venkatram/IdeaProjects/wordcount/target/wordcount-1.0-SNAPSHOT.jar com.venkat.ojoin.EmpDeptDriver
    //bin/hdfs dfs -cat /user/venkat/oj/output/*

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();

        Path input = new Path("hdfs://localhost:9000/user/venkat/oj/emp_oj.txt");
        Path input2 = new Path("hdfs://localhost:9000/user/venkat/oj/dept_oj.txt");

        Path output = new Path("hdfs://localhost:9000/user/venkat/oj/output");

        Job job=new Job(conf,"EmpDeptDriver Analysis");

        job.setJarByClass(EmpDeptDriver.class);
        job.setMapperClass(DeptMapper.class);
        job.setMapperClass(EmpMapper.class);
        job.setReducerClass(EmpDeptReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job,input, TextInputFormat.class, EmpMapper.class);
        MultipleInputs.addInputPath(job,input2, TextInputFormat.class, DeptMapper.class);

        FileOutputFormat.setOutputPath(job, output);
        output.getFileSystem(job.getConfiguration()).delete(output,true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

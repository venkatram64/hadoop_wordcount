package com.venkat.wc;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new WordCountDriver(),args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] strings) throws Exception {
        if(strings.length < 2){
            System.out.println("give input and output directories.");
            return -1;
        }

        JobConf conf = new JobConf(WordCountDriver.class);
        FileInputFormat.setInputPaths(conf, new Path(strings[0]));
        FileOutputFormat.setOutputPath(conf, new Path(strings[1]));

        conf.setMapperClass(WordCountMapper.class);
        conf.setReducerClass(WordCountReducer.class);

        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(IntWritable.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        JobClient.runJob(conf);
        return 0;
    }
}

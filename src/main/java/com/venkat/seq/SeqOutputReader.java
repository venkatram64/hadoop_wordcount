package com.venkat.seq;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Reader;

public class SeqOutputReader {

    public static void main(String[] args) {

        Configuration conf = new Configuration();
        Path path = new Path(args[0]);
        Reader reader = null;

        try{
            //reader = new Reader(conf, Reader.file(path));
            Text key = new Text();
            IntWritable value = new IntWritable();
            while(reader.next(key, value)){
                System.out.println(key + ", " + value.toString());
            }

        }catch (Exception e){
            e.printStackTrace();
        }
    }
}

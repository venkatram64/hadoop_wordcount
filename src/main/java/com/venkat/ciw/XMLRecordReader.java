package com.venkat.ciw;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;

public class XMLRecordReader extends RecordReader<LongWritable, Text> {

    private final String startTag = "<MOVIES>";
    private final String endTag = "</MOVIES>";

    private LineReader lineReader;

    private long currentPosition = 0;
    private long startOfFile;
    private long endOfFile;

    private LongWritable key = new LongWritable();
    private Text value = new Text();

    public XMLRecordReader() {
        super();
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        FileSplit fileSplit = (FileSplit)inputSplit;
        Configuration conf = taskAttemptContext.getConfiguration();

        startOfFile = fileSplit.getStart();
        endOfFile = startOfFile + fileSplit.getLength();

        Path file = fileSplit.getPath();
        FileSystem fileSystem = file.getFileSystem(conf);

        FSDataInputStream fsDataInputStream = fileSystem.open(fileSplit.getPath());
        fsDataInputStream.seek(startOfFile);

        lineReader = new LineReader(fsDataInputStream,conf);
        this.currentPosition = startOfFile;

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        key.set(currentPosition);
        value.clear();

        Text line = new Text();
        boolean startFound = false;

        while(currentPosition <endOfFile){
            long lineLength = lineReader.readLine(line);
            currentPosition = currentPosition + lineLength;

            if(!startFound && line.toString().equalsIgnoreCase(this.startTag)){

                startFound = true;
            }else if(!startFound && line.toString().equalsIgnoreCase(this.endTag)){

                String withOutComma = value.toString().substring(0, value.toString().length() - 1);
                value.set(withOutComma);
                return true;
            }else if(startFound){

                String s = line.toString();
                String content = s.replaceAll("<[^>]+>","");
                value.append(content.getBytes("utf-8"),0,content.length());
                value.append(",".getBytes("utf-8"),0,",".length());

            }
        }

        return false;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {

        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {

        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {

        return (currentPosition - startOfFile)/(float)(endOfFile - startOfFile);
    }

    @Override
    public void close() throws IOException {

        if(lineReader != null){
            lineReader.close();
        }
    }
}

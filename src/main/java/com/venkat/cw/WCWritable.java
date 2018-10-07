package com.venkat.cw;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WCWritable implements WritableComparable<WCWritable> {

    private String word;

    public WCWritable(){
        this.set("");
    }

    public WCWritable(String word){
        this.set(word);
    }

    public void set(String word){
        this.word = word;
    }

    public String getWord(){
        return this.word;
    }


    @Override
    public int compareTo(WCWritable wcWritable) {
        return this.word.compareTo(wcWritable.getWord());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeString(dataOutput,this.word);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.word = WritableUtils.readString(dataInput);
    }

    @Override
    public String toString(){
        return this.word;
    }
}

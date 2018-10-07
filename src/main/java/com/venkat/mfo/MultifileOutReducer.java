package com.venkat.mfo;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class MultifileOutReducer extends Reducer<Text, Text, Text, Text> {

    private MultipleOutputs<Text, Text> out;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        out = new MultipleOutputs<>(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        int totalSalary = 0;
        String dept = "";
        String name = "";

        Iterator<Text> iterator = values.iterator();

        while(iterator.hasNext()){
            String[] data = iterator.next().toString().split(",");
            name = data[0];
            dept = data[1];
            totalSalary += Integer.parseInt(data[2]);
        }

        if(dept.equalsIgnoreCase("hr")){
            out.write("HR",key, new Text(name + "," + totalSalary));
        }else if(dept.equalsIgnoreCase("accounts")){
            out.write("Accounts",key, new Text(name + "," + totalSalary));
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        out.close();
    }
}

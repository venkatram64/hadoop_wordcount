package com.venkat.dc;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

public class SalaryIncMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private HashMap<String, Double> desgMap = new HashMap<>();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //read data from distributed cache

        BufferedReader br = null;
        Path[] localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
        String record = "";

        for(Path path : localFiles){
            if(path.getName().toString().trim().equals("designation.txt")){
                br = new BufferedReader(new FileReader(path.toString()));
                record = br.readLine();
                while(record != null){
                    String[] data = record.split(",");
                    desgMap.put(data[0].trim(), Double.parseDouble(data[1].trim()));
                    record = br.readLine();
                }
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] words = line.split(",");

        String designation = words[2];
        double n = 1;

        if(designation.toString().equalsIgnoreCase("manager")){
            n = desgMap.get("MGR");
        }else if(designation.toString().equalsIgnoreCase("developer")){
            n = desgMap.get("DLP");
        }else if(designation.toString().equalsIgnoreCase("hr")){
            n = desgMap.get("HR");
        }else{
            System.out.println("Invalid designation.");
        }

        int currentSalary = Integer.parseInt(words[3].trim());
        double increment = (n/100) * currentSalary;
        context.write(new Text(designation), new DoubleWritable(increment));
    }
}

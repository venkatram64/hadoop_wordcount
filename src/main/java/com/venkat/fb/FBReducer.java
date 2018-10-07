package com.venkat.fb;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class FBReducer extends Reducer<Text, Text, Text, Text> {
    //Ecommerce [{Mumbai,39,13} {Mumabai,281,5} {Delhi,341,9} {Delhi 398,10}......]
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

        HashMap<String,String> cityData = new HashMap<>();
        Iterator<Text> iter = values.iterator();

        while(iter.hasNext()){
            String line = iter.next().toString();
            String[] words = line.split(",");

            String location = words[0].trim();
            int clickCount = Integer.parseInt(words[1]);
            int conversionCount = Integer.parseInt(words[2]);

            Double successRate = new Double (conversionCount/(clickCount*1.0) * 100);

            if(cityData.containsKey(location)){
                String s1 = cityData.get(location);
                String[] hvalues = s1.split(",");
                Double totalSuccessRate = Double.parseDouble(hvalues[1]) + successRate;
                int totalCount = Integer.parseInt(hvalues[1]) + 1;
                cityData.put(location,totalSuccessRate + "," + totalCount);
            }else{
                cityData.put(location, successRate + ",1");
            }
        }

        System.out.println(cityData.toString());

        for(Map.Entry<String,String> e : cityData.entrySet()){
            String[] v = e.getValue().split(",");
            Double avgSuccessRate = Double.parseDouble(v[0])/Integer.parseInt(v[1]);
            context.write(key,new Text(e.getKey() + "," + avgSuccessRate));
        }
    }
}

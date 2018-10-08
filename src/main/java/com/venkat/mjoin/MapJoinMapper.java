package com.venkat.mjoin;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

public class MapJoinMapper extends Mapper<LongWritable,Text,Text, IntWritable> {

    private HashMap<String,String> stores = new HashMap<>();
    private HashMap<String,String> products = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        BufferedReader br = null;
        Path[] localCacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
        String line = "";
        for(Path path: localCacheFiles){
            if(path.getName().toString().trim().equals("store.txt")){
                br = new BufferedReader(new FileReader(path.toString()));
                line = br.readLine();
                while(line != null){
                    String[] sd = line.split(","); //[{STR_1} {Bangalore} {Walmart}]
                    stores.put(sd[0].trim(), sd[1].trim());
                    line = br.readLine();
                }
            }else if(path.getName().toString().trim().equals("products.txt")){
                br = new BufferedReader(new FileReader(path.toString()));
                line = br.readLine();
                while(line != null){
                    String[] prod = line.split(","); //[{PR_1} {Shoes} {Sport} {40}]
                    products.put(prod[0].trim(), prod[3].trim());
                    line = br.readLine();
                }
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //STR_1,PR_1,06:09:01,7
        String line = value.toString();
        String[] words = line.split(","); //[{STR_1} {PR_1} {06:09:01} {7}]
        String storedId = words[0];
        int productSale = Integer.parseInt(words[3].trim());

        int productPrice = Integer.parseInt(products.get(words[1]));
        int revenue = productSale * productPrice;
        String location = stores.get(storedId.toString());

        context.write(new Text(storedId+ " " + location), new IntWritable(revenue) );
    }
}

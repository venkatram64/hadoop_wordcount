package com.venkat.fd;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;


public class FraudDetectionReducer extends Reducer<Text, FraudDetectionWritable, Text, IntWritable> {

    ArrayList<String> customers = new ArrayList<>();

    @Override
    protected void reduce(Text key, Iterable<FraudDetectionWritable> values, Context context) throws IOException, InterruptedException {
        int fraudPoints = 0;
        int returnsCount = 0;
        int ordersCount = 0;

        FraudDetectionWritable data = null;
        Iterator<FraudDetectionWritable> iterator = values.iterator();

        while(iterator.hasNext()){
            ordersCount++;
            data = iterator.next();
            if(data.isReturned()){
                returnsCount++;
                try{
                    SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy");
                    Date receiveDate = sdf.parse(data.getReceiveDate());
                    Date returnedDate = sdf.parse(data.getReturnDate());

                    long diffInMillis = Math.abs(returnedDate.getTime() - receiveDate.getTime());
                    long diffDays = TimeUnit.DAYS.convert(diffInMillis,TimeUnit.MILLISECONDS);

                    //1 fraud point to a customer
                    if(diffDays > 10){
                        fraudPoints++;
                    }

                }catch (Exception e){
                    e.printStackTrace();
                }
            }
            //10 fraud points to the customer whose return rate is more than 50%
            double returnRate = (returnsCount/(ordersCount* 1.0) * 100);
            if(returnRate >= 50){
                fraudPoints += 10;
            }

            customers.add(key.toString() + "," + data.getCustomerName() +"," + fraudPoints);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Collections.sort(customers, new Comparator<String>() {
            @Override
            public int compare(String s, String t1) {
                int fp1 = Integer.parseInt(s.split(",")[2]);
                int fp2 = Integer.parseInt(t1.split(",")[2]);
                return -(fp1 - fp2);
            }
        });

        for(String f: customers){
            String[] words = f.split(",");
            //custId ,custname, fraud points
            context.write(new Text(words[0] + "," + words[1]), new IntWritable(Integer.parseInt(words[2])));
        }
    }
}

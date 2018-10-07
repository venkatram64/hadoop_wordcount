package com.venkat.ijoin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class EmpAddressReducer extends Reducer<Text, Text, Text, Text> {

    //1 [{Emp,Jack} {Address,Paris}]
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        List<String> empList = new ArrayList<>();
        List<String> addressList = new ArrayList<>();

        Iterator<Text> iterator = values.iterator();
        while(iterator.hasNext()){
            Text data = iterator.next();
            String[] newrecord = data.toString().split(",");

            if(newrecord[0].equalsIgnoreCase("Emp")){
                empList.add(newrecord[1]);
            }else if(newrecord[0].equalsIgnoreCase("Address")){
                addressList.add(newrecord[1]);
            }
        }

        if(!empList.isEmpty() && !addressList.isEmpty()){
            for(String record: empList){
                for(String address: addressList){
                    context.write(new Text(key), new Text(record +","+ address));
                }
            }
        }
    }
}

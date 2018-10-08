package com.venkat.ojoin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class EmpDeptReducer extends Reducer<Text, Text, Text, Text> {

    //20 [{Employee,1381 Jacob Admin 4560 1481} {Department,Accounts Pune}]
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        List<String> empList = new ArrayList<>();
        String department = "";

        Iterator<Text> iterator = values.iterator();
        while(iterator.hasNext()){
            Text data = iterator.next();
            String[] newrecord = data.toString().split(",");

            if(newrecord[0].equalsIgnoreCase("Employee")){
                empList.add(newrecord[1]);
            }else if(newrecord[0].equalsIgnoreCase("Department")){
                department = newrecord[1];
            }
        }
        //condition for inner join
        if(!empList.isEmpty() && !department.isEmpty()){
            for(String record: empList){
                context.write(new Text(key), new Text(record +" "+ department));

            }
        }
        //condition for left outer join
        if(!empList.isEmpty() && department.isEmpty()){
            for(String record: empList){
                context.write(new Text(key), new Text(record +" "+ "null_value null_value"));

            }
        }

        //condition for right outer join
        if(!empList.isEmpty() && department.isEmpty()){
            for(String record: empList){
                context.write(new Text(key), new Text("null_value null_value null_value null_value null_value" +" "+ department));

            }
        }
    }
}

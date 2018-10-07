package com.venkat.fd;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FraudDetectionWritable implements Writable {

    private String customerName;
    private String receiveDate;
    private boolean returned;
    private String returnDate;

    public FraudDetectionWritable(){
        set("","","no","");
    }

    public void set(String customerName, String receiveDate, String returned, String returnDate){

        this.customerName = customerName;
        this.receiveDate = receiveDate;
        if(returned.equalsIgnoreCase("yes")){
            this.returned = true;
        }else{
            this.returned = false;
        }
        this.returnDate = returnDate;
    }

    public String getCustomerName() {
        return customerName;
    }

    public String getReceiveDate() {
        return receiveDate;
    }

    public boolean isReturned() {
        return returned;
    }

    public String getReturnDate() {
        return returnDate;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        WritableUtils.writeString(dataOutput,this.customerName);
        WritableUtils.writeString(dataOutput,this.receiveDate);
        dataOutput.writeBoolean(this.returned);
        WritableUtils.writeString(dataOutput, this.returnDate);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        this.customerName = WritableUtils.readString(dataInput);
        this.receiveDate = WritableUtils.readString(dataInput);
        this.returned = dataInput.readBoolean();
        this.returnDate = WritableUtils.readString(dataInput);
    }
}

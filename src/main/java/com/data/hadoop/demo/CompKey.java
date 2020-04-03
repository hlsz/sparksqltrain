package com.data.hadoop.demo;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompKey implements WritableComparable<CompKey> {

    private String year;
    private int temp;

    @Override
    public int compareTo(CompKey o) {
        if(this.getYear().equals(o.getYear())){
            return o.getTemp() - this.getTemp();
        }
        return this.getYear().compareTo(o.getYear());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(year);
        out.writeInt(temp);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        year = in.readUTF();
        temp = in.readInt();
    }

    @Override
    public String toString() {
        return year + "\t" + temp;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public int getTemp() {
        return temp;
    }

    public void setTemp(int temp) {
        this.temp = temp;
    }
}

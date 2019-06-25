package com.hxr.hadoop.mr.tq;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TQ implements WritableComparable<TQ> {

    private int year;
    private int month;
    private int day;
    private int wd;

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }

    public int getWd() {
        return wd;
    }

    public void setWd(int wd) {
        this.wd = wd;
    }

    //serialize
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(year);
        dataOutput.writeInt(month);
        dataOutput.writeInt(day);
        dataOutput.writeInt(wd);
    }

    //deserialize
    public void readFields(DataInput dataInput) throws IOException {
        this.year = dataInput.readInt();
        this.month = dataInput.readInt();
        this.day = dataInput.readInt();
        this.wd = dataInput.readInt();
    }

    public int compareTo(TQ o) {

        //date early to late order
        int c1 = Integer.compare(this.year,this.getYear());
        if(c1 == 0){
            int c2 = Integer.compare(this.month,this.getMonth());
            if (c2 == 0){
                return Integer.compare(this.day,this.getDay());
            }
            return c2;
        }
        return c1;
    }
}

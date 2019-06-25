package com.hxr.hadoop.mr.fof;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

public class FMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    Text mKey = new Text();
    IntWritable mVal = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //tom hello hadoop cat

        String[] strs = StringUtils.split(value.toString(), ' ');

        for (int i = 1; i <strs.length ; i++) {
            mKey.set(getFof(strs[0],strs[i]));
            //0-> direct relationship, 1->indirect relationship
            mVal.set(0);
            context.write(mKey,mVal);
            for (int j = i+1; j <strs.length ; j++) {
                mKey.set(getFof(strs[i],strs[j]));
                mVal.set(1);
                context.write(mKey,mVal);
            }
        }

    }


    public static String getFof(String s1, String s2){
        if(s1.compareTo(s2)<0){
            return s1+":"+s2;
        }
        return s2+":"+s1;
    }
}

package com.hxr.hadoop.mr.tq;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TReducer extends Reducer<TQ, IntWritable, Text, IntWritable> {

    Text rkey = new Text();
    IntWritable rval = new IntWritable();

    @Override
    protected void reduce(TQ key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        //  same key in one group
        //  1970 01 01 88
        //  1970 01 11 88
        //  1970 01 11 78
        //  1970 01 21 68
        //  1970 01 01 58

        int flag = 0;
        int day = 0;
        for (IntWritable v : values) {
            if (day == 0){
                rkey.set(key.getYear() + "-" + key.getMonth()
                        + "-" + key.getDay() + ":" + key.getWd());
                //  1970-01-01:88
                rval.set(key.getWd());
                flag++;
                day = key.getDay();
                context.write(rkey,rval);
            }
            if(flag!=0 && day!=key.getDay()){
                rkey.set(key.getYear() + "-" + key.getMonth()
                        + "-" + key.getDay() + ":" + key.getWd());
                //  1970-01-01:88
                rval.set(key.getWd());
                context.write(rkey,rval);
                break;
            }
        }


    }
}

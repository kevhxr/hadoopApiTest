package com.hxr.hadoop.mr.tfidf;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.io.Text;
import java.io.IOException;

public class FirstReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int sum = 0;
        for (IntWritable i:values) {
            sum = sum +i.get();
            
        }
        if(key.equals(new Text("count"))){
            System.out.println(key.toString()+"_________"+sum);
        }
        context.write(key,new IntWritable(sum));
    }
}

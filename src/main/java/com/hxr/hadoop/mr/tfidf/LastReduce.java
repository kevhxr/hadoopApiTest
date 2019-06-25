package com.hxr.hadoop.mr.tfidf;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class LastReduce extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        StringBuffer sb = new StringBuffer();
        for (Text i : values) {
            sb.append(i.toString() + "\t");

        }
        context.write(key, new Text(sb.toString()));
    }
}


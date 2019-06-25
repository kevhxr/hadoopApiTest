package com.hxr.hadoop.mr.tfidf;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.HashPartitioner;

public class FirstPartition extends HashPartitioner<Text, IntWritable> {

    @Override
    public int getPartition(Text key, IntWritable value, int numReduceTasks) {
        if (key.equals(new Text("count"))) {
            return 3;
        } else {
            return super.getPartition(key, value, numReduceTasks - 1);
        }
    }
}

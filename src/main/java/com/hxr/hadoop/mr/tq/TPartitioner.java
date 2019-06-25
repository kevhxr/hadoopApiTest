package com.hxr.hadoop.mr.tq;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class TPartitioner extends Partitioner<TQ, IntWritable> {

    //partition based on key
    @Override
    public int getPartition(TQ key, IntWritable value, int numPartitions) {


        return key.hashCode() % numPartitions;
    }
}

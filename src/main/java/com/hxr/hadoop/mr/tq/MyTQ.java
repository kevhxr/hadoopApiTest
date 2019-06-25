package com.hxr.hadoop.mr.tq;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;

public class MyTQ {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration(true);
        Job job = Job.getInstance();
        job.setJarByClass(MyTQ.class);

        //conf
        //MAP START
        job.setMapperClass(TMapper.class);
        job.setMapOutputKeyClass(TQ.class);
        job.setOutputValueClass(IntWritable.class);

        //KVP
        job.setPartitionerClass(TPartitioner.class);

        job.setSortComparatorClass(TSortComparator.class);
/*
        job.setCombinerClass(TCombiner.class);*/
        //MAP END

        //REDUCE START
        job.setGroupingComparatorClass(TGroupingComparator.class);
        job.setReducerClass(TReducer.class);
        //REDUCE END

        Path input = new Path("/data/tq/input");
        FileInputFormat.addInputPath(job,input );

        Path output = new Path("/data/tq/output");
        if(output.getFileSystem(configuration).exists(output)){
            output.getFileSystem(configuration).delete(output,true);
        }
        FileOutputFormat.setOutputPath(job,output);

        job.setNumReduceTasks(2);

        job.waitForCompletion(true);
    }
}

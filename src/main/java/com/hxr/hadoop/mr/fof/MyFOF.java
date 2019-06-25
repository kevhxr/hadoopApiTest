package com.hxr.hadoop.mr.fof;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

public class MyFOF {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration(true);
        Job job = Job.getInstance(conf);

        job.setJarByClass(MyFOF.class);

        //conf

        //map
        job.setMapperClass(FMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //reduce
        job.setReducerClass(FReducer.class);

        Path input = new Path("/data/fof/input");
        FileInputFormat.addInputPath(job, input);
        Path output = new Path("/data/fof/output");
        if(output.getFileSystem(conf).exists(output)){
            output.getFileSystem(conf).delete(output);
        }

        FileOutputFormat.setOutputPath(job, output);

        job.waitForCompletion(true);
    }
}

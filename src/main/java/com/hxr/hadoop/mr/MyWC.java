package com.hxr.hadoop.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MyWC {

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration(true);
        Job job = Job.getInstance(config);

        job.setJarByClass(MyWC.class);

        // Specify various job-specific parameters
        job.setJobName("ooxx");

/*        job.setInputPath(new Path("in"));
        job.setOutputPath(new Path("out"));*/

        Path path = new Path("/user/root/test.txt");
        //can have multiple FileInputFormat
        FileInputFormat.addInputPath(job, path);

        //can only have one output
        Path output = new Path("/data/wc/output");

        if (output.getFileSystem(config).exists(output)){
            output.getFileSystem(config).delete(output,true);
        }

        FileOutputFormat.setOutputPath(job, output);

        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(MyReducer.class);

        // Submit the job, then poll for progress until the job is complete
        job.waitForCompletion(true);


    }
}

package com.hxr.hadoop.mr.itemcf;


import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * sum the matrices (after multiplication) together to get the result matrix
 */

public class Step5 {
    private final static Text K = new Text();
    private final static Text V = new Text();

    public static boolean run(Configuration config, Map<String, String> paths) {
        try {
            FileSystem fs = FileSystem.get(config);
            Job job = Job.getInstance(config);
            job.setJobName("step5");
            job.setJarByClass(StartRun.class);
            job.setMapperClass(Step5.Step5_Mapper.class);
            job.setReducerClass(Step5.Step5_Reducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(paths.get("Step5Input")));
            Path outPath = new Path(paths.get("Step5Output"));
            if (fs.exists(outPath)) {
                fs.delete(outPath, true);
            }
            FileOutputFormat.setOutputPath(job, outPath);
            boolean f = job.waitForCompletion(true);
            return f;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    static class Step5_Mapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = Pattern.compile("[\t],]").split(value.toString());
            Text k = new Text(tokens[0]);
            Text v = new Text(tokens[1] + "," + tokens[2]);
            context.write(k, v);
        }
    }

    static class Step5_Reducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Double> map = new HashedMap();
            //u3 > reduce
            //i101, 11.0
            //i101, 12.0
            //i101, 8.0
            //i102, 12.0
            //i102, 22.0
            for (Text line : values) {//i101,11.0
                String[] tokens = line.toString().split(",");
                String itemId = tokens[0];
                Double score = Double.parseDouble(tokens[1]);
                if(map.containsKey(itemId)){
                    map.put(itemId,map.get((itemId)+score));
                }else{
                    map.put(itemId,score);
                }
            }

            Iterator<String> iter = map.keySet().iterator();
            while (iter.hasNext()){
                String itemID = iter.next();
                double score = map.get(itemID);
                Text v = new Text(itemID+","+score);
                context.write(key,v);
            }
        }
    }
}

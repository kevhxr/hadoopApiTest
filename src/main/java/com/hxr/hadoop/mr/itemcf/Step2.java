package com.hxr.hadoop.mr.itemcf;

import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;

public class Step2 {

    public static boolean run(Configuration config, Map<String, String> paths) {
        try {
            FileSystem fs = FileSystem.get(config);
            Job job = Job.getInstance(config);
            job.setJobName("step2");
            job.setJarByClass(StartRun.class);
            job.setMapperClass(Step2.Step2_Mapper.class);
            job.setReducerClass(Step2.Step2_Reducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            FileInputFormat.addInputPath(job, new Path(paths.get("Step2Input")));
            Path outPath = new Path(paths.get("Step2Output"));
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

    private static class Step2_Mapper extends Mapper<LongWritable,Text,Text,Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //better to set key as item+user
            //i101,u1,click,2019/05/12 15:01
            String[]tokens= value.toString().split(",");
            String item = tokens[0];
            String user = tokens[1];
            String action = tokens[2];
            Text k = new Text(user);
            Integer actionValue =StartRun.R.get(action);
            Text v = new Text(item+":"+actionValue.intValue());
            //u1    i101:1
            context.write(k,v);
        }
    }

    private static class Step2_Reducer extends Reducer<Text,Text,Text,Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String,Integer>r = new HashedMap();
            //u1
            //i101:1
            //i102:1
            for (Text value:values) {
                String[] vs = value.toString().split(":");
                String item=vs[0];
                Integer action=Integer.parseInt(vs[1]);
                action=((Integer)(r.get(item)==null?0:r.get(item))).intValue()+action;
                r.put(item,action);
            }
            StringBuffer sb = new StringBuffer();
            for(Map.Entry<String, Integer> entry :r.entrySet()){
                sb.append(entry.getKey()+":"+entry.getValue().intValue()+",");
            }
            context.write(key,new Text(sb.toString()));
        }
    }
}

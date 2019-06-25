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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

public class Step4 {

    public static boolean run(Configuration config, Map<String, String> paths) {
        try {
            FileSystem fs = FileSystem.get(config);
            Job job = Job.getInstance(config);
            job.setJobName("step4");
            job.setJarByClass(StartRun.class);
            job.setMapperClass(Step4.Step4_Mapper.class);
            job.setReducerClass(Step4.Step4_Reducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            //important "setInputPaths"
            FileInputFormat.setInputPaths(job,
                    new Path[]{
                            new Path(paths.get("Step4Input1")),
                            new Path(paths.get("Step4Input2"))
                    }
            );
            Path outPath = new Path(paths.get("Step4Output"));
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

    static class Step4_Mapper extends Mapper<LongWritable, Text, Text, Text> {

        private String flag; //A parallel matrix or B score matrix

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            FileSplit split = (FileSplit) context.getInputSplit();
            flag = split.getPath().getParent().getName(); //decide which data set to read
            System.out.println(flag + "********************");
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            super.map(key, value, context);
            //can split based on \t or ,
            String[] tokens = Pattern.compile("[\t,]").split(value.toString());
            if (flag.equals("step3")) {  //parallel matrix
                //i100:i125 1
                String[] v1 = tokens[0].split(":");
                String itemID1 = v1[0];
                String itemID2 = v1[1];
                String num = tokens[1];
                //A:B 3
                //B:A 3
                Text k = new Text(itemID1);//use the first item as key, such as i100
                Text v = new Text("A:" + itemID2 + "," + num); //A:i125,1
                // Here why only write A as key but not B?
                // because if you have A:B then you must have B:A key as well in some other map
                context.write(k, v);
            } else if (flag.equals("step2")) {
                //u26   i276:1,i201:1,i348:1,i321:1,i361:1,
                String userId = tokens[0];
                //pay attention here i starts by 1, because 0 is user
                for (int i = 1; i < tokens.length; i++) {
                    String[] vector = tokens[i].split(":");
                    String itemID = vector[0]; //item ID
                    String pref = vector[i]; //score

                    Text k = new Text(itemID);//use the item as key, such as i276
                    Text v = new Text("B:" + userId + "," + pref); //B:u26,1
                    //i100  B:u26,1
                    context.write(k, v);
                }

            }
        }
    }


    static class Step4_Reducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Integer> mapA = new HashedMap();//parallel matrix
            Map<String, Integer> mapB = new HashedMap();//score matrix

            //A > reduce same key as a group
            //value :
            //item parallel A:b:2 c:4 d:8
            //score B:u1:18 u2:33 u3:22
            for (Text line : values) {
                String val = line.toString();
                if (val.startsWith("A:")) {
                    //A:i109,1
                    String[] kv = Pattern.compile("[\t,]").split(val.substring(2));
                    try {
                        mapA.put(kv[0], Integer.parseInt(kv[1]));
                        //A:b:2 c:4 d:8
                        //based on A, the number of appearance of item
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } else if (val.startsWith("B:")) {
                    //B:u26,1
                    String[] kv = Pattern.compile("[\t,]").split(val.substring(2));
                    try {
                        mapB.put(kv[0], Integer.parseInt(kv[1]));
                        //A:b:2 c:4 d:8
                        //based on A, the number of appearance of item
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            //i100
            //i101  3
            //i102  4
            double result = 0;
            Iterator<String> iter = mapA.keySet().iterator();
            while (iter.hasNext()) {
                String mapk = iter.next();
                int num = mapA.get(mapk).intValue();
                //i100
                //u3    3
                //u4    5
                Iterator<String> iterb = mapB.keySet().iterator();
                while (iterb.hasNext()) {
                    String mapkb = iterb.next();
                    int pref = mapB.get(mapkb).intValue();
                    result = num * pref;


                    Text k = new Text(mapkb);//use the user ID as key
                    //based on item A, other item's existence and rate score(all user of item A)
                    Text v = new Text(mapk + "," + result);
                    context.write(k,v);
                }

            }
        }
    }
}

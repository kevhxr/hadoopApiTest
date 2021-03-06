package com.hxr.hadoop.mr.tfidf;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TwoMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        FileSplit fs = (FileSplit) context.getInputSplit();

        //豆浆_28828291132    3
        if(!fs.getPath().getName().contains("part-r-0003")){
            String[] v = value.toString().trim().split("\t");
            if(v.length>=2){
                String[] ss = v[0].split("_");
                if(ss.length>=2){
                    String w = ss[0];
                    context.write(new Text(w),new IntWritable(1));
                }
            }else{
                System.out.println(value.toString()+"--------");
            }
        }
    }
}

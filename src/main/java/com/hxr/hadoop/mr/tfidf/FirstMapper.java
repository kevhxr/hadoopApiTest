package com.hxr.hadoop.mr.tfidf;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;

/**
 * count TF(the time of appearance of a word in a paragraph) and N(total number of weibo)
 */
public class FirstMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        //28828291132   今天我的豆浆，油条
        String[] v = value.toString().trim().split("\t");
        if (v.length >= 2) {
            String id = v[0].trim();
            String content = v[1].trim();

            StringReader sr = new StringReader(content);
            IKSegmenter ikSegmenter = new IKSegmenter(sr, true);
            Lexeme word = null;
            while ((word = ikSegmenter.next()) != null) {
                String w = word.getLexemeText();

                context.write(new Text(w + "_" + id), new IntWritable(1));
                //今天_28828291132    1
            }

            context.write(new Text("count"), new IntWritable(1));
            //count 1
        } else {
            System.out.println(value.toString() + "-----------");
        }
    }
}

package com.hxr.hadoop.mr.tq;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class TMapper extends Mapper<LongWritable, Text, TQ, IntWritable> {

    TQ mkey = new TQ();
    IntWritable mval = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //  1949-10-01 14:21:02 34c
        try {
            String[] strs = StringUtils.split(value.toString(), '\t');
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            Date date = sdf.parse(strs[0]);
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);

            mkey.setYear(cal.get(Calendar.YEAR));
            mkey.setMonth(cal.get(Calendar.MONTH) + 1);
            mkey.setDay(cal.get(Calendar.DAY_OF_MONTH));
            int wd = Integer.parseInt(strs[1].substring(0, strs[1].length() - 1));
            mkey.setWd(wd);

            mval.set(wd);
            context.write(mkey, mval);
        } catch (ParseException e) {
            e.printStackTrace();
        }

    }
}

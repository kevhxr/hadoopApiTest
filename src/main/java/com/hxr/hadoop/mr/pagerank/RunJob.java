package com.hxr.hadoop.mr.pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class RunJob {

    public static enum MyCounter {
        my
    }

    public static void main(String[] args) {
        Configuration conf = new Configuration(true);
        conf.set("mapreduce.app-submission.cross-platform", "true");
        conf.set("mapreduce.framework.name", "local");

        double d = 0.001;
        int i = 0;

        while (true) {
            i++;
            try {
                conf.setInt("runCount", i);
                FileSystem fs = FileSystem.get(conf);
                Job job = Job.getInstance(conf);

                job.setJarByClass(RunJob.class);
                job.setJobName("pr" + i);
                job.setMapperClass(PageRankMapper.class);
                job.setReducerClass(PageRankRecuder.class);
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);
                job.setInputFormatClass(KeyValueTextInputFormat.class);

                Path inputPath = new Path("/data/pagerank/input/");
                if (i > 1) {
                    inputPath = new Path("/data/pagerank/output/pr" + (i - 1));
                }
                FileInputFormat.addInputPath(job, inputPath);

                Path outputPath = new Path("/data/pagerank/output/pr" + i);
                if (fs.exists(outputPath)) {
                    fs.delete(outputPath, true);
                }
                FileOutputFormat.setOutputPath(job, outputPath);
                boolean f = job.waitForCompletion(true);
                if (f) {
                    System.out.println("success.");
                    long sum = job.getCounters().findCounter(MyCounter.my).getValue();
                    System.out.println(sum);
                    double avgd = sum / 4000.0;
                    if (avgd < d) {
                        break;
                    }
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    private static class PageRankMapper extends Mapper<Text, Text, Text, Text> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

            int runCount = context.getConfiguration().getInt("runCount", 1);
            //  A B D
            //  A B D 0.3

            //  K:A
            //  V:B D
            //  K:A
            //  V:0.3 B D
            String page = key.toString();
            Node node = null;
            if (runCount == 1) {
                node = Node.fromMR("1.0", value.toString());
            } else {
                node = Node.fromMR(value.toString());
            }

            //  A 1.0 B D
            // transfer old pr value and page relationship
            context.write(new Text(page), new Text(node.toString()));
            if (node.containAdjacentNodes()) {
                //count the par value
                double outValue = node.getPageRank() / node.getAdjacentNodeNames().length;
                for (int i = 0; i < node.getAdjacentNodeNames().length; i++) {
                    String outpage = node.getAdjacentNodeNames()[i];
                    //  B:0.5
                    //  D:0.5
                    //  which page A vote for, that page will be the key,
                    //  val will be the par value
                    //  par value equals to A's pr value/amount of hyperlink
                    context.write(new Text(outpage), new Text(outValue + ""));
                }
            }
        }
    }

    private static class PageRankRecuder extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            //  same key share same group
            //  key as the page name like 'B'
            //  contain two type of data
            //  B:1.0 C //    page relationship and old pr value
            //  B:0.5   // value of vote

            double sum = 0.0;

            //  need save old pr value
            // since we need to use new pr value to reduce old pr value
            Node sourceNode = null;
            for (Text i : values) {
                Node node = Node.fromMR(i.toString());
                if (node.containAdjacentNodes()) {
                    sourceNode = node;
                } else {
                    sum = sum + node.getPageRank();
                }
            }
            //4.0 here is the total number of pages
            double newPR = (0.15 / 4.0) + (0.85 * sum);
            System.out.println("*********** new pageRank value is " + newPR);

            //compare old and new PR value
            double d = newPR - sourceNode.getPageRank();

            int j = (int) (d * 1000.0);
            j = Math.abs(j);
            System.out.println(j + "_____________");
            context.getCounter(MyCounter.my).increment(j);

            sourceNode.setPageRank(newPR);
            context.write(key,new Text(sourceNode.toString()));
            //  A B D 0.8

        }
    }
}

package com.lxy.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class phoneMain {
    public static void main(String[] args) throws InterruptedException, IOException,
            ClassNotFoundException {
        // if(args==null || args.length !=1) {
        // throw new RuntimeException("argument is not right!");
        // }
        String input = "D:\\pos.txt";
        Configuration configuration = new Configuration();
        configuration.set("mapred.textoutputformat.ignoreseparator", "true");
        configuration.set("mapred.textoutputformat.separator", ",");            // 设置 MapReduce 的输出的分隔符为逗号
        configuration.set("timeRange", "09-18-24");
        Job job = Job.getInstance(configuration);
        job.setJarByClass(phoneMain.class);
        job.setMapperClass(phoneMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(phoneReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path("D:\\output"));
        job.waitForCompletion(true);
    }
}
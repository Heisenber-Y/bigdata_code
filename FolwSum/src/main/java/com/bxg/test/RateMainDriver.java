package com.bxg.test;

import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeSet;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class RateMainDriver extends Configured implements Tool{

    public int run(String[] args) throws Exception {
        Job job = new Job();
        job.setNumReduceTasks(1);
        job.setJarByClass(com.bxg.test.RateMainDriver.class);

        job.setMapperClass(com.bxg.test.RateDriver.RateMapper.class);
        job.setReducerClass(com.bxg.test.RateDriver.RateReducer.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(RateBean.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        RateMainDriver driver = new RateMainDriver();
        int exitCode = ToolRunner.run(driver, args);
        System.exit(exitCode);
    }
}

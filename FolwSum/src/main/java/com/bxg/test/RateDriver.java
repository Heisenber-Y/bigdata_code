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

public class RateDriver {



    public static class RateMapper extends Mapper<LongWritable, Text, Text, RateBean> {

            Gson gson =new Gson();

            @Override
            protected void map(LongWritable key, Text value,Context context)
                    throws IOException, InterruptedException {

                String line = value.toString();


                RateBean rateBean = gson.fromJson(line,RateBean.class);
                Text k= new Text();
                k.set(rateBean.getUid());
                context.write(k,rateBean);

            }
        }


        public static class RateReducer extends Reducer<Text, RateBean, Text, Text> {
            private static Logger logger= LoggerFactory.getLogger(RateReducer.class);
            @Override
            protected void reduce(Text key, Iterable<RateBean> values, Context context)
                    throws IOException, InterruptedException {
                StringBuffer sb =new StringBuffer("电影的id: ");
                TreeSet<RateBean> ts =new TreeSet<RateBean> ();
               java.util.List<RateBean> list = new   java.util.ArrayList<RateBean>();

                for(RateBean t: values){
                    list.add(new RateBean(t.getMovie(),t.getRate(),t.getTimeStamp(),t.getUid()));
                }

                java.util.Collections.sort(list);
                int count = 0;
                for (RateBean r : list) {
                    if(count<3){
                        sb.append(r.getMovie()+"||");
                    }
                    count++;
                }

//
//                for(RateBean t: values){
//                    ts.add(new RateBean(t.getMovie(),t.getRate(),t.getTimeStamp(),t.getUid()));
//                    if(ts.size() > 3){
//                        ts.remove(ts.first());
//                    }
//                }
//
//                for (RateBean t : ts ) {
//
//                    sb.append(t.getMovie()+"||");
//                }
                context.write(new Text("uid:"+key), new Text(sb.toString()));
            }
        }

//
//        public static void main(String[] args) throws Exception {
//            System.setProperty("HADOOP_USER_NAME", "root");
//            Configuration conf = new Configuration();
//            conf.set("fs.defaultFS", "hdfs://192.168.184.128:9000");
//            conf.set("mapreduce.framework.name", "yarn");
//            conf.set("yarn.resourcemanager.hostname", "mini1");
//            conf.set("mapreduce.app-submission.cross-platform", "true");
//            conf.set("mapred.jar", "E:\\FolwSum-1.0.jar");
//
//            Job job = Job.getInstance(conf);
//
//            job.setJarByClass(cn.itcast.mapreduce.FlowSum.class);
//
//            job.setMapperClass(com.bxg.test.RateDriver.RateMapper.class);
//            job.setReducerClass(com.bxg.test.RateDriver.RateReducer.class);
//
//
//            job.setOutputKeyClass(Text.class);
//            job.setOutputValueClass(RateBean.class);
//
//            job.setInputFormatClass(TextInputFormat.class);
//            job.setOutputFormatClass(TextOutputFormat.class);
//            job.setNumReduceTasks(1);
//            //告诉框架，我们要处理的数据文件在那个路劲下
//            FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.184.128:9000/rate/input"));
//
//            //告诉框架，我们的处理结果要输出到什么地方
//            FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.184.128:9000/rate/output"));
//
//            boolean res = job.waitForCompletion(true);
//
//            System.exit(res?0:1);
//        }




}

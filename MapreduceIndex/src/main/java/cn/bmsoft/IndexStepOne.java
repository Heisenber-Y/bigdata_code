package cn.bmsoft;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class IndexStepOne {


    public  static  class IndexStepOneMapper extends Mapper{


        Text k=new Text();
        LongWritable v= new LongWritable();
        @Override
        protected void map(Object key, Object value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String[] words = line.split(" ");
            FileSplit is = (FileSplit) context.getInputSplit();
            String name = is.getPath().getName();
            //输出key     单词+文件命  value 1
            for (String word : words) {
                    k.set(word+"-"+name);
                context.write(k,v);

            }
        }
    }

    public  static class IndexStepOneReduce extends Reducer<Text,IntWritable,Text, IntWritable>{


        @Override
        protected void reduce(Text key, Iterable <IntWritable>values, Context context) throws IOException, InterruptedException {

           int count=0;
            for (IntWritable value : values) {
                count += value.get();

            context.write(key,new IntWritable(count));

            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {


      Configuration conf=new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.247.128:9000");

        Job job = Job.getInstance(conf);

        job.setJarByClass(IndexStepOne.class);
        //job.setMapOutputValueClass();
        //job.setMapOutputKeyClass();

        FileInputFormat.setInputPaths(job,new Path("/index/input"));
        FileOutputFormat.setOutputPath(job,new Path("index/output"));
        boolean res = job.waitForCompletion(true);
        System.exit(res?1:0);


    }





}

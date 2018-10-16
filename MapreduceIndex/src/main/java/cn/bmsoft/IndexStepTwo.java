package cn.bmsoft;

import org.apache.commons.configuration.ConfigurationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class IndexStepTwo {

    public  static  class  IndexStepTwoMapper extends Mapper{
        @Override
        protected void map(Object key, Object value, Context context) throws IOException, InterruptedException {


           Text k= new Text();
           Text v=new Text();
            String line = value.toString();
            String[] splits = line.split("\t");

            String word_file = splits[0];
            String count = splits[1];
            String[] split = word_file.split("-");
            String word = split[0];
            String file = split[1];
            k.set(word);
            v.set(file+"--"+count);
            context.write(k,v);




        }
    }

    public  static  class  IndexStepTwoReduce extends Reducer{

        Text v=new Text();
        @Override
        protected void reduce(Object key, Iterable values, Context context) throws IOException, InterruptedException {

            StringBuffer sb=new StringBuffer();
            for (Object value : values) {
            sb.append(values.toString()).append(" ");
            }
            v.set(sb.toString());
            context.write(key,v);



        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf= new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.247.128:9000");
        Job job = Job.getInstance(conf);

        FileInputFormat.setInputPaths(job,new Path("/index/output/"));
        FileOutputFormat.setOutputPath(job,new Path("/index/output_1"));
        boolean res = job.waitForCompletion(true);
        System.exit(res?1:0);


    }


}

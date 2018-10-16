package cn.itcast.mapreduce.commonfriends;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CommonFriendsStepOne {

    public  static  class CommonFriendsStepOneMapper extends Mapper{

        @Override
        protected void map(Object key, Object value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] splits = line.split(":");
            String person = splits[0];
            String[] friends = splits[1].split(",");
            for (String friend : friends) {
                context.write(new Text(friend),new Text(person));

            }
            }

        }

    public  static  class CommonFriendsStepOneReducer extends Reducer{
        @Override
        protected void reduce(Object friend, Iterable  persons, Context context) throws IOException, InterruptedException {

            StringBuffer sb = new StringBuffer();

            for (Object person : persons) {
                sb.append(person).append("-");
            }
        context.write(friend,new Text(sb.toString()));


        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.247.128:9000");

        Job job = Job.getInstance(conf);


        job.setJarByClass(CommonFriendsStepOne.class);

        //告诉程序，程序使用的mapper类和reducer类是什么
    job.setMapperClass(CommonFriendsStepOneMapper.class);
    job.setReducerClass(CommonFriendsStepOneReducer.class);

    //告诉框架 程序的输出类型
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(Text.class);

      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);

      //告诉框架 处理的数据文件存放路径

        FileInputFormat.setInputPaths(job,new Path("/Users/yml/文件"));

        //告诉框架 处理数据的结果存放路径
        FileOutputFormat.setOutputPath(job,new Path("/Users/yml/文件/text"));

        boolean b = job.waitForCompletion(true);

        System.exit(b?0:1);


    }


}

package cn.bmsoft;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;
import java.util.Arrays;

public class CommonFriendsStepTwo {

    public  static  class CommonFriendsStepTwoMapper extends Mapper{
        @Override
        protected void map(Object key, Object value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String[] splits= line.split("\t");
            String friend = splits[0];
            String[] persons = splits[1].split("-");
            Arrays.sort(persons);

            for (int i = 0; i < persons.length-1; i++) {
                for (int j = i+1; j < persons.length; j++) {

                    context.write(new Text(persons[i]+"-"+persons[j]),new Text(friend));
                }
            }

        }
    }

    public  static class  CommonFriendsStepTwoReduce extends Reducer{
        @Override
        protected void reduce(Object key, Iterable values, Context context) throws IOException, InterruptedException {
            StringBuffer sb = new StringBuffer();
            for (Object value : values) {
                sb.append(value).append(" ");

            }
        context.write(key,new Text(sb.toString()));
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        conf.set("fs.defaultFS", "hdfs://192.168.247.128:9000");
        job.setJarByClass(CommonFriendsStepTwo.class);

        //job.setMapOutputValueClass(Text.class);
        //job.setMapOutputValueClass(Text.class);

        //job.setOutputValueClass(Text.class);
        //job.setOutputKeyClass(Text.class);

        FileInputFormat.setInputPaths(job,new Path("/output"));
        FileOutputFormat.setOutputPath(job,new Path("/ouput_1"));


        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);


    }



}

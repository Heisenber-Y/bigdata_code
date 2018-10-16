package cn.bmsoft;

import org.apache.commons.lang.StringUtils;
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


import java.io.IOException;
import java.util.TreeMap;

public class FlowSum {






public static class FlowSumMapper extends Mapper<LongWritable, Text, Text, FlowBean>{

        Text k =new Text();

      FlowBean v=new FlowBean();



   // TreeMap tre= new TreeMap<FlowBean,Text>();



    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] files = StringUtils.split(line,"\t");
        //抽取我们需要的字段
        //获取手机号
        String num = files[1];
        //获取上行流量
        long  upFlow = Long.parseLong(files[files.length - 3]);
        //获取下行流量
        long downFlow = Long.parseLong(files[files.length - 2]);


        // cn.bmsoft.FlowBean flowBean = new cn.bmsoft.FlowBean(upFlow, downFlow);

        // context.write(new Text(num),new cn.bmsoft.FlowBean(upFlow,downFlow));

        k.set(num);
        v.set(upFlow,downFlow);
        context.write(k,v);
    }

}


public  static class FlowSumReducer extends Reducer<Text,FlowBean,Text,FlowBean>{


   FlowBean v= new FlowBean();

    @Override
    protected void reduce(Text key, Iterable <FlowBean>values, Context context) throws IOException, InterruptedException {
        //这里reduce接受到的key就是某一组 手机号 还有对象bean（上传，下载，总流量）中的第一个
        //value就是这一组kv只能够所有bean的一个迭代器

        long upFlowCount=0;
        long downFlowCount=0;

        for(FlowBean bean:values){
             upFlowCount +=bean.getUpFlow();
            downFlowCount += bean.getDownFlow();


        }

context.write(key,v);

    }
}


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();


        conf.set("fs.defaultFS","hdfs://192.168.247.127:9000");

        Job job = Job.getInstance(conf);



        job.setJarByClass(FlowSum.class);

        //告诉程序，我们的程序所用的mapper类和reducer类是什么
        job.setMapperClass(FlowSumMapper.class);
        job.setReducerClass(FlowSumReducer.class);

        //告诉框架，我们程序输出的数据类型
	    job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //告诉框架，我们程序使用的数据读取组件 结果输出所用的组件是什么
        //TextInputFormat是mapreduce程序中内置的一种读取数据组件  准确的说 叫做 读取文本文件的输入组件
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //告诉框架，我们要处理的数据文件在那个路劲下
        FileInputFormat.setInputPaths(job, new Path("/flowsum/input"));

        //告诉框架，我们的处理结果要输出到什么地方
        FileOutputFormat.setOutputPath(job, new Path("/flowsum/output"));

        //boolean res = job.waitForCompletion(true);
        boolean res = job.waitForCompletion(true);



        System.exit(res?0:1);




    }






}

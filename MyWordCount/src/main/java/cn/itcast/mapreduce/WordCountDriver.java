package cn.itcast.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 
 * @author AllenWoon
 *
 * 本类是客户端用来指定wordcount job程序运行时候所需要的很多参数
 * 
 * 比如：指定哪个类作为map阶段的业务逻辑类  哪个类作为reduce阶段的业务逻辑类
 * 		指定用哪个组件作为数据的读取组件  数据结果输出组件
 * 		指定这个wordcount jar包所在的路径
 * 
 * 		....
 * 		以及其他各种所需要的参数
 */
public class WordCountDriver {
	
	public static void main(String[] args) throws Exception {
	Configuration conf=	new Configuration();
		//Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://mini1:9000");
//		conf.set("mapreduce.framework.name", "yarn");
//		conf.set("yarn.resourcemanager.hostname", "mini1");
		Job job = Job.getInstance(conf);
		
		//$JAVA_HOMEbin -cp hdfs-2.3.4.jar:mapreduce-2.0.6.4.jar;		
		//告诉框架，我们的的程序所在jar包的位置
//		job.setJar("/root/wordcount.jar");
		job.setJarByClass(WordCountDriver.class);
		
		//告诉程序，我们的程序所用的mapper类和reducer类是什么
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		
		//告诉框架，我们程序输出的数据类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//告诉框架，我们程序使用的数据读取组件 结果输出所用的组件是什么
		//TextInputFormat是mapreduce程序中内置的一种读取数据组件  准确的说 叫做 读取文本文件的输入组件
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//告诉框架，我们要处理的数据文件在那个路劲下
		FileInputFormat.setInputPaths(job, new Path("/wordcount/input"));
		
		//告诉框架，我们的处理结果要输出到什么地方
		FileOutputFormat.setOutputPath(job, new Path("/wordcount/output"));
		
		boolean res = job.waitForCompletion(true);
		
		System.exit(res?0:1);
		
		
	}

}

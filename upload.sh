#!/bin/bash

#先定义好环境变量
export JAVA_HOME=/usr/java/jdk1.8.0_151
export JRE_HOME=${JAVA_HOME}/jre
export CLASSPATH=.:${JAVA_HOME}/lib:${JRE_HOME}/lib
export PATH=${JAVA_HOME}/bin:${PATH}

#HADOOP 环境变量

export HADOOP_HOME=/home/hadoop/app/hadoop-2.7.2
export PATH=${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin:${PATH}


#日志存放目录：
log_src_dir=/home/hadoop/data/logs/log

#日志待上传目录：
log_wait_put=/home/hadoop/data/logs/wait_put_log

#日志文件上传到hdfs 路径

hdfs_dir=/data/chick/log/20180808

#打印环境变量信息

echo "envs:hadoop_home:${HADOOP_HOME}"


#读取日志文件的目录，确认日志是否需要上传
echo "日志源文件存放目录：${log_src_dir}"
ls ${log_src_dir} | while read filename
do
if [[ $filename == access.log.* ]];then
date=`date +%Y_%m_%d_%H_%M_%S`
#将文件上传到待上传目录 ，并打印相关i信息

echo "将${log_src_dir}${filename} 准备移动到 ${log_wait_put}/${filename}"
mv  ${log_src_dir}/${filename} ${log_wait_put}/${filename}${date}
echo "${log_wait_put}/${filename}${date}" >>${log_wait_put}"willDoing${date}"
fi
done


ls 	${log_wait_put} |grep -v "_COPY_" |grep -v "_DONE_" | while read line 

do
#打印星系
echo "upload is in ${line}"
mv ${log_wait_put}/${line} ${log_wait_put}/${line}"_COPY_"
#将读取文件一个个上传
cat ${log_wait_put}/${line}"_COPY_" |while read line
do
echo "putting ${line} to hdfs path ${hdfs_dir}"

hadoop fs -put ${line} ${hdfs_dir}
done
mv ${log_wait_put}/${line}"_COPY_" ${log_wait_put}/${line}"_DONE_"

done 






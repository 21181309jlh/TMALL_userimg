package com.userimg.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 支付偏好分析与统计
 * @version 1.0.0
 * @Auther jialinhan
 * @CreatTime 2021/3/17 14:23
 */
public class PaylikeDriver {
    /**
     * Map处理类
     */

    public static class PaylikeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value,
                           Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(" +");
            //判断是否存在字段
            if (fields != null && fields.length > 0) {
                String payType = fields[fields.length - 1];
                //输出数据
                context.write(new Text(payType),new IntWritable(1));
            }
        }
    }
    /**
     * Reduce处理类
     */

    public static class PaylikeReducer extends Reducer<Text, IntWritable,Text,IntWritable> {
        @Override
        protected void reduce(Text key,Iterable<IntWritable>values,
                              Context context)throws IOException,InterruptedException{
            //声明计数的变量
            int count=0;
            //遍历提取数据
            for(IntWritable i:values){
                count+=i.get();
            }
            context.write(key,new IntWritable(count));
        }
    }

    /**
     * 执行作业的接口
     */

    public void run()throws Exception{
        Configuration configuration =new Configuration();
        Job job= Job.getInstance(configuration);
        job.setJarByClass(PaylikeDriver.class);
        job.setMapperClass(PaylikeMapper.class);
        job.setReducerClass(PaylikeReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths(job,new Path(
                "hdfs://hadoop101:9000/data/userimgs/input/paylog.txt"));
        FileOutputFormat.setOutputPath(job ,new Path(
                "hdfs://hadoop101:9000/data/userimgs/output/paylike"));
        boolean result =job.waitForCompletion(true);
        System.out.println("Finish...");
    }

}

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
 * @version 1.0.0
 * @Auther jialinhan
 * @CreatTime 2021/3/17 8:31
 */
public class WordCountDriver {

    /**
     * Map处理类
     */

    public static class wordCountMapper extends Mapper<LongWritable,Text, Text,IntWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context)throws IOException,InterruptedException{
            //分割数据行
            String[] words=value.toString().split(" +");
            //循环映射数据
            for(String word:words){
                //context.write(new Text(word),new IntWritable(1));
                context.write(new Text(word),new IntWritable(1));
            }
        }
    }

    /**
     * Reduce处理类
     */

    public static class wordCountReducer extends Reducer<Text, IntWritable,Text,IntWritable>{
        @Override
        protected void reduce(Text key,Iterable<IntWritable>values,
                              Context context)throws IOException,InterruptedException{
            int count=0;
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
        job.setJarByClass(WordCountDriver.class);
        job.setMapperClass(wordCountMapper.class);
        job.setReducerClass(wordCountReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths(job,new Path("hdfs://hadoop101:9000/data/input/words.txt"));
        FileOutputFormat.setOutputPath(job ,new Path("hdfs://hadoop101:9000/data/output/wordcount/"));
        boolean result =job.waitForCompletion(true);
        System.out.println("Finish...");
    }

}

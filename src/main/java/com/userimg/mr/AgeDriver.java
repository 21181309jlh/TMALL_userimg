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
 * @CreatTime 2021/3/18 10:45
 */
public class AgeDriver {
    public static class AgeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value,
                           Context context) throws IOException, InterruptedException {
            //获取单行数据
            String line = value.toString();
            //分割字符串
            String[] fields = line.split( " +");
            if(fields != null && fields.length > 0){
                //获得用户年龄
                //以10位一个年龄段 0-10,11-20,21-30...91-100
                String temp = fields[fields.length - 6];
                int num = Integer.parseInt(temp);
                String payType = null;
                if(0 <= num && num <=10) {
                    payType = "0-10";
                }
                else if (11 <= num && num <= 20) {
                    payType = "11-20";
                }
                else if (21 <= num && num <= 30) {
                    payType = "21-30";
                }
                else if (31 <= num && num <= 40) {
                    payType = "31-40";
                }
                else if (41 <= num && num <= 50) {
                    payType = "41-50";
                }
                else if (51 <= num && num <= 60) {
                    payType = "51-60";
                }
                else if (61 <= num && num <= 70) {
                    payType = "61-70";
                }
                else if (71 <= num && num <= 80) {
                    payType = "71-80";
                }
                else if (81 <= num && num <= 90) {
                    payType = "81-90";
                }
                else {
                    payType = "91-100";
                }

                //输出数据
                context.write(new Text(payType), new IntWritable(1));


            }

        }
    }

    /**
     * Reduce处理类
     * 数据汇总
     */
    public static class AgeReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,
                              Context context) throws IOException, InterruptedException {
            //声明计数变量
            int count = 0;
            //遍历提取数据
            for(IntWritable i:values) {
                count ++;
            }
            context.write(key, new IntWritable(count));
        }
    }

    /**
     *执行作业的接口
     */
    public void run() throws Exception {
        //创建配置信息对象
        Configuration configuration = new Configuration();
        //创建作业对象
        Job job = Job.getInstance(configuration);
        //指定本程序的jar包所在的本地路径
        job.setJarByClass(AgeDriver.class);

        //指定Mapper与Reducer业务处理类
        job.setMapperClass(AgeDriver.AgeMapper.class);
        job.setReducerClass(AgeDriver.AgeReducer.class);

        //指定Mapper输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //指定Reducer输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //指定输入输出文件的路径或目录
        FileInputFormat.setInputPaths(job, new Path("hdfs://hadoop101:9000/data/userimgs/input/paylog.txt"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop101:9000/data/userimgs/output/age"));

        //等待作业执行完毕
        boolean result = job.waitForCompletion(true);
        System.out.println("Finish...");

    }
}

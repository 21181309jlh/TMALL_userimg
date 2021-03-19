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

public class ShopabilityDriver {

    public static class ShopabilityMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value,
                           Context context) throws IOException, InterruptedException {
            //获取单行数据
            String line = value.toString();
            //分割字符串
            String[] fields = line.split( " +");
            if(fields != null && fields.length > 0){
                //获得支付金额
                String payType = null;

                //以10位一个年龄段 0-1000,1001-2000...9001-10000
                String temp = fields[fields.length - 2];
                int num = Integer.parseInt(temp);

                if(0 <= num && num <=100) {
                    payType = "0-100";
                }
                else if (101 <= num && num <= 200) {
                    payType = "101-200";
                }
                else if (201 <= num && num <= 300) {
                    payType = "201-300";
                }
                else if (301 <= num && num <= 400) {
                    payType = "301-400";
                }
                else if (401 <= num && num <= 500) {
                    payType = "401-500";
                }
                else if (501 <= num && num <= 600) {
                    payType = "501-600";
                }
                else if (601 <= num && num <= 700) {
                    payType = "601-700";
                }
                else if (701 <= num && num <= 800) {
                    payType = "701-800";
                }
                else if (801 <= num && num <= 900) {
                    payType = "801-900";
                }
                else if (901 <= num && num <=1000){
                    payType = "901-1000";
                }
                else if (1001 <= num && num <= 3000){
                    payType = "1001-3000";
                }
                else if (3001 <= num && num <= 5000){
                    payType = "3001-5000";
                }
                else{
                    payType = ">5000";
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
    public static class ShopabilityReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

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
        job.setJarByClass(ShopabilityDriver.class);

        //指定Mapper与Reducer业务处理类
        job.setMapperClass(ShopabilityMapper.class);
        job.setReducerClass(ShopabilityReducer.class);

        //指定Mapper输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //指定Reducer输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //指定输入输出文件的路径或目录
        FileInputFormat.setInputPaths(job, new Path("hdfs://hadoop101:9000/data/userimgs/input/paylog.txt"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop101:9000/data/userimgs/output/shopability"));

        //等待作业执行完毕
        boolean result = job.waitForCompletion(true);
        System.out.println("Finish...");

    }


}

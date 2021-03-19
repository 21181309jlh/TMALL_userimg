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
 * @CreatTime 2021/3/18 10:46
 */
public class ProvinceDriver {
    public static class ProvinceMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value,
                           Context context) throws IOException, InterruptedException {
            //获取单行数据
            String line = value.toString();
            //分割字符串
            String[] fields = line.split( " +");
            if(fields != null && fields.length > 0){
                //获得省份
                String payType = fields[fields.length - 5];
                //输出数据
                context.write(new Text(payType), new IntWritable(1));


            }

        }
    }

    /**
     * Reduce处理类
     * 数据汇总
     */
    public static class ProvinceReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

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
        job.setJarByClass(ProvinceDriver.class);

        //指定Mapper与Reducer业务处理类
        job.setMapperClass(ProvinceDriver.ProvinceMapper.class);
        job.setReducerClass(ProvinceDriver.ProvinceReducer.class);

        //指定Mapper输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //指定Reducer输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //指定输入输出文件的路径或目录
        FileInputFormat.setInputPaths(job, new Path("hdfs://hadoop101:9000/data/userimgs/input/paylog.txt"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop101:9000/data/userimgs/output/province"));

        //等待作业执行完毕
        boolean result = job.waitForCompletion(true);
        System.out.println("Finish...");

    }

}

package com.userimg.hdfs.impl;

import com.userimg.hdfs.IHDFSDao;
import com.userimg.hdfs.impl.HDFSDaoImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;

/**
 * 测试HDFS操作
 * @Author xj
 * @CreateTime 2021/3/16 11:16
 * @Version 1.0.0
 */
public class TestHDFS {

    public static final String uri = "hdfs://202.198.100.101:9000";
    public static final String user = "tom";
    public static final Configuration configuration = new Configuration();


    /**
     * 创建目录
     *
     * @throws Exception
     */
    @Test
    public void testMkdir() throws Exception {
        FileSystem fs = FileSystem.get(new URI(uri), configuration, user);

        Path path = new Path("/data/userimgs");
        //创建目录
        fs.mkdirs(path);
        //关闭文件
        fs.close();
        System.out.println("success...");
    }


    /**
     * 上传文件
     *
     * @throws Exception
     */
    @Test
    public void testUploadFile() throws Exception {
        //获得文件系统对象
        FileSystem fs = FileSystem.get(new URI(uri), configuration, user);
        //创建文件路径
        //源文件路径
        Path sourcePath = new Path("D:/myfile/paylog.txt");
        //目标文件的路径
        Path targetPath = new Path("/data/userimgs/input/paylog.txt");
        //复制文件数据
        fs.copyFromLocalFile(sourcePath, targetPath);
        //关闭文件系统
        fs.close();
        System.out.println("Success......");
    }


    /**
     * 下载文件
     *
     * @throws Exception
     */
    @Test
    public void testDownload() throws Exception {
        //获得文件系统对象
        FileSystem fs = FileSystem.get(new URI(uri), configuration, user);
        //创建文件路径
        //源文件路径
        Path sourcePath = new Path("/data/userimgs/input/paylog.txt");
        //目标文件的路径
        Path targetPath = new Path("D:/myfile/paylog.txt");
        //复制文件数据
        fs.copyToLocalFile(sourcePath, targetPath);
        //关闭文件系统
        fs.close();
        System.out.println("Success download...");
    }


    /**
     * 文件的读取，操作输入流
     */
    @Test
    public void testReadFile() throws Exception {
        FileSystem fs = FileSystem.get(new URI(uri), configuration, user);
        //创建读取文件的路径
        Path path = new Path("/data/userimgs/input/paylog.txt");
        //打开文件系统的文件数据输入流
        FSDataInputStream in = fs.open(path);
        //创建缓冲字符输出流
        BufferedReader br = new BufferedReader(new InputStreamReader(in, "GBK"));
        //循环读取输出
        String line;
        while ((line = br.readLine()) != null) {
            System.out.println(line);
        }
        //关闭流，清理资源
        br.close();
        in.close();
        fs.close();

    }

    /**
     * 删除文件
     * @throws Exception
     */
    @Test
    public void testDeleteFile () throws Exception{
        FileSystem fs = FileSystem.get(new URI(uri), configuration, user);
        Path path = new Path("/data/userimgs/input/paylog.txt");
        //删除文件，true才支持递归删除
        fs.delete(path, false);
        fs.close();
        System.out.println("success delete...");
    }

    @Test
    public void testIHDFSDao() throws Exception{
        //创建访问接口的对象
        IHDFSDao ihdfsDao = new HDFSDaoImpl();
//        //上传文件
//        ihdfsDao.UploadFile("D:/myfile/part-00001.txt","/data/input");
//        //读取文件
//        Map<String, Object> result = ihdfsDao.readFile("/data/input/part-00001.txt");
//        System.out.println(result.get("weixin"));
//        System.out.println(result.get("zhifubao"));
//        System.out.println(result.get("bankcard"));
//        //创建目录
//        ihdfsDao.Mkdir("/data/input3");
//        //下载文件到本地
//        ihdfsDao.DownloadFile("/data/input/part-00001.txt", "D:/myfile/name-00001.txt");
//        System.out.println("sdfdsffa");

    }


}
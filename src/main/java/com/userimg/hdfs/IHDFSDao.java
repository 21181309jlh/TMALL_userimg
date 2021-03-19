package com.userimg.hdfs;
import org.apache.hadoop.conf.Configuration;

import java.util.Map;

/**
 * HDFS数据访问接口
 * @version 1.0.0
 * @Auther jialinhan
 * @CreatTime 2021/3/16 14:00
 */

public interface IHDFSDao {
    public static final String uri="hdfs://hadoop101:9000";
    public static final String user="tom";
    public static final Configuration configuration= new Configuration();
    /**
     * 上传文件
     * @param sourceUri
     * @param targetUri
     * @throws Exception
     */
    public void uploadFile(String sourceUri, String targetUri) throws Exception;

    /**
     * 读取文件
     * @param sourceUri 读取文件的路径
     * @return
     * @throws Exception
     */
    public Map<String,Object>readFile(String sourceUri) throws Exception;
    /**
     * 创建目录
     * @param targetUri 创建目录的位置
     * @throws Exception
     */
    public void mkdir(String targetUri) throws Exception;
    /**
     * 下载文件
     * @param sourceUri 要下载文件的路径
     * @param targetUri 存储到本地的路径
     * @throws Exception
     */
    public void downloadFile(String sourceUri, String targetUri) throws Exception;

    /**
     * 删除文件
     * @param sourceUri 要删除文件的路径
     * @param digui 是否递归删除
     * @throws Exception
     */
    public void deleteFile(String sourceUri, boolean digui) throws Exception;
}

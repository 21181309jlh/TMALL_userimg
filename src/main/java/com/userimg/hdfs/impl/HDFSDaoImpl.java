package com.userimg.hdfs.impl;

import com.userimg.hdfs.IHDFSDao;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * @version 1.0.0
 * @Auther jialinhan
 * @CreatTime 2021/3/16 14:10
 */
public class HDFSDaoImpl implements IHDFSDao {
    @Override
    public void uploadFile(String sourceUri,String targerUri)throws Exception{
        FileSystem fs=FileSystem.get(new URI(uri),configuration,user);
        Path sourcePath=new Path(sourceUri);
        Path targetPath=new Path(targerUri);
        fs.copyFromLocalFile(sourcePath,targetPath);
        fs.close();
    }
    @Override
    public Map<String,Object>readFile(String sourceUri)throws Exception{
        FileSystem fs=FileSystem.get(new URI(uri),configuration,user);
        Path sourcePath=new Path(sourceUri);
        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(sourcePath)));
        Map<String,Object>result =new HashMap<>();
        String line;
        while ((line=br.readLine())!=null){
            String[] kv=line.split("\t");
            result.put(kv[0],kv[1]);
        }
        fs.close();
        return result;
    }
    @Override
    public void mkdir(String targetUri) throws Exception {
        FileSystem fs = FileSystem.get(new URI(uri), configuration, user);
        Path path = new Path(targetUri);
        fs.mkdirs(path);
        fs.close();
    }

    @Override
    public void downloadFile(String sourceUri, String targetUri) throws Exception {
        FileSystem fs = FileSystem.get(new URI(uri), configuration, user);
        Path sourcePath = new Path(sourceUri);
        Path targetPath = new Path(targetUri);
        fs.copyToLocalFile(sourcePath, targetPath);
        fs.close();
    }

    @Override
    public void deleteFile(String sourceUri, boolean digui) throws Exception {
        FileSystem fs = FileSystem.get(new URI(uri), configuration, user);
        Path path = new Path(sourceUri);
        fs.delete(path, digui);
        fs.close();
    }
}

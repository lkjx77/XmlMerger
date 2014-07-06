package com.hp.actsights.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.net.URI;

/**
 * Created by kim on 7/2/14.
 */

public class DataProcessUtil {

    public DataProcessUtil(Configuration conf) {
        this.conf = conf;
    }

    public Configuration getConf() {
        return conf;
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    private Configuration conf;

    public void copyToHDFS() {
//        try {
//            FileSystem fs = FileSystem.get(conf);
//            FileUtil.copy(new File("/home/kim/IdeaProjects/actinsights-useranalytic/data"), fs, new Path("profiles"), false, conf);
        String defaultURI = FileSystem.getDefaultUri(conf).toString();
            try {
                Configuration conf=new Configuration();
                FileSystem src=FileSystem.getLocal(conf);

//                FileSystem dst= FileSystem.get(conf);

                FileSystem dst = FileSystem.get(new URI("hdfs://localhost:50070"), conf);
                FileSystem.get(URI.create("hdfs://localhost:50070"),conf);

//                Path phdfs = new Path(
//                        "hdfs://hadoop1.devqa.local:8020/user/hdfs/java/");

                Path srcpath = new Path("/home/kim/IdeaProjects/actinsights-useranalytic/data");
                Path dstpath = new Path("/profile");
//                Path dstpath = new Path(defaultURI);
                FileUtil.copy(src, srcpath, dst, dstpath,false,conf);

//                FileUtil.copy(src, srcpath, dstpath, false, conf);
                for(String files: FileUtil.list(new File("/home/kim/IdeaProjects/actinsights-useranalytic/data"))){
                    System.out.println("file : " + files);
                };


                System.out.println("defaultURI: " + defaultURI);

            } catch (Exception e) {
                e.printStackTrace();
            }
//            FileSystem fs = FileSystem.get(conf);
//            fs.copyFromLocalFile(false, new Path("/home/kim/IdeaProjects/actinsights-useranalytic/data"), new Path("tmp"));
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }

    public void copyFromHDFS() {
        try {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://localhost:54310/user/hadoop/");
            FileSystem fs = FileSystem.get(conf);
            FileStatus[] status = fs.listStatus(new Path("hdfsdirectory"));
            for (int i = 0; i < status.length; i++) {
                System.out.println(status[i].getPath());
                fs.copyToLocalFile(false, status[i].getPath(), new Path("localdir"));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //upload a local file 上传文件
    public void testUploadLocalFile() throws Exception{
        try{
            String hdfsUrl = "hdfs://localhost:9000";
//            String hdfsUrl = "hdfs://localhost";

            Configuration conf= new Configuration();
            FileSystem fs = FileSystem.get(URI.create(hdfsUrl),conf);
            Path src = new Path("/home/kim/IdeaProjects/actinsights-useranalytic/data");
            Path dst = new Path("/test");
            fs.copyFromLocalFile(src, dst);

        }catch(Exception e){
            e.printStackTrace();
        }
    }


}

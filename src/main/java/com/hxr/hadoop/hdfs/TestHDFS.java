package com.hxr.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;

public class TestHDFS {

    Configuration conf;
    FileSystem fs;

    @Before
    public void conn() throws IOException {
        conf = new Configuration(true);
        fs = FileSystem.get(conf);

    }


    @After
    public void close() throws IOException {
        fs.close();
    }


    @Test
    public void mkdir() throws Exception{


        Path ifile = new Path("/ooxx");
        if(fs.exists(ifile)){
            fs.delete(ifile,true);
        }
        fs.mkdirs(ifile);
    }


    @Test
    public void upload() throws IOException {
        Path f = new Path("/ooxx/hello.txt");
        FSDataOutputStream output = fs.create(f);
        InputStream input = new BufferedInputStream(new FileInputStream(new File("D:\\elastext.txt")));
        IOUtils.copyBytes(input,output,conf,true);
    }

    @Test
    public void blks() throws IOException {
        Path i = new Path("/user/root/test.txt");
        FileStatus fileStatus = fs.getFileStatus(i);
        BlockLocation[] blks = fs.getFileBlockLocations(fileStatus,0,fileStatus.getLen());

        for (BlockLocation blk:blks) {
            System.out.println(blk);
        }

        FSDataInputStream in = fs.open(i);
        System.out.print((char)in.readByte());
        System.out.print((char)in.readByte());
        System.out.print((char)in.readByte());
        System.out.print((char)in.readByte());
        System.out.print((char)in.readByte());
        in.seek(1048576);
        System.out.println();
        System.out.print((char)in.readByte());
        System.out.print((char)in.readByte());
        System.out.print((char)in.readByte());
        System.out.print((char)in.readByte());
        System.out.print((char)in.readByte());
    }
}

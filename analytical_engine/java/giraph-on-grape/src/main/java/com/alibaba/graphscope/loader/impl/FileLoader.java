package com.alibaba.graphscope.loader.impl;

import com.alibaba.graphscope.loader.LoaderBase;
import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.alibaba.graphscope.loader.LoaderUtils.checkFileExist;

/**
 * Load from a file on system.
 */
public class FileLoader implements LoaderBase {
    private static Logger logger = LoggerFactory.getLogger(FileLoader.class);
    private int threadNum;

    public FileLoader(){

    }

    public boolean init(int threadNum, String inputPath, String vertexInputFormatClass){
        this.threadNum = threadNum;
        if (!checkFileExist(inputPath)){
            logger.error("Input file doesn't not exist.");
            return false;
        }
        return true;
    }

    @Override
    public LoaderBase.TYPE loaderType() {
        return TYPE.FileLoader;
    }

    @Override
    public int concurrency() {
        return threadNum;
    }
}

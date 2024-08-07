package com.alibaba.graphscope.example.intVid;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public abstract class FileObjectStorage implements IObjectStorage{
    private String path;
    private static Logger logger = LoggerFactory.getLogger(FileObjectStorage.class);
    private boolean append;
    public FileObjectStorage(String path, boolean append) {
        this.path = path;
        this.append = append;
    }

    public abstract void loadObjects(ObjectInputStream in);
    public abstract void dumpObjects(ObjectOutputStream out);

    @Override
    public void load() {
        try {
            FileInputStream fileIn = new FileInputStream(path);
            ObjectInputStream in = new ObjectInputStream(fileIn);
            logger.info("load objects from file: " + path);
            loadObjects(in);
            in.close();
            fileIn.close();
            logger.info("load objects from file: " + path + " done");
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void dump() {
        try {
            // the old content will be cleared.
            ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream
                    (path,append));
            logger.info("dump objects to file: " + path);
            dumpObjects(out);
            out.close();
            logger.info("dump objects to file: " + path + " done");
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}

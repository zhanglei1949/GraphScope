package com.alibaba.graphscope.loader;

import java.io.File;

public class LoaderUtils {
    public static boolean checkFileExist(String path){
        File temp;
        temp = new File(path);
        return temp.exists();
    }
}

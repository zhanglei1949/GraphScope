package com.alibaba.graphscope.loader;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

public class LoaderUtils {
    public static boolean checkFileExist(String path){
        File temp;
        temp = new File(path);
        return temp.exists();
    }

    public static long getNumLinesOfFile(String path){
        ProcessBuilder builder = new ProcessBuilder("wc", "-l", path);
        builder.inheritIO().redirectOutput(ProcessBuilder.Redirect.PIPE);
        Process process = null;
        try {
            process = builder.start();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))){
                String res = reader.readLine();
                return Long.parseLong(res);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }
}

package org.apache.giraph.utils;

import com.alibaba.fastjson.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class GiraphParamsChecker {

    private static Logger logger = LoggerFactory.getLogger(GiraphParamsChecker.class.getName());

    private static final String[] keys = {
        "input_format_class",
        "output_format_class",
        "app_class",
        "input_vfile",
        "input_efile",
        "output_path",
        "aggregator_class",
        "combiner_class",
        "resolver_class"
    };

    public static void verifyClasses(String classPath, String classes) {
        URLClassLoader classLoader =
                new URLClassLoader(
                        classPath2URLArray(classPath), GiraphParamsChecker.class.getClassLoader());

        JSONObject jsonObject = JSONObject.parseObject(classes);
        String errorKey;

        for (String key : keys) {
            if (jsonObject.containsKey(key)) {
                try {
                    Class<?> clz = classLoader.loadClass("key");
                } catch (Exception e) {
                    e.printStackTrace();
                    logger.error("Fatal error in verifying classes");
                    return;
                }
            } else {
                logger.error(key + " is  not specified");
                return;
            }
        }
    }

    private static URL[] classPath2URLArray(String classPath) {
        if (Objects.isNull(classPath) || classPath.length() == 0) {
            System.err.println("Empty class Path!");
            return new URL[] {};
        }
        String[] splited = classPath.split(":");
        List<URL> res =
                Arrays.stream(splited)
                        .map(File::new)
                        .map(
                                file -> {
                                    try {
                                        return file.toURL();
                                    } catch (MalformedURLException e) {
                                        e.printStackTrace();
                                    }
                                    return null;
                                })
                        .collect(Collectors.toList());
        System.out.println(
                "Extracted URL"
                        + String.join(
                                ":", res.stream().map(URL::toString).collect(Collectors.toList())));
        URL[] ret = new URL[splited.length];
        for (int i = 0; i < splited.length; ++i) {
            ret[i] = res.get(i);
        }
        return ret;
    }
}

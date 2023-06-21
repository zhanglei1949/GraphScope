/*
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphscope.common.utils;

import com.google.common.io.Resources;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

public class FileUtils {
  public static String readJsonFromResource(String file) {
    try {
      URL url = Thread.currentThread().getContextClassLoader().getResource(file);
      return Resources.toString(url, StandardCharsets.UTF_8).trim();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static String readCypherQueryFromFile(String filename) {
    // read cypher query from file
    StringBuilder sb = new StringBuilder();
    try {
      File file = new File(filename);
      FileReader fileReader = new FileReader(file);
      BufferedReader bufferedReader = new BufferedReader(fileReader);
      String line;
      while ((line = bufferedReader.readLine()) != null) {
        System.out.println(line);
        sb.append(line);
        sb.append("\n");
      }
      bufferedReader.close();
      fileReader.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return sb.toString();
  }
}

package com.laurence.tstream.util;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by laurencewelch on 3/24/15.
 */
public class Util {

  public static void configureTwitterCredentials(String filename) throws Exception {
    List<String> lines = new Util().readLines(filename);
    if (lines == null || lines.size() == 0) {
      throw new Exception("Could not find configuration file " + filename);
    }

    HashMap<String, String> map = new HashMap<String, String>();
    for (int i = 0; i < lines.size(); i++) {
      String line  = lines.get(i);
      String[] splits = line.split("=");
      if (splits.length != 2) {
        throw new Exception("Error parsing configuration file - incorrectly formatted line [" + line + "]");
      }
      map.put(splits[0].trim(), splits[1].trim());
    }
    String[] configKeys = { "consumerKey", "consumerSecret", "accessToken", "accessTokenSecret" };
    for (int k = 0; k < configKeys.length; k++) {
      String key = configKeys[k];
      String value = map.get(key);
      if (value == null) {
        throw new Exception("Error setting OAuth authentication - value for " + key + " not found");
      } else if (value.length() == 0) {
        throw new Exception("Error setting OAuth authentication - value for " + key + " is empty");
      }
      String fullKey = "twitter4j.oauth." + key;
      System.setProperty(fullKey, value);
      System.out.println("\tProperty " + fullKey + " set as " + value);
    }
    System.out.println();
  }
  public  List<String> readLines(String file)  {

    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(this.getClass().getResourceAsStream(file)));
    List<String> lines = new ArrayList<String>();
    String line = null;
    try{
      while ((line = bufferedReader.readLine()) != null) {
        if (line.length() > 0) lines.add(line);
      }
      bufferedReader.close();
      return lines;
    } catch(Exception ex){
    }
   return lines;
  }


}

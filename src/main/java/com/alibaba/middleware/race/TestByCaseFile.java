package com.alibaba.middleware.race;

import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * Created by hahong on 2016/7/19.
 */
public class TestByCaseFile {
    public static String[] getKeys(String str) {
        String[] keys = str.replace("KEYS:[", "").replace("]", "").split(",");
        for (int i = 0; i < keys.length; ++i) {
            keys[i] = keys[i].trim();
        }
        return keys;
    }
    public static void main(String args[]) {
        String filename = "D:\\middleware-data\\prerun_data\\prerun_data\\case.0";
        try {
            FileInputStream fis = new FileInputStream(filename);
            BufferedReader br = new BufferedReader(new InputStreamReader(fis, StandardCharsets.UTF_8));
            String cs = br.readLine();
            while (true) {
                if (cs.contains("QUERY_ORDER")) {
                    long orderid = Long.parseLong(br.readLine().replace("ORDERID:", ""));
                    String[] keys = getKeys(br.readLine());
                    
                } else if (cs.contains("QUERY_BUYER_TSRANGE")) {

                } else if (cs.contains("QUERY_SALER_GOOD")) {

                } else {

                }
                br.readLine();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

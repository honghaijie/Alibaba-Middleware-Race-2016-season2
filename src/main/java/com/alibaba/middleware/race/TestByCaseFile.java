package com.alibaba.middleware.race;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
    public static void checkResult(String str, OrderSystem.Result r) {
        Pattern pattern = Pattern.compile("orderid:(\\d+)");
        Matcher matcher = pattern.matcher(str);
        long orderId = Long.parseLong(matcher.group(1));
        if (r.orderId() != orderId) {
            System.err.printf("Error, key: %s, expected value: %d, actural value: %d\n", "orderid", orderId, r.orderId());
        }
        pattern = Pattern.compile("KV:[(.*)]");
        matcher = pattern.matcher(str);
        String kvStr = matcher.group(1);
        String[] kvs = kvStr.split(",");
        for (String kv : kvs) {
            String t = kv.trim();
            if (t.equals("")) continue;
            String[] tp = t.split(":");
            String k = tp[0], v = tp[1];
            if (!r.get(k).equals(v)) {
                System.err.printf("Error, key: %s, expected value: %s, actural value: %s\n", k, v, r.get(k));
            }
        }
    }
    public static void main(String args[]) throws IOException, InterruptedException {
        long beginTime = System.currentTimeMillis();
        String folder = "D:\\middleware-data\\prerun_data\\prerun_data\\";
        List<String> orderFiles = Arrays.asList(folder + "order.0.0", folder + "order.0.3", folder + "order.1.1", folder + "order.2.2");
        List<String> goodFiles = Arrays.asList(folder + "good.0.0", folder + "good.1.1", folder + "good.2.2");
        List<String> buyerFiles = Arrays.asList(folder + "buyer.0.0", folder + "buyer.1.1");
        List<String> storeFolders = Arrays.asList("D:\\middleware-data\\prerun_data\\prerun_data\\store\\");

        OrderSystemImpl osi = new OrderSystemImpl();

        osi.construct(orderFiles, buyerFiles, goodFiles, storeFolders);

        String filename = "D:\\middleware-data\\prerun_data\\prerun_data\\case.0";
        try {
            FileInputStream fis = new FileInputStream(filename);
            BufferedReader br = new BufferedReader(new InputStreamReader(fis, StandardCharsets.UTF_8));
            while (true) {
                String cs = br.readLine();
                if (cs.contains("QUERY_ORDER")) {
                    long orderid = Long.parseLong(br.readLine().replace("ORDERID:", ""));
                    String[] keys = getKeys(br.readLine());
                    OrderSystem.Result r = osi.queryOrder(orderid, Arrays.asList(keys));
                    br.readLine();
                    while (true) {
                        String t = br.readLine();
                        checkResult(t, r);
                        if (t.equals("}")) break;
                    }
                    br.readLine();
                } else if (cs.contains("QUERY_BUYER_TSRANGE")) {
                    String buyerid = br.readLine().replace("BUYERID:", "");
                    long startTime = Long.parseLong(br.readLine().replace("STARTTIME:", ""));
                    long endTime = Long.parseLong(br.readLine().replace("ENDTIME:", ""));
                    Iterator<OrderSystem.Result> r = osi.queryOrdersByBuyer(startTime, endTime, buyerid);
                    while (true) {
                        OrderSystem.Result sr = r.next();
                        String t = br.readLine();
                        checkResult(t, sr);
                        if (t.equals("}")) break;
                    }
                    if (r.hasNext()) {
                        System.err.println("Error, extra result found.");
                    }
                    br.readLine();
                } else if (cs.contains("QUERY_SALER_GOOD")) {
                    String salerid = br.readLine().replace("SALERID:", "");
                    String goodid = br.readLine().replace("GOODID:", "");
                    String[] keys = getKeys(br.readLine());
                    Iterator<OrderSystem.Result> r = osi.queryOrdersBySaler(salerid, goodid, Arrays.asList(keys));
                    br.readLine();
                    while (true) {
                        OrderSystem.Result sr = r.next();
                        String t = br.readLine();
                        checkResult(t, sr);
                        if (t.equals("}")) break;
                    }
                    if (r.hasNext()) {
                        System.err.println("Error, extra result found.");
                    }
                    br.readLine();
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

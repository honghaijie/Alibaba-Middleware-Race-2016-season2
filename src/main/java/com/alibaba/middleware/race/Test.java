package com.alibaba.middleware.race;

import java.io.*;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created by hahong on 2016/7/19.
 */
public class Test {

    public static void main(String args[]) throws IOException, InterruptedException {
        long startTime = System.currentTimeMillis();
        String folder = "D:\\middleware-data\\prerun_data\\prerun_data\\";
        List<String> orderFiles = Arrays.asList(folder + "order.0.0", folder + "order.0.3", folder + "order.1.1", folder + "order.2.2");
        List<String> goodFiles = Arrays.asList(folder + "good.0.0", folder + "good.1.1", folder + "good.2.2");
        List<String> buyerFiles = Arrays.asList(folder + "buyer.0.0", folder + "buyer.1.1");
        List<String> storeFolders = Arrays.asList("D:\\middleware-data\\prerun_data\\prerun_data\\store\\");

        OrderSystemImpl osi = new OrderSystemImpl();

        osi.construct(orderFiles, buyerFiles, goodFiles, storeFolders);

        String s = "aliyun_694d9233-ca7a-436d-a235-9412aac0c31f";
        String buyerid = "tb_9a20ec63-cc2e-4056-b8c5-238ed94f2ec6";
        //List<String> ans = osi.QueryOrderByBuyer(Utils.hash(buyerid), 1470668508, 5463667280L, osi.orderBuyerIndexOffset, osi.sortedOrderBuyerIndexBlockFiles);
/*
        List<String> ans = Arrays.asList(osi.QueryBuyerByBuyer("tb_171da9af-8527-45cc-97f9-e4fb6da4aee6"));
        System.out.println(ans.size());
        for (String e : ans) {
            System.out.println(e);
        }
*/
        /*
        Iterator<OrderSystem.Result> ans = Arrays.asList(osi.queryOrder(624813187L, Arrays.asList("amount"))).iterator();
        while (ans.hasNext()) {
            OrderSystem.Result r = ans.next();
            for (OrderSystem.KeyValue k : r.getAll()) {
                System.out.print(k.key() + ":" + k.valueAsString() + ", ");
            }
            System.out.println();
        }
        System.out.printf("Time: %f\n", (System.currentTimeMillis() - startTime) / 1000.0);
        */
        OrderSystem.KeyValue ans = osi.sumOrdersByGood("aye-a156-162c1d69d26c", "a_o_22304");
        System.out.print(ans.key() + ":" + ans.valueAsString() + ", ");

        System.out.printf("Time: %f\n", (System.currentTimeMillis() - startTime) / 1000.0);
    }
}

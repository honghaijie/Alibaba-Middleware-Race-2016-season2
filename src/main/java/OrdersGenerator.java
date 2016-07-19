import java.io.*;
import java.util.Random;

import static com.alibaba.middleware.race.Utils.ClearFile;

/**
 * Created by hahong on 2016/6/14.
 */
public class OrdersGenerator {
    public static void main(String[] args) {
        String filename = "D:\\middleware-data\\order-random.txt";
        ClearFile(filename);
        File fout = new File(filename);
        FileOutputStream fos = null;
        Random rd = new Random();
        try {
            fos = new FileOutputStream(fout);
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos, "UTF-8"));
            //orderid:3003844	goodid:goodxn_f4c8cc36-b325-431c-83cc-b27801f15194	buyerid:tb_94fc46f8-aa42-4090-9ab5-174987031ec3	createtime:1469834935	done:true	amount:73	remark:灵通十四分联营惊喜玉府家尊	app_order_33_2:11552.8
            for (int i = 0; i < 50000000; ++i) {
                long orderId = Math.abs(rd.nextLong());
                String goodId = java.util.UUID.randomUUID().toString();
                String buyerId = java.util.UUID.randomUUID().toString();
                long createTime = Math.abs(rd.nextLong());
                boolean done = rd.nextBoolean();
                long amount = Math.abs(rd.nextInt()) % 1000;
                String remark = "It's the remark of " + orderId;
                String output = String.format("orderid:%d\tgoodid:%s\tbuyerid:%s\tcreatetime:%d\tdone:%b\tamount:%d", orderId, goodId, buyerId, createTime, done, amount);
                bw.write(output + "\n");
                if (i % 1000000 == 0) {
                    System.out.println(output);
                }
            }
            bw.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
        }
    }
}

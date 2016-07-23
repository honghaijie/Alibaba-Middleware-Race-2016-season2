package com.alibaba.middleware.race.diskio;

import java.io.*;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by hahong on 2016/7/23.
 */
public class DiskStringReader {
    BlockingQueue<String> q;
    BufferedReader br;
    Thread t;
    int bufferSize = 1024 * 1024;
    int capacity = 1000;
    public DiskStringReader(String filename) throws FileNotFoundException {
        q = new ArrayBlockingQueue<String>(capacity);
        File file = new File(filename);
        InputStreamReader isr = null;
        try {
            isr = new InputStreamReader(new FileInputStream(file), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        br = new BufferedReader(isr, bufferSize);
        startReadThread();
    }
    void startReadThread() {
        t = new Thread() {
            public void run() {
                while (true) {
                    try {
                        String msg = br.readLine();
                        if (msg != null) {
                            q.put(msg);
                        } else {
                            q.put("#");
                            break;
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        };
        t.start();
    }
    public String readLine() {
        String s = null;
        try {
            s = q.take();
            if (s.equals("#")) {
                s = null;
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return s;
    }
    public void close() throws IOException {
        br.close();
    }
}

package com.alibaba.middleware.race.diskio;

import com.alibaba.middleware.race.Tuple;

import java.io.*;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by hahong on 2016/7/23.
 */
public class DiskStringReader {
    BlockingQueue<Tuple<String, String>> q;
    BufferedReader br;
    Thread t;
    int bufferSize = 2 * 1024 * 1024;
    int capacity = 100000;
    Iterator<String> files;
    String currentFilename = null;
    public DiskStringReader(Collection<String> filenames) {
        try {
            files = filenames.iterator();
            q = new ArrayBlockingQueue<>(capacity);
            br = null;
            startReadThread();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    void startReadThread() {
        t = new Thread() {
            public void run() {
                while (true) {
                    try {
                        if (br == null) {
                            if (!files.hasNext()) {
                                q.put(new Tuple<String, String>("#", null));
                                break;
                            }
                            currentFilename = files.next();
                            File file = new File(currentFilename);

                            InputStreamReader isr = null;
                            try {
                                isr = new InputStreamReader(new FileInputStream(file), "UTF-8");
                            } catch (UnsupportedEncodingException e) {
                                e.printStackTrace();
                            }
                            br = new BufferedReader(isr, bufferSize);
                        }
                        String msg = br.readLine();
                        if (msg != null) {
                            q.put(new Tuple<String, String>(msg, currentFilename));
                        } else {
                            br.close();
                            br = null;
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
        Tuple<String, String> t = readLineAndFileName();
        if (t == null) return null;
        return t.x;
    }
    public Tuple<String, String> readLineAndFileName() {
        Tuple<String, String> s = null;
        try {
            s = q.take();
            if (s.y == null) {
                s = null;
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return s;
    }
}

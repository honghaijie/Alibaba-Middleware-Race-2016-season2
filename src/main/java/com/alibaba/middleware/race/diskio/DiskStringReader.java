package com.alibaba.middleware.race.diskio;

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
    BlockingQueue<String> q;
    BufferedReader br;
    Thread t;
    int bufferSize = 2 * 1024 * 1024;
    int capacity = 1000;
    Iterator<String> files;
    public DiskStringReader(Collection<String> filenames) {
        try {
            files = filenames.iterator();
            q = new ArrayBlockingQueue<String>(capacity);
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
                                q.put("#");
                                break;
                            }
                            File file = new File(files.next());
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
                            q.put(msg);
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
    }
}

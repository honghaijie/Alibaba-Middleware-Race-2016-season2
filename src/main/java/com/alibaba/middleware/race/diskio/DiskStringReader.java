package com.alibaba.middleware.race.diskio;

import com.alibaba.middleware.race.Tuple;
import com.alibaba.middleware.race.Utils;

import java.io.*;
import java.nio.charset.StandardCharsets;
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
    public class ReadEntry {
        public String content;
        public String filename;
        public long offset;

        public ReadEntry(String content, String filename, long offset) {
            this.content = content;
            this.filename = filename;
            this.offset = offset;
        }
    }
    BlockingQueue<ReadEntry> q;
    BufferedReader br;
    Thread t;
    int bufferSize = 1024 * 1024;
    int capacity = 1000;
    Iterator<String> files;
    String currentFilename = null;
    long currentOffset = 0L;
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
                                q.put(new ReadEntry("#", null, -1));
                                q.put(new ReadEntry("#", null, -1));
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
                            q.put(new ReadEntry(msg, currentFilename, currentOffset));
                            currentOffset += Utils.UTF8Length(msg) + 1;
                        } else {
                            currentOffset = 0L;
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
        ReadEntry t = readLineAndFileName();
        if (t == null) return null;
        return t.content;
    }
    public ReadEntry readLineAndFileName() {
        ReadEntry s = null;
        try {
            s = q.take();
            if (s.filename == null) {
                s = null;
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return s;
    }
}

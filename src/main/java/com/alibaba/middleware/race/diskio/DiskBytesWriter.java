package com.alibaba.middleware.race.diskio;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by hahong on 2016/7/22.
 */
public class DiskBytesWriter {
    BlockingQueue<WriteMessage<byte[]>> q[];
    Map<String, BufferedOutputStream> filenameMapper = new HashMap<>(1000);
    Thread[] ths;
    int bufferSize = 512 * 1024;
    int capacity = 1000;
    int threadNum = 2;
    public DiskBytesWriter(Collection<String> files)  {
        try {
            q = new BlockingQueue[threadNum];
            for (int i = 0; i < threadNum; ++i) {
                q[i] = new ArrayBlockingQueue<WriteMessage<byte[]>>(capacity);
            }
            for (String filename : files) {
                filenameMapper.put(filename, new BufferedOutputStream(new FileOutputStream(filename), bufferSize));
            }
            startWriteThread();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public void write(String filename, byte[] bytes) {
        try {
            q[Math.abs(filename.hashCode()) % threadNum].put(new WriteMessage<byte[]>(filenameMapper.get(filename), bytes));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    void startWriteThread() {
        ths = new Thread[threadNum];
        for (int i = 0; i < threadNum; ++i) {
            final int v = i;
            ths[i] = new Thread() {
                public void run() {
                    while (true) {
                        try {
                            WriteMessage<byte[]> msg = q[v].take();
                            if (msg.isEnd()) {
                                break;
                            }
                            msg.outputStream.write(msg.content);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            };
            ths[i].start();
        }
    }
    public void close() {
        try {
            for (int i = 0; i < threadNum; ++i) {
                q[i].put(WriteMessage.END());
            }
            for (int i = 0; i < threadNum; ++i) {
                ths[i].join();
            }
            for (BufferedOutputStream bos : filenameMapper.values()) {
                bos.close();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}

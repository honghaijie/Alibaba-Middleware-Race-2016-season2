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
    BlockingQueue<WriteMessage<byte[]>> q;
    Map<String, BufferedOutputStream> filenameMapper = new HashMap<>(1000);
    Thread t;
    int bufferSize = 512 * 1024;
    int capacity = 1000;
    public DiskBytesWriter(Collection<String> files)  {
        try {
            q = new ArrayBlockingQueue<WriteMessage<byte[]>>(capacity);
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
            q.put(new WriteMessage<byte[]>(filenameMapper.get(filename), bytes));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    void startWriteThread() {
        t = new Thread() {
            public void run() {
                while (true) {
                    try {
                        WriteMessage<byte[]> msg = q.take();
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
        t.start();
    }
    public void close() {
        try {
            q.put(WriteMessage.END());
            t.join();
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

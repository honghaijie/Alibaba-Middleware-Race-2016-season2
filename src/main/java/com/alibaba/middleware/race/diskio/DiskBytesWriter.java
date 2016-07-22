package com.alibaba.middleware.race.diskio;

import com.alibaba.middleware.race.Tuple;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
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
    int bufferSize = 64 * 1024;
    int capacity = 10000;
    public DiskBytesWriter(Collection<String> files) throws FileNotFoundException {
        q = new ArrayBlockingQueue<WriteMessage<byte[]>>(capacity);
        for (String filename : files) {
            filenameMapper.put(filename, new BufferedOutputStream(new FileOutputStream(filename), bufferSize));
        }
        startWriteThread();
    }
    public void write(String filename, byte[] bytes) {
        try {
            q.put(new WriteMessage<byte[]>(filename, bytes));
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
                        filenameMapper.get(msg.filename).write(msg.content);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        };
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

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
 * Created by hahong on 2016/7/23.
 */
public class DiskBytesReader {
    BlockingQueue<WriteMessage<byte[]>> q;
    Map<String, BufferedOutputStream> filenameMapper = new HashMap<>(1000);
    Thread t;
    int bufferSize = 64 * 1024;
    int byteLength;
    public DiskBytesReader(Collection<String> files, int capacity, int byteLength) throws FileNotFoundException {
        this.byteLength = byteLength;
    }
    public byte[] read(String filename) {
        return null;
    }

}

package com.alibaba.middleware.race;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;

/**
 * Created by hahong on 2016/7/21.
 */
public class BigMappedByteBuffer {
    ByteBuffer[] buffers;
    long totalLength;
    long blockSize;
    int currentBuffer = 0;
    private BigMappedByteBuffer() {}
    public BigMappedByteBuffer(String filename, long blockSize) {
        try {
            this.blockSize = blockSize;
            FileChannel fc = FileChannel.open(Paths.get(filename));
            totalLength = fc.size();
            int blockNum = (int)((totalLength + blockSize - 1) / blockSize);
            buffers = new MappedByteBuffer[blockNum];
            int cnt = 0;
            for (long i = 0; i < totalLength; i += blockSize) {
                long currentBlockLength = Math.min(blockSize, totalLength - i);
                buffers[cnt++] = fc.map(FileChannel.MapMode.READ_ONLY, i, currentBlockLength);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public BigMappedByteBuffer slice() {
        BigMappedByteBuffer res = new BigMappedByteBuffer();
        res.totalLength = this.totalLength;
        res.blockSize = this.blockSize;
        res.buffers = new MappedByteBuffer[this.buffers.length];
        for (int i = 0; i < res.buffers.length; ++i) {
            res.buffers[i] = this.buffers[i].slice();
        }
        return res;
    }
    public void get(byte[] buf) {
        int remain = buffers[currentBuffer].remaining();
        if (remain >= buf.length) {
            buffers[currentBuffer].get(buf);
        } else {
            buffers[currentBuffer++].get(buf, 0, remain);
            buffers[currentBuffer].get(buf, remain, buf.length - remain);
        }
        if (!buffers[currentBuffer].hasRemaining()) {
            ++currentBuffer;
        }
    }
    public byte get() {
        byte res = buffers[currentBuffer].get();
        if (!buffers[currentBuffer].hasRemaining()) {
            ++currentBuffer;
        }
        return res;
    }
    public void get(byte[] buf, int offset, int length) {
        int remain = buffers[currentBuffer].remaining();
        if (remain >= length) {
            buffers[currentBuffer].get(buf, offset, length);
        } else {
            buffers[currentBuffer++].get(buf, offset, remain);
            buffers[currentBuffer].get(buf, offset + remain, length - remain);
        }
        if (!buffers[currentBuffer].hasRemaining()) {
            ++currentBuffer;
        }
    }
    public boolean hasRemaining() {
        return (currentBuffer != buffers.length);
    }
    public long remaining() {
        if (!hasRemaining()) {
            return 0;
        }
        if (currentBuffer == buffers.length - 1) return buffers[currentBuffer].remaining();
        return buffers[currentBuffer].remaining() + totalLength - (currentBuffer + 1) * blockSize;
    }
    public void position(long offset) {
        currentBuffer = (int)(offset / blockSize);
        int blockOffset = (int)(offset % blockSize);
        buffers[currentBuffer].position(blockOffset);
    }
}

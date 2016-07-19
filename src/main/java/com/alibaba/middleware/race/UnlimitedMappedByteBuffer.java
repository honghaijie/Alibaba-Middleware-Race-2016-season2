package com.alibaba.middleware.race;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by hahong on 2016/6/14.
 */
public class UnlimitedMappedByteBuffer {
    long BlockSize = 0x70000000L;
    int PageSize = 4096;
    private MappedByteBuffer[] buffers;
    private RandomAccessFile raf;
    private FileChannel fc;
    public void UnlimitedMappedByteBuffer(String filename, long length) throws IOException {
        long blockNum = (length + BlockSize - 1) / BlockSize;
        buffers = new MappedByteBuffer[(int)blockNum];
        raf = new RandomAccessFile(filename, "rw");
        fc = raf.getChannel();
        long offset = 0;
        int bufferId = 0;
        while (offset < length) {
            long blockLength = Math.min(BlockSize, length - offset);
            buffers[bufferId] = fc.map(FileChannel.MapMode.READ_WRITE, offset, blockLength);
            offset += blockLength;
            bufferId += 1;
        }
    }
    public byte[] get(long offset) {
        int blockId = (int)((PageSize * offset) / BlockSize);
        int blockOffset = (int)((PageSize * offset) % BlockSize);
        byte[] ans = new byte[PageSize];
        buffers[blockId].get(ans, blockOffset, PageSize);
        return ans;
    }
    public void set(long offset, byte[] bytes) {
        int blockId = (int)((PageSize * offset) / BlockSize);
        int blockOffset = (int)((PageSize * offset) % BlockSize);
        byte[] ans = new byte[PageSize];
        buffers[blockId].put(ans, blockOffset, PageSize);
    }
    public void Close() throws IOException {
        fc.close();
        raf.close();
    }
}

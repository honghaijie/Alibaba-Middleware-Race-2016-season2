package com.alibaba.middleware.race;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Created by hahong on 2016/7/21.
 */
public class ByteBufferBackedInputStream extends InputStream {

    BigMappedByteBuffer buf;
    int cur;

    public ByteBufferBackedInputStream(BigMappedByteBuffer buf, int offset) {
        this.buf = buf;
        buf.position(offset);
        this.cur = 0;
    }

    public int read() throws IOException {
        if (!buf.hasRemaining()) {
            return -1;
        }

        return buf.get() & 0xFF;
    }

    public int read(byte[] bytes, int off, int len)
            throws IOException {
        if (!buf.hasRemaining()) {
            return -1;
        }
        long remaining = buf.remaining();
        if (remaining > Integer.MAX_VALUE) {
            remaining = Integer.MAX_VALUE;
        }
        len = Math.min(len, (int)remaining);
        buf.get(bytes, off, len);
        return len;
    }
}

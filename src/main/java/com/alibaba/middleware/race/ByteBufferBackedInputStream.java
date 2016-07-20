package com.alibaba.middleware.race;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Created by hahong on 2016/7/21.
 */
public class ByteBufferBackedInputStream extends InputStream {

    ByteBuffer buf;
    int cur;

    public ByteBufferBackedInputStream(ByteBuffer buf, int offset) {
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

        len = Math.min(len, buf.remaining());
        buf.get(bytes, off, len);
        return len;
    }
}

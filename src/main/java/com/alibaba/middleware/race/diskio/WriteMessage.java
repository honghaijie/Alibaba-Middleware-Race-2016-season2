package com.alibaba.middleware.race.diskio;

import java.io.BufferedOutputStream;

/**
 * Created by hahong on 2016/7/22.
 */
public class WriteMessage<T> {
    public static WriteMessage END() {
        return new WriteMessage(null, null);
    }
    public BufferedOutputStream outputStream;
    public T content;
    public WriteMessage(BufferedOutputStream file, T content) {
        this.outputStream = file;
        this.content = content;
    }
    public boolean isEnd() {
        return this.outputStream == null;
    }
}

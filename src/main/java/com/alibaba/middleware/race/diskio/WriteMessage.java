package com.alibaba.middleware.race.diskio;

/**
 * Created by hahong on 2016/7/22.
 */
public class WriteMessage<T> {
    public static WriteMessage END() {
        return new WriteMessage(null, null);
    }
    public String filename;
    public T content;
    public WriteMessage(String filename, T content) {
        this.filename = filename;
        this.content = content;
    }
    public boolean isEnd() {
        return this.filename == null;
    }
}

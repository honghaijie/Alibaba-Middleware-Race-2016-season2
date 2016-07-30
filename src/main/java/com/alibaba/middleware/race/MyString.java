package com.alibaba.middleware.race;

/**
 * Created by hahong on 2016/7/30.
 */
public class MyString {
    char[] c;
    int offset;
    int len;
    public MyString(char[] c) {
        this.c = c;
        this.offset = offset;
        this.len = len;
    }
    public MyString(char[] c, int offset, int len) {
        this.c = c;
        this.offset = offset;
        this.len = len;
    }
    public MyString subString(int start, int len) {
        return new MyString(c, offset + start, len);
    }
    public char charAt(int i) {
        return c[offset + i];
    }
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof MyString)) return false;
        final MyString t=(MyString)obj;
        if (this.len != t.len) return false;
        for (int i = 0; i < len; ++i) {
            if (this.charAt(i) != t.charAt(i)) {
                return false;
            }
        }
        return true;
    }
}

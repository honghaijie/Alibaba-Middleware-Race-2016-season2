package com.alibaba.middleware.race;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * Created by hahong on 2016/7/22.
 */
public class SimpleCache {
    class CacheEntry {
        long key;
        String value;
        public CacheEntry(long key, String value) {
            this.key = key;
            this.value = value;
        }
    }
    public SimpleCache(int size) {
        this.size = size;
        entries = new CacheEntry[size];
    }
    public void put(long key, String value) {
        int hashVal = ((int)(key % size));
        entries[hashVal] = new CacheEntry(key, value);
    }
    public String get(long key) {
        int hashVal = ((int)(key % size));
        CacheEntry entry = entries[hashVal];
        if (entry == null) return null;
        if (entry.key == key) {
            return entry.value;
        }
        return null;
    }
    int size;
    CacheEntry[] entries;
}

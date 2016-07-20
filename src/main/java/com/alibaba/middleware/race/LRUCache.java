package com.alibaba.middleware.race;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by hahong on 2016/7/20.
 */

public class LRUCache<K,V> extends LinkedHashMap<K, V> {
    private int capacity;

    public LRUCache(int capacity) {
        super(capacity, 0.75f, true);
        this.capacity = capacity;
    }

    protected boolean removeEldestEntry(Map.Entry<K, V> entry) {
        boolean res = this.size() > capacity;
        return res;

    }
}
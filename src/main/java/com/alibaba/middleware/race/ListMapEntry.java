package com.alibaba.middleware.race;

/**
 * Created by hahong on 2016/7/22.
 */
public class ListMapEntry<K, V> {
    K key;
    V value;
    public ListMapEntry(K key, V value) {
        this.key = key;
        this.value = value;
    }
}

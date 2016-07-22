package com.alibaba.middleware.race;

import java.util.Map;

/**
 * Created by hahong on 2016/7/22.
 */
public class ListMapEntry<K, V> implements Map.Entry<K, V> {
    K key;
    V value;
    public ListMapEntry(K key, V value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public K getKey() {
        return this.key;
    }

    @Override
    public V getValue() {
        return this.value;
    }

    @Override
    public V setValue(V value) {
        this.value = value;
        return value;
    }
}

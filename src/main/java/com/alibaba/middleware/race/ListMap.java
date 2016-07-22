package com.alibaba.middleware.race;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.*;

/**
 * Created by hahong on 2016/7/22.
 */
public class ListMap<K, V> {
    private List<ListMapEntry<K, V>> kvs = new ArrayList<>(10);

    public V get(Object key) {
        for (ListMapEntry<K, V> e : kvs) {
            if (e.key.equals(key)) {
                return e.value;
            }
        }
        return null;
    }

    public V put(K key, V value) {
        kvs.add(new ListMapEntry(key, value));
        return value;
    }

    public void putAll(ListMap<K, V> m) {
        kvs.addAll(m.kvs);
    }

    public Collection<ListMapEntry<K, V>> entrySet() {
        return kvs;
    }

}

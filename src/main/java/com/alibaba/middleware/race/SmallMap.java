package com.alibaba.middleware.race;

import java.util.Collection;
import java.util.Map;

/**
 * Created by hahong on 2016/7/22.
 */
public interface SmallMap<K, V> {
    void put(K key, V value);
    void putAll(SmallMap<K, V> t);
    V get(K key);
    Collection<Map.Entry<K, V>> entries();
}

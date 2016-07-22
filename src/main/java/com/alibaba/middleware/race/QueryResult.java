package com.alibaba.middleware.race;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by hahong on 2016/7/18.
 */
public class QueryResult implements OrderSystem.Result {
    @Override
    public OrderSystem.KeyValue get(String key) {
        return new QueryKeyValue(key, m.get(key));
    }

    @Override
    public OrderSystem.KeyValue[] getAll() {
        OrderSystem.KeyValue[] ans = new OrderSystem.KeyValue[m.size()];
        int i = 0;
        for (Map.Entry<String, String> e : m.entrySet()) {
            ans[i] = new QueryKeyValue(e.getKey(), e.getValue());
            ++i;
        }
        return ans;
    }

    @Override
    public long orderId() {
        return Long.parseLong(m.get("orderid"));
    }

    public QueryResult(HashMap<String, String> m) {
        this.m = m;
    }
    public QueryResult(ListMap<String, String> m) {
        this.m = new HashMap<>();
        for (ListMapEntry<String, String> e : m.entrySet()) {
            this.m.put(e.key, e.value);
        }
    }

    private Map<String, String> m;
}

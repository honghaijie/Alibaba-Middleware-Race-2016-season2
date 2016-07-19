package com.alibaba.middleware.race;

/**
 * Created by hahong on 2016/7/18.
 */
public class QueryKeyValue implements OrderSystem.KeyValue {
    @Override
    public String key() {
        return this.key;
    }

    @Override
    public String valueAsString() {
        return value;
    }

    @Override
    public long valueAsLong() throws OrderSystem.TypeException {
        return Long.parseLong(value);
    }

    @Override
    public double valueAsDouble() throws OrderSystem.TypeException {
        return Double.parseDouble(value);
    }

    @Override
    public boolean valueAsBoolean() throws OrderSystem.TypeException {
        return Boolean.parseBoolean(value);
    }

    public QueryKeyValue(String key, String value) {
        this.key = key;
        this.value = value;
    }
    String key;
    String value;

}

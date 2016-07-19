package com.alibaba.middleware.race;

import java.util.Comparator;
import java.util.Objects;

/**
 * Created by hahong on 2016/6/14.
 */
public class Tuple<X extends Comparable<X>, Y extends Comparable<Y>> implements Comparable<Tuple<X, Y>> {
    public final X x;
    public final Y y;
    public Tuple(X x, Y y) {
        this.x = x;
        this.y = y;
    }

    @Override
    public int compareTo(Tuple<X, Y> o) {
        int r = x.compareTo(o.x);
        if (r != 0) return r;
        return y.compareTo(o.y);
    }

    public boolean equals(Object o){
        return x.equals(((Tuple<X, Y>)o).x) && y.equals(((Tuple<X, Y>)o).y);
    }
}
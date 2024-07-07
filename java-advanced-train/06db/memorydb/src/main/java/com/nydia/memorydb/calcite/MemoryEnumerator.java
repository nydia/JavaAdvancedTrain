package com.nydia.memorydb.calcite;

import org.apache.calcite.linq4j.Enumerator;

import java.util.List;

/**
 * Author : j.fan
 * Date   : 2024/7/3
 * Desc   :
 */
public class MemoryEnumerator implements Enumerator<Object[]> {

    private List<List<Object>> source;

    private int i = -1;

    private int length;

    public MemoryEnumerator(List<List<Object>> source){
        this.source = source;
        length = source.size();
    }

    @Override
    public Object[] current() {
        List<Object> list = source.get(i);
        return list.toArray();
    }

    @Override
    public boolean moveNext() {
        if(i < length - 1){
            i++;
            return true;
        }
        return false;
    }

    @Override
    public void reset() {
        i = 0;
    }

    @Override
    public void close() {

    }
}

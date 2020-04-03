package com.data.hadoop.hdfs;

public class PairOfStringLong {

    private String key;
    private long pos;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public long getPos() {
        return pos;
    }

    public void setPos(long pos) {
        this.pos = pos;
    }

    public PairOfStringLong(String key, long pos) {
        this.key = key;
        this.pos = pos;
    }
}

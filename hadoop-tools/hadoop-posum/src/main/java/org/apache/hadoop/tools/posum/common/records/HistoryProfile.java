package org.apache.hadoop.tools.posum.common.records;

/**
 * Created by ane on 3/7/16.
 */
public class HistoryProfile<T extends GeneralProfile> extends GeneralProfile {
    private T original;
    private Class<T> tClass;
    private Long timestamp;

    public HistoryProfile(){}

    public HistoryProfile(T original) {
        this.original = original;
        this.tClass = (Class<T>) original.getClass();
        this.timestamp = System.currentTimeMillis();
    }

    public T getOriginal() {
        return original;
    }

    public void setOriginal(T original) {
        this.original = original;
    }

    public Class<T> gettClass() {
        return tClass;
    }

    public void settClass(Class<T> tClass) {
        this.tClass = tClass;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }
}

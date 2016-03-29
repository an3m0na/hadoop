package org.apache.hadoop.tools.posum.common.records.dataentity;

/**
 * Created by ane on 3/7/16.
 */
public class HistoryProfile<T extends GeneralDataEntity> extends GeneralDataEntity {
    private T original;
    private String originalId;
    private DataEntityType type;
    private Long timestamp;

    public HistoryProfile(){}

    public HistoryProfile(DataEntityType type, T original) {
        this.original = original;
        this.originalId = original.getId();
        this.type = type;
        this.timestamp = System.currentTimeMillis();
    }

    public T getOriginal() {
        return original;
    }

    public void setOriginal(T original) {
        this.original = original;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public DataEntityType getType() {
        return type;
    }

    public void setType(DataEntityType type) {
        this.type = type;
    }

    public String getOriginalId() {
        return originalId;
    }

    public void setOriginalId(String originalId) {
        this.originalId = originalId;
    }
}

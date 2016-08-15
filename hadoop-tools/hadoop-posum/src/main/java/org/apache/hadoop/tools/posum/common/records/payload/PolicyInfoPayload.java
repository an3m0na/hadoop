package org.apache.hadoop.tools.posum.common.records.payload;

import org.apache.hadoop.yarn.util.Records;

/**
 * Created by ane on 8/15/16.
 */
public abstract class PolicyInfoPayload implements Payload{

    public static PolicyInfoPayload newInstance() {
        return Records.newRecord(PolicyInfoPayload.class);
    }

    public void start(Long now) {
        if (getLastStarted() != 0) {
            setUsageTime(getUsageTime() + now - getLastStarted());
        }
        setLastStarted(now);
        setUsageNumber(getUsageNumber() + 1);
    }

    public void stop(Long now) {
        setUsageTime(getUsageTime() + now - getLastStarted());
        setLastStarted(0L);
    }

    public abstract Integer getUsageNumber();

    public abstract void setUsageNumber(Integer usageNumber);

    public abstract Long getUsageTime();

    public abstract void setUsageTime(Long usageTime);

    public abstract Long getLastStarted();

    public abstract void setLastStarted(Long lastStarted);


}

package org.apache.hadoop.tools.posum.common.records.payload;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.hadoop.yarn.util.Records;

@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class PolicyInfoPayload implements Payload {

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

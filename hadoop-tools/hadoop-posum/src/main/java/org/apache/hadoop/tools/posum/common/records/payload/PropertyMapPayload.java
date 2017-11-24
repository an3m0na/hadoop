package org.apache.hadoop.tools.posum.common.records.payload;

import org.apache.hadoop.yarn.util.Records;

import java.util.Map;

public abstract class PropertyMapPayload implements Payload {

  public static PropertyMapPayload newInstance(Map<String, Object> map) {
    PropertyMapPayload payload = Records.newRecord(PropertyMapPayload.class);
    payload.setEntries(map);
    return payload;
  }

  public abstract void addEntry(String key, Object value);

  public abstract Map<String, Object> getEntries();

  public abstract void setEntries(Map<String, Object> map);

}

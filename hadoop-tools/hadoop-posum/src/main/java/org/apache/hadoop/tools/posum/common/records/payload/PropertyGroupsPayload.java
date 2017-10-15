package org.apache.hadoop.tools.posum.common.records.payload;

import org.apache.hadoop.yarn.util.Records;

import java.util.Map;

public abstract class PropertyGroupsPayload implements Payload {

  public static PropertyGroupsPayload newInstance(Map<String, Map<String, Object>> groups) {
    PropertyGroupsPayload payload = Records.newRecord(PropertyGroupsPayload.class);
    payload.setGroups(groups);
    return payload;
  }

  public abstract void addGroup(String key, Map<String, Object> value);

  public abstract Map<String, Map<String, Object>> getGroups();

  public abstract void setGroups(Map<String, Map<String, Object>> map);

}

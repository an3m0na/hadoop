package org.apache.hadoop.tools.posum.common.records.payload;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.hadoop.yarn.util.Records;

import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class PolicyInfoMapPayload implements Payload {
  public static PolicyInfoMapPayload newInstance(Map<String, PolicyInfoPayload> entries) {
    PolicyInfoMapPayload map = Records.newRecord(PolicyInfoMapPayload.class);
    map.setEntries(entries);
    return map;
  }

  public abstract Map<String, PolicyInfoPayload> getEntries();

  public abstract void setEntries(Map<String, PolicyInfoPayload> entries);

}

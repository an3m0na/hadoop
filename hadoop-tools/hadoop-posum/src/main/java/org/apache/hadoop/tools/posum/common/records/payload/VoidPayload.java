package org.apache.hadoop.tools.posum.common.records.payload;

import org.apache.hadoop.yarn.util.Records;

public abstract class VoidPayload implements Payload {

  public static VoidPayload newInstance() {
    return Records.newRecord(VoidPayload.class);
  }
}

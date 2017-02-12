package org.apache.hadoop.tools.posum.common.records.payload;

import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.DatabaseReference;
import org.apache.hadoop.yarn.util.Records;

import java.util.List;
import java.util.Map;

public abstract class CollectionMapPayload implements Payload {

  public static CollectionMapPayload newInstance(Map<DatabaseReference, List<DataEntityCollection>> collections) {
    CollectionMapPayload payload = Records.newRecord(CollectionMapPayload.class);
    payload.setEntries(collections);
    return payload;
  }

  public abstract Map<DatabaseReference, List<DataEntityCollection>> getEntries();

  public abstract void setEntries(Map<DatabaseReference, List<DataEntityCollection>> map);
}

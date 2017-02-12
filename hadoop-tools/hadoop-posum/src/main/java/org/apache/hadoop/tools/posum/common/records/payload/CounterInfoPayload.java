package org.apache.hadoop.tools.posum.common.records.payload;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.hadoop.tools.posum.common.records.payload.impl.pb.CounterInfoPayloadPBImpl;
import org.apache.hadoop.yarn.util.Records;

@JsonDeserialize(as = CounterInfoPayloadPBImpl.class)
@org.codehaus.jackson.map.annotate.JsonDeserialize(as = CounterInfoPayloadPBImpl.class)
public abstract class CounterInfoPayload implements Payload {

  public static CounterInfoPayload newInstance(String name, long total, long map, long reduce) {
    CounterInfoPayload payload = Records.newRecord(CounterInfoPayload.class);
    payload.setName(name);
    payload.setTotalCounterValue(total);
    payload.setMapCounterValue(map);
    payload.setReduceCounterValue(reduce);
    return payload;
  }

  public static CounterInfoPayload newInstance(String name, long value) {
    CounterInfoPayload payload = Records.newRecord(CounterInfoPayload.class);
    payload.setName(name);
    payload.setTotalCounterValue(value);
    return payload;
  }

  public abstract long getReduceCounterValue();

  public abstract void setReduceCounterValue(long reduce);

  public abstract long getMapCounterValue();

  public abstract void setMapCounterValue(long map);

  public abstract long getTotalCounterValue();

  public abstract void setTotalCounterValue(long total);

  public abstract String getName();

  public abstract void setName(String name);

  public abstract void setValue(long total);

}

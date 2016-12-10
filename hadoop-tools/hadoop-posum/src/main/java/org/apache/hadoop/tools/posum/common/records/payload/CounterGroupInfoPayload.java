package org.apache.hadoop.tools.posum.common.records.payload;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.hadoop.tools.posum.common.records.payload.impl.pb.CounterGroupInfoPayloadPBImpl;
import org.apache.hadoop.yarn.util.Records;

import java.util.List;

@JsonDeserialize(as = CounterGroupInfoPayloadPBImpl.class)
@org.codehaus.jackson.map.annotate.JsonDeserialize(as = CounterGroupInfoPayloadPBImpl.class)
public abstract class CounterGroupInfoPayload implements Payload {

    public static CounterGroupInfoPayload newInstance(String name, List<CounterInfoPayload> counter) {
        CounterGroupInfoPayload payload = Records.newRecord(CounterGroupInfoPayload.class);
        payload.setCounterGroupName(name);
        payload.setCounter(counter);
        return payload;
    }

    public abstract List<CounterInfoPayload> getCounter();

    public abstract void setCounter(List<CounterInfoPayload> counter);

    public abstract String getCounterGroupName();

    public abstract void setCounterGroupName(String name);

}

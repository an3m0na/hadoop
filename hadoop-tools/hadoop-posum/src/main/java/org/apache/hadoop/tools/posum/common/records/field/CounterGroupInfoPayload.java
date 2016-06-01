package org.apache.hadoop.tools.posum.common.records.field;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.hadoop.tools.posum.common.records.field.impl.pb.CounterGroupInfoPayloadPBImpl;
import org.apache.hadoop.yarn.util.Records;

import java.util.List;

/**
 * Created by ane on 3/20/16.
 */
@JsonDeserialize(as=CounterGroupInfoPayloadPBImpl.class)
@org.codehaus.jackson.map.annotate.JsonDeserialize(as=CounterGroupInfoPayloadPBImpl.class)
public abstract class CounterGroupInfoPayload {

    public static CounterGroupInfoPayload newInstance(String name, List<CounterInfoPayload> counter) {
        CounterGroupInfoPayload ret = Records.newRecord(CounterGroupInfoPayload.class);
        ret.setCounterGroupName(name);
        ret.setCounter(counter);
        return ret;
    }

    public abstract List<CounterInfoPayload> getCounter();

    public abstract void setCounter(List<CounterInfoPayload> counter);

    public abstract String getCounterGroupName();

    public abstract void setCounterGroupName(String name);

}

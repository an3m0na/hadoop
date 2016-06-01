package org.apache.hadoop.tools.posum.common.records.field;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.hadoop.tools.posum.common.records.field.impl.pb.CounterInfoPayloadPBImpl;
import org.apache.hadoop.yarn.util.Records;

/**
 * Created by ane on 3/20/16.
 */
@JsonDeserialize(as=CounterInfoPayloadPBImpl.class)
@org.codehaus.jackson.map.annotate.JsonDeserialize(as=CounterInfoPayloadPBImpl.class)
public abstract class CounterInfoPayload {

    public static CounterInfoPayload newInstance(String name, long total, long map, long reduce) {
        CounterInfoPayload ret = Records.newRecord(CounterInfoPayload.class);
        ret.setName(name);
        ret.setTotalCounterValue(total);
        ret.setMapCounterValue(map);
        ret.setReduceCounterValue(reduce);
        return ret;
    }

    public abstract long getReduceCounterValue();

    public abstract void setReduceCounterValue(long reduce);

    public abstract long getMapCounterValue();

    public abstract void setMapCounterValue(long map);

    public abstract long getTotalCounterValue();

    public abstract void setTotalCounterValue(long total);

    public abstract String getName();

    public abstract void setName(String name);

}

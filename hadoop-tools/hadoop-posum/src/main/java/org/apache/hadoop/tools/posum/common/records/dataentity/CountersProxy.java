package org.apache.hadoop.tools.posum.common.records.dataentity;

import org.apache.hadoop.tools.posum.common.records.field.CounterGroupInfoPayload;

import java.util.List;

/**
 * Created by ane on 2/8/16.
 */
public interface CountersProxy extends GeneralDataEntity {

    List<CounterGroupInfoPayload> getCounterGroup();

    void setCounterGroup(List<CounterGroupInfoPayload> counterGroups);

    void setTaskCounterGroup(List<CounterGroupInfoPayload> counterGroups);

}

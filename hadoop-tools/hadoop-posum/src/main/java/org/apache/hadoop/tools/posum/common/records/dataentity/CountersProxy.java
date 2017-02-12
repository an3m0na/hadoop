package org.apache.hadoop.tools.posum.common.records.dataentity;

import org.apache.hadoop.tools.posum.common.records.payload.CounterGroupInfoPayload;

import java.util.List;

public interface CountersProxy extends GeneralDataEntity<CountersProxy> {

  List<CounterGroupInfoPayload> getCounterGroup();

  void setCounterGroup(List<CounterGroupInfoPayload> counterGroups);

  void setTaskCounterGroup(List<CounterGroupInfoPayload> counterGroups);

}

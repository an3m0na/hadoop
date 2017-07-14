package org.apache.hadoop.tools.posum.common.records.request;

import org.apache.hadoop.tools.posum.common.records.payload.SimulationResultPayload;
import org.apache.hadoop.yarn.util.Records;

import java.util.List;

public abstract class HandleSimResultRequest {

  public static HandleSimResultRequest newInstance() {
    return Records.newRecord(HandleSimResultRequest.class);
  }

  public static HandleSimResultRequest newInstance(List<SimulationResultPayload> results) {
    HandleSimResultRequest request = newInstance();
    request.setResults(results);
    return request;
  }

  public abstract void setResults(List<SimulationResultPayload> results);

  public abstract List<SimulationResultPayload> getResults();

  public abstract void addResult(SimulationResultPayload result);
}

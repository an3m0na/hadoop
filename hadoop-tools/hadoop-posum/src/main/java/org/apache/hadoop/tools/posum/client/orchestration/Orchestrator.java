package org.apache.hadoop.tools.posum.client.orchestration;

import org.apache.hadoop.tools.posum.common.records.request.HandleSimResultRequest;
import org.apache.hadoop.tools.posum.common.util.communication.CommUtils;

public interface Orchestrator {
  void handleSimulationResult(HandleSimResultRequest resultRequest);

  String register(CommUtils.PosumProcess process, String address);    // returns DM address

}

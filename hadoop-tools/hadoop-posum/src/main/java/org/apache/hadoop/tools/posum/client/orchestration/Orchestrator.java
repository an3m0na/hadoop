package org.apache.hadoop.tools.posum.client.orchestration;

import org.apache.hadoop.tools.posum.common.records.request.HandleSimResultRequest;
import org.apache.hadoop.tools.posum.common.util.Utils;

public interface Orchestrator {
    void handleSimulationResult(HandleSimResultRequest resultRequest);

    String register(Utils.PosumProcess process, String address);    // returns DM address

}

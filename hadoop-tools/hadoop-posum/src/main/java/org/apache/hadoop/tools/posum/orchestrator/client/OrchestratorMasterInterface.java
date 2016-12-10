package org.apache.hadoop.tools.posum.orchestrator.client;

import org.apache.hadoop.tools.posum.common.records.request.HandleSimResultRequest;
import org.apache.hadoop.tools.posum.common.util.Utils;

public interface OrchestratorMasterInterface {
    void handleSimulationResult(HandleSimResultRequest resultRequest);

    String register(Utils.PosumProcess process, String address);    // returns DM address

}

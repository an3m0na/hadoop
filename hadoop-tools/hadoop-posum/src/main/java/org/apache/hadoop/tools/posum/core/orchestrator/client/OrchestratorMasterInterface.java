package org.apache.hadoop.tools.posum.core.orchestrator.client;

import org.apache.hadoop.tools.posum.common.records.request.HandleSimResultRequest;
import org.apache.hadoop.tools.posum.common.util.Utils;

/**
 * Created by ane on 4/15/16.
 */
public interface OrchestratorMasterInterface {
    void handleSimulationResult(HandleSimResultRequest resultRequest);

    String register(Utils.PosumProcess process, String address);    // returns DM address

}

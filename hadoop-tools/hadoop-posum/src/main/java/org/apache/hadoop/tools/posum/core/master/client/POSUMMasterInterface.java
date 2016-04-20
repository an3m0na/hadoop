package org.apache.hadoop.tools.posum.core.master.client;

import org.apache.hadoop.tools.posum.common.records.request.HandleSimResultRequest;
import org.apache.hadoop.tools.posum.common.util.Utils;

/**
 * Created by ane on 4/15/16.
 */
public interface POSUMMasterInterface {
    void handleSimulationResult(HandleSimResultRequest resultRequest);

    String register(Utils.POSUMProcess process, String address);    // returns DM address

}

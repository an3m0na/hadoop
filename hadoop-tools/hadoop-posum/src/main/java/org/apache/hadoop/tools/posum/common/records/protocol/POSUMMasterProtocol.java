package org.apache.hadoop.tools.posum.common.records.protocol;

import org.apache.hadoop.tools.posum.common.records.request.HandleSimResultRequest;
import org.apache.hadoop.tools.posum.common.records.response.SimpleResponse;
import org.apache.hadoop.tools.posum.common.records.request.SimpleRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;

/**
 * Created by ane on 3/19/16.
 */
public interface POSUMMasterProtocol{
    long versionID = 1L;

    SimpleResponse handleSimpleRequest(SimpleRequest request) throws IOException, YarnException;
    SimpleResponse handleSimulationResult(HandleSimResultRequest resultRequest) throws IOException, YarnException;

}

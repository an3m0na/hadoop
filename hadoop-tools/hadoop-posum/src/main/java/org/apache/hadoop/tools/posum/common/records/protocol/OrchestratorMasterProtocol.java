package org.apache.hadoop.tools.posum.common.records.protocol;

import org.apache.hadoop.tools.posum.common.records.request.HandleSimResultRequest;
import org.apache.hadoop.tools.posum.common.records.request.RegistrationRequest;
import org.apache.hadoop.tools.posum.common.records.response.SimpleResponse;
import org.apache.hadoop.tools.posum.common.records.request.SimpleRequest;
import org.apache.hadoop.tools.posum.common.util.StandardProtocol;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;

public interface OrchestratorMasterProtocol extends StandardProtocol{
   SimpleResponse handleSimulationResult(HandleSimResultRequest resultRequest) throws IOException, YarnException;
   SimpleResponse registerProcess(RegistrationRequest request) throws IOException, YarnException;

}

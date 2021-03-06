package org.apache.hadoop.tools.posum.client.orchestration;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.tools.posum.common.records.payload.StringStringMapPayload;
import org.apache.hadoop.tools.posum.common.records.protocol.OrchestratorMasterProtocol;
import org.apache.hadoop.tools.posum.common.records.request.HandleSimResultRequest;
import org.apache.hadoop.tools.posum.common.records.request.RegistrationRequest;
import org.apache.hadoop.tools.posum.common.records.request.SimpleRequest;
import org.apache.hadoop.tools.posum.common.records.response.SimpleResponse;
import org.apache.hadoop.tools.posum.common.util.communication.CommUtils;
import org.apache.hadoop.tools.posum.common.util.conf.PosumConfiguration;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.tools.posum.common.util.communication.StandardClientProxyFactory;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class OrchestrationMasterClient extends AbstractService implements Orchestrator {

  private static Log logger = LogFactory.getLog(OrchestrationMasterClient.class);

  public OrchestrationMasterClient() {
    super(OrchestrationMasterClient.class.getName());
  }

  private OrchestratorMasterProtocol pmClient;

  @Override
  protected void serviceStart() throws Exception {
    final Configuration conf = getConfig();
    try {
      pmClient = new StandardClientProxyFactory<>(conf,
        conf.get(PosumConfiguration.PM_ADDRESS),
        PosumConfiguration.PM_ADDRESS_DEFAULT,
        PosumConfiguration.PM_PORT_DEFAULT,
        OrchestratorMasterProtocol.class).createProxy();
      checkPing();
    } catch (IOException e) {
      throw new PosumException("Could not init OrchestrationMaster client", e);
    }
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    if (this.pmClient != null) {
      RPC.stopProxy(this.pmClient);
    }
    super.serviceStop();
  }

  public SimpleResponse sendSimpleRequest(SimpleRequest.Type type) {
    return sendSimpleRequest(type.name(), SimpleRequest.newInstance(type));
  }

  private SimpleResponse sendSimpleRequest(String kind, SimpleRequest request) {
    try {
      return CommUtils.handleError(kind, pmClient.handleSimpleRequest(request));
    } catch (IOException | YarnException e) {
      throw new PosumException("Error during RPC call", e);
    }
  }

  private void checkPing() {
    sendSimpleRequest("checkPing", SimpleRequest.newInstance(SimpleRequest.Type.PING, "Hello world!"));
    logger.info("Successfully connected to OrchestrationMaster");
  }

  @Override
  public String register(CommUtils.PosumProcess process, String address) {
    try {
      return CommUtils.handleError("register",
        pmClient.registerProcess(RegistrationRequest.newInstance(process, address))).getText();
    } catch (IOException | YarnException e) {
      throw new PosumException("Error during RPC call", e);
    }
  }

  @Override
  public void handleSimulationResult(HandleSimResultRequest request) {
    try {
      CommUtils.handleError("handleSimulationResult", pmClient.handleSimulationResult(request));
    } catch (IOException | YarnException e) {
      throw new PosumException("Error during RPC call", e);
    }
  }

  public Map<CommUtils.PosumProcess, String> getSystemAddresses() {
    SimpleResponse response =
      sendSimpleRequest("getSystemAddresses", SimpleRequest.newInstance(SimpleRequest.Type.SYSTEM_ADDRESSES));
    StringStringMapPayload payload = (StringStringMapPayload) response.getPayload();
    if (payload != null) {
      Map<CommUtils.PosumProcess, String> ret = new HashMap<>();
      for (Map.Entry<String, String> entry : payload.getEntries().entrySet()) {
        ret.put(CommUtils.PosumProcess.valueOf(entry.getKey()), entry.getValue());
      }
      return ret;
    }
    return null;
  }
}

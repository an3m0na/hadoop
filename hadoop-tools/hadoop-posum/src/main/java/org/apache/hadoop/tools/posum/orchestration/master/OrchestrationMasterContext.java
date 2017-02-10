package org.apache.hadoop.tools.posum.orchestration.master;

import org.apache.hadoop.tools.posum.common.util.DummyTokenSecretManager;
import org.apache.hadoop.yarn.event.Dispatcher;

public class OrchestrationMasterContext {
  private Dispatcher dispatcher;
  private DummyTokenSecretManager tokenSecretManager;
  private OrchestrationCommService commService;

  public void setDispatcher(Dispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  public Dispatcher getDispatcher() {
    return dispatcher;
  }

  public void setTokenSecretManager(DummyTokenSecretManager tokenSecretManager) {
    this.tokenSecretManager = tokenSecretManager;
  }

  public DummyTokenSecretManager getTokenSecretManager() {
    return tokenSecretManager;
  }

  public void setCommService(OrchestrationCommService commService) {
    this.commService = commService;
  }

  public OrchestrationCommService getCommService() {
    return commService;
  }

}
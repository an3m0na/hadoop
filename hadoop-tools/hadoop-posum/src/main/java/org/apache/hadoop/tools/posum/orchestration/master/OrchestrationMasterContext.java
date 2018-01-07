package org.apache.hadoop.tools.posum.orchestration.master;

import org.apache.hadoop.tools.posum.common.util.communication.DummyTokenSecretManager;
import org.apache.hadoop.tools.posum.common.util.conf.PolicyPortfolio;
import org.apache.hadoop.tools.posum.orchestration.core.SimulationScoreComparator;
import org.apache.hadoop.yarn.event.Dispatcher;

public class OrchestrationMasterContext {
  private Dispatcher dispatcher;
  private DummyTokenSecretManager tokenSecretManager;
  private OrchestrationCommService commService;
  private SimulationScoreComparator simulationScoreComparator;
  private PolicyPortfolio policyPortfolio;
  private boolean switchEnabled;
  private String currentPolicy;

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

  public void setSimulationScoreComparator(SimulationScoreComparator simulationScoreComparator) {
    this.simulationScoreComparator = simulationScoreComparator;
  }

  public SimulationScoreComparator getSimulationScoreComparator() {
    return simulationScoreComparator;
  }

  public void setPolicyPortfolio(PolicyPortfolio policyPortfolio) {
    this.policyPortfolio = policyPortfolio;
  }

  public PolicyPortfolio getPolicyPortfolio() {
    return policyPortfolio;
  }

  public void setSwitchEnabled(boolean switchEnabled) {
    this.switchEnabled = switchEnabled;
  }

  public boolean isSwitchEnabled() {
    return switchEnabled;
  }

  public void setCurrentPolicy(String currentPolicy) {
    this.currentPolicy = currentPolicy;
  }

  public String getCurrentPolicy() {
    return currentPolicy;
  }
}
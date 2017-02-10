package org.apache.hadoop.tools.posum.simulation.master;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.client.data.DataStore;
import org.apache.hadoop.tools.posum.client.simulation.Simulator;
import org.apache.hadoop.tools.posum.common.util.DummyTokenSecretManager;

public class SimulationMasterContext {
  private DummyTokenSecretManager tokenSecretManager;
  private SimulationMasterCommService commService;
  private Configuration conf;
  private Simulator simulator;


  public Simulator getSimulator() {
    return simulator;
  }

  public void setSimulator(Simulator simulator) {
    this.simulator = simulator;
  }

  public void setTokenSecretManager(DummyTokenSecretManager tokenSecretManager) {
    this.tokenSecretManager = tokenSecretManager;
  }

  public DummyTokenSecretManager getTokenSecretManager() {
    return tokenSecretManager;
  }

  public void setCommService(SimulationMasterCommService commService) {
    this.commService = commService;
  }

  public SimulationMasterCommService getCommService() {
    return commService;
  }

  public DataStore getDataBroker() {
    return commService.getDataMaster();
  }

  public Configuration getConf() {
    return conf;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }
}

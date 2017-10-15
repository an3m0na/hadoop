package org.apache.hadoop.tools.posum.simulation.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.client.data.DataMasterClient;
import org.apache.hadoop.tools.posum.client.data.DataStore;
import org.apache.hadoop.tools.posum.client.simulation.Simulator;
import org.apache.hadoop.tools.posum.common.util.communication.DummyTokenSecretManager;

public class SimulationMasterContext {
  private static Log logger = LogFactory.getLog(SimulationMasterContext.class);

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
    DataMasterClient dm = commService.getDataMaster();
    while (dm == null) {
      logger.info("Simulations are waiting for Data Master to be available");
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        logger.error("Awaiting Data Master was interrupted", e);
      }
    }
    return dm;
  }

  public Configuration getConf() {
    return conf;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

}

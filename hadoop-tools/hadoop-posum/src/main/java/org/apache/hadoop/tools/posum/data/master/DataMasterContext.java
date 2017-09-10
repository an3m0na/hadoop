package org.apache.hadoop.tools.posum.data.master;

import org.apache.hadoop.tools.posum.client.data.DataStore;
import org.apache.hadoop.tools.posum.common.util.DummyTokenSecretManager;
import org.apache.hadoop.tools.posum.data.monitor.PosumInfoCollector;
import org.apache.hadoop.tools.posum.data.monitor.cluster.AppInfoCollector;

public class DataMasterContext {
  private DummyTokenSecretManager tokenSecretManager;
  private DataStore dataStore;
  private DataMasterCommService commService;
  private AppInfoCollector clusterInfoCollector;
  private PosumInfoCollector posumInfoCollector;

  public void setTokenSecretManager(DummyTokenSecretManager tokenSecretManager) {
    this.tokenSecretManager = tokenSecretManager;
  }

  public DummyTokenSecretManager getTokenSecretManager() {
    return tokenSecretManager;
  }

  public void setDataStore(DataStore dataStore) {
    this.dataStore = dataStore;
  }

  public DataStore getDataStore() {
    return dataStore;
  }

  public void setCommService(DataMasterCommService commService) {
    this.commService = commService;
  }

  public DataMasterCommService getCommService() {
    return commService;
  }

  public AppInfoCollector getClusterInfoCollector() {
    return clusterInfoCollector;
  }

  public void setClusterInfoCollector(AppInfoCollector clusterInfoCollector) {
    this.clusterInfoCollector = clusterInfoCollector;
  }

  public PosumInfoCollector getPosumInfoCollector() {
    return posumInfoCollector;
  }

  public void setPosumInfoCollector(PosumInfoCollector posumInfoCollector) {
    this.posumInfoCollector = posumInfoCollector;
  }
}

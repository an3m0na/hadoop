package org.apache.hadoop.tools.posum.data.master;

import org.apache.hadoop.tools.posum.common.util.DummyTokenSecretManager;
import org.apache.hadoop.tools.posum.client.data.DataStore;
import org.apache.hadoop.tools.posum.data.monitor.ClusterInfoCollector;
import org.apache.hadoop.tools.posum.data.monitor.PosumInfoCollector;

public class DataMasterContext {
    private DummyTokenSecretManager tokenSecretManager;
    private DataStore dataStore;
    private DataMasterCommService commService;
    private ClusterInfoCollector clusterInfo;
    private PosumInfoCollector posumInfo;

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

    public ClusterInfoCollector getClusterInfo() {
        return clusterInfo;
    }

    public void setClusterInfo(ClusterInfoCollector clusterInfo) {
        this.clusterInfo = clusterInfo;
    }

    public PosumInfoCollector getPosumInfo() {
        return posumInfo;
    }

    public void setPosumInfo(PosumInfoCollector posumInfo) {
        this.posumInfo = posumInfo;
    }
}

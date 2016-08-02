package org.apache.hadoop.tools.posum.database.master;

import org.apache.hadoop.tools.posum.common.util.DummyTokenSecretManager;
import org.apache.hadoop.tools.posum.database.monitor.ClusterInfoCollector;
import org.apache.hadoop.tools.posum.database.monitor.PosumInfoCollector;
import org.apache.hadoop.tools.posum.database.store.DataStore;
import org.apache.hadoop.yarn.event.Dispatcher;

/**
 * Created by ane on 3/19/16.
 */
public class DataMasterContext {
    private Dispatcher dispatcher;
    private DummyTokenSecretManager tokenSecretManager;
    private DataStore dataStore;
    private DataCommService commService;
    private ClusterInfoCollector clusterInfo;
    private PosumInfoCollector posumInfo;

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

    public void setDataStore(DataStore dataStore) {
        this.dataStore = dataStore;
    }

    public DataStore getDataStore() {
        return dataStore;
    }

    public void setCommService(DataCommService commService) {
        this.commService = commService;
    }

    public DataCommService getCommService() {
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

package org.apache.hadoop.tools.posum.database.master;

import org.apache.hadoop.tools.posum.common.util.DummyTokenSecretManager;
import org.apache.hadoop.tools.posum.database.client.DataBroker;
import org.apache.hadoop.tools.posum.database.monitor.ClusterInfoCollector;
import org.apache.hadoop.tools.posum.database.monitor.PosumInfoCollector;
import org.apache.hadoop.yarn.event.Dispatcher;

public class DataMasterContext {
    private Dispatcher dispatcher;
    private DummyTokenSecretManager tokenSecretManager;
    private DataBroker dataBroker;
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

    public void setDataBroker(DataBroker dataBroker) {
        this.dataBroker = dataBroker;
    }

    public DataBroker getDataBroker() {
        return dataBroker;
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

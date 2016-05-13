package org.apache.hadoop.tools.posum.database.master;

import org.apache.hadoop.tools.posum.common.util.DummyTokenSecretManager;
import org.apache.hadoop.tools.posum.database.client.DataStoreInterface;
import org.apache.hadoop.yarn.event.Dispatcher;

/**
 * Created by ane on 3/19/16.
 */
public class DataMasterContext {
    private Dispatcher dispatcher;
    private DummyTokenSecretManager tokenSecretManager;
    private DataStoreInterface dataStoreInterface;
    private String hostAddress;

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

    public void setDataStoreInterface(DataStoreInterface dataStoreInterface) {
        this.dataStoreInterface = dataStoreInterface;
    }

    public DataStoreInterface getDataStore() {
        return dataStoreInterface;
    }

    public void setHostAddress(String hostAddress) {
        this.hostAddress = hostAddress;
    }

    public String getHostAddress() {
        return hostAddress;
    }
}

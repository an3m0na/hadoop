package org.apache.hadoop.tools.posum.core.master;

import org.apache.hadoop.tools.posum.common.util.DummyTokenSecretManager;
import org.apache.hadoop.yarn.event.Dispatcher;

/**
 * Created by ane on 3/19/16.
 */
public class POSUMMasterContext {
    private Dispatcher dispatcher;
    private DummyTokenSecretManager tokenSecretManager;

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

}

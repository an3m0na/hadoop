package org.apache.hadoop.tools.posum.simulator.master;

import org.apache.hadoop.tools.posum.common.util.DummyTokenSecretManager;
import org.apache.hadoop.tools.posum.simulator.master.client.SimulatorInterface;

/**
 * Created by ane on 3/19/16.
 */
public class SimulationMasterContext {
    private DummyTokenSecretManager tokenSecretManager;
    private SimulatorCommService commService;

    public SimulatorInterface getSimulator() {
        return simulator;
    }

    public void setSimulator(SimulatorInterface simulator) {
        this.simulator = simulator;
    }

    private SimulatorInterface simulator;

    public void setTokenSecretManager(DummyTokenSecretManager tokenSecretManager) {
        this.tokenSecretManager = tokenSecretManager;
    }

    public DummyTokenSecretManager getTokenSecretManager() {
        return tokenSecretManager;
    }

    public void setCommService(SimulatorCommService commService) {
        this.commService = commService;
    }

    public SimulatorCommService getCommService(){
        return commService;
    }

}

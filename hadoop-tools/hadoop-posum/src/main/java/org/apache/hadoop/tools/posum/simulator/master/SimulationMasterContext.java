package org.apache.hadoop.tools.posum.simulator.master;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.common.util.DummyTokenSecretManager;
import org.apache.hadoop.tools.posum.database.client.DataBroker;
import org.apache.hadoop.tools.posum.simulator.master.client.SimulatorInterface;

/**
 * Created by ane on 3/19/16.
 */
public class SimulationMasterContext {
    private DummyTokenSecretManager tokenSecretManager;
    private SimulationMasterCommService commService;
    private Configuration conf;
    private SimulatorInterface simulator;


    public SimulatorInterface getSimulator() {
        return simulator;
    }

    public void setSimulator(SimulatorInterface simulator) {
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

    public SimulationMasterCommService getCommService(){
        return commService;
    }

    public DataBroker getDataBroker(){
        return commService.getDataMaster();
    }

    public Configuration getConf(){
        return conf;
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
    }
}

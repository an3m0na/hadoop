package org.apache.hadoop.tools.posum.simulator.master;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;

/**
 * Created by ane on 2/4/16.
 */
public class SimulationMaster extends CompositeService {

    private SimulatorImpl simulator;

    public SimulationMaster() {
        super(SimulationMaster.class.getName());
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        simulator = new SimulatorImpl();
        simulator.init(conf);
        addIfService(simulator);
    }

    public static void main(String[] args) {
        Configuration conf = POSUMConfiguration.newInstance();
        SimulationMaster master = new SimulationMaster();
        master.init(conf);
        master.start();
    }

}

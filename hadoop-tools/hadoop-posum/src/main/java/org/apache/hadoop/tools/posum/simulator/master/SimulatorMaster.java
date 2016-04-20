package org.apache.hadoop.tools.posum.simulator.master;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.tools.posum.common.util.POSUMException;
import org.apache.hadoop.tools.posum.database.client.DataStoreInterface;
import org.apache.hadoop.tools.posum.database.client.DataMasterClient;
import org.apache.hadoop.tools.posum.simulator.predictor.BasicPredictor;
import org.apache.hadoop.tools.posum.simulator.predictor.JobBehaviorPredictor;

/**
 * Created by ane on 2/4/16.
 */
public class SimulatorMaster extends CompositeService {

    SimulatorImpl simulator;

    public SimulatorMaster() {
        super(SimulatorMaster.class.getName());
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        simulator = new SimulatorImpl();
        simulator.init(conf);
        addIfService(simulator);
    }

    @Override
    protected void serviceStart() throws Exception {
        super.serviceStart();
    }

    public static void main(String[] args) {
        Configuration conf = POSUMConfiguration.newInstance();
        SimulatorMaster master = new SimulatorMaster();
        master.init(conf);
        master.start();
    }

}

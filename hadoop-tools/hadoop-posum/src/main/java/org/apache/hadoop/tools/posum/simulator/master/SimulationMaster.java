package org.apache.hadoop.tools.posum.simulator.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;

/**
 * Created by ane on 2/4/16.
 */
public class SimulationMaster extends CompositeService {

    private static final Log logger =LogFactory.getLog(SimulationMaster.class);

    private SimulatorImpl simulator;
    private SimulationMasterContext smContext;
    private SimulatorCommService commService;


    public SimulationMaster() {
        super(SimulationMaster.class.getName());
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        smContext = new SimulationMasterContext();

        simulator = new SimulatorImpl(smContext);
        simulator.init(conf);
        addIfService(simulator);
        smContext.setSimulator(simulator);

        commService = new SimulatorCommService(smContext);
        commService.init(conf);
        addIfService(commService);
        smContext.setCommService(commService);


    }

    public static void main(String[] args) {
        try {
            Configuration conf = POSUMConfiguration.newInstance();
            SimulationMaster master = new SimulationMaster();
            master.init(conf);
            master.start();
        }catch (Exception e){
            logger.fatal("Could not start Simulation Master", e);
        }
    }

}

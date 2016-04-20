package org.apache.hadoop.tools.posum.core.master;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;

/**
 * Created by ane on 2/4/16.
 */
public class POSUMMaster extends CompositeService {

    public POSUMMaster() {
        super(POSUMMaster.class.getName());
    }

    private POSUMMasterContext pmContext;
    private POSUMMasterService pmService;
    private SimulationMonitor simulator;

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        pmContext = new POSUMMasterContext();

        //service to allow other processes to communicate with the master
        pmService = new POSUMMasterService(pmContext);
        pmService.init(conf);
        addIfService(pmService);

        //service that starts simulations and gathers information from them
        simulator = new SimulationMonitor(pmContext);
        simulator.init(conf);
        addIfService(simulator);

        super.serviceInit(conf);
    }

    @Override
    protected void serviceStop() throws Exception {
        if (pmService != null)
            pmService.stop();
        if (simulator != null)
            simulator.stop();
        super.serviceStop();
    }

    public static void main(String[] args) {
        Configuration conf = POSUMConfiguration.newInstance();
        POSUMMaster master = new POSUMMaster();
        master.init(conf);
        master.start();
    }
}

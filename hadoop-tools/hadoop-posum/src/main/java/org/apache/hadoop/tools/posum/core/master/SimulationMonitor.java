package org.apache.hadoop.tools.posum.core.master;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.common.util.GeneralLooper;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;

/**
 * Created by ane on 4/18/16.
 */
public class SimulationMonitor extends GeneralLooper<SimulationMonitor> {
    private final POSUMMasterContext context;

    public SimulationMonitor(POSUMMasterContext context) {
        super(SimulationMonitor.class);
        this.context = context;
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        super.serviceInit(conf);
        setSleepInterval(conf.getLong(POSUMConfiguration.SIMULATION_INTERVAL,
                POSUMConfiguration.SIMULATION_INTERVAL_DEFAULT));

    }

    @Override
    protected void doAction() {

    }
}

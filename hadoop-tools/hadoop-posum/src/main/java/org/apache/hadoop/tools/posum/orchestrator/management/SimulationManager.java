package org.apache.hadoop.tools.posum.orchestrator.management;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.common.util.GeneralLooper;
import org.apache.hadoop.tools.posum.common.util.PosumConfiguration;
import org.apache.hadoop.tools.posum.orchestrator.master.OrchestratorMasterContext;

public class SimulationManager extends GeneralLooper<SimulationManager> {
    private static Log logger = LogFactory.getLog(SimulationManager.class);

    private final OrchestratorMasterContext context;
    private volatile boolean simulationRunning = false;
    private final Object lock = new Object();

    public SimulationManager(OrchestratorMasterContext context) {
        super(SimulationManager.class);
        this.context = context;
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        super.serviceInit(conf);
        setSleepInterval(conf.getLong(PosumConfiguration.SIMULATION_INTERVAL,
                PosumConfiguration.SIMULATION_INTERVAL_DEFAULT));

    }

    @Override
    protected void doAction() {
        synchronized (lock) {
            while (simulationRunning)
                try {
                    lock.wait();
                } catch (InterruptedException e) {
                    logger.warn(e);
                }
            //TODO check if simulation is actually needed
            logger.trace("Should start simulation");
            simulationRunning = true;
            context.getDispatcher().getEventHandler().handle(new PosumEvent(PosumEventType.SIMULATION_START));
        }

    }

    void simulationFinished() {
        synchronized (lock) {
            simulationRunning = false;
            lock.notifyAll();
        }
    }
}

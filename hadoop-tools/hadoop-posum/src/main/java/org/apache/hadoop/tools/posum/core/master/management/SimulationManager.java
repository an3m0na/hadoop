package org.apache.hadoop.tools.posum.core.master.management;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.common.util.GeneralLooper;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.tools.posum.common.util.POSUMException;
import org.apache.hadoop.tools.posum.core.master.POSUMMasterContext;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by ane on 4/18/16.
 */
public class SimulationManager extends GeneralLooper<SimulationManager> {
    private static Log logger = LogFactory.getLog(Orchestrator.class);

    private final POSUMMasterContext context;
    private Lock lock = new ReentrantLock();

    public SimulationManager(POSUMMasterContext context) {
        super(SimulationManager.class);
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
        lock.lock();
        //TODO check if simulation is actually needed
        logger.debug("Should start simulation");
        context.getDispatcher().getEventHandler().handle(new POSUMEvent(POSUMEventType.SIMULATION_START));
    }

    void simulationFinished() {
        lock.unlock();
    }
}

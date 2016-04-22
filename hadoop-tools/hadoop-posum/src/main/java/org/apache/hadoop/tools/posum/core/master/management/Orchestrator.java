package org.apache.hadoop.tools.posum.core.master.management;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.tools.posum.common.records.field.SimulationResult;
import org.apache.hadoop.tools.posum.common.util.POSUMException;
import org.apache.hadoop.tools.posum.core.master.POSUMMasterContext;
import org.apache.hadoop.tools.posum.core.scheduler.meta.client.MetaSchedulerInterface;
import org.apache.hadoop.yarn.event.EventHandler;

import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Created by ane on 4/20/16.
 */
public class Orchestrator extends CompositeService implements EventHandler<POSUMEvent> {

    private static Log logger = LogFactory.getLog(Orchestrator.class);

    private POSUMMasterContext pmContext;
    private SimulationManager simulationManager;

    public Orchestrator(POSUMMasterContext pmContext) {
        super(Orchestrator.class.getName());
        this.pmContext = pmContext;
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        simulationManager = new SimulationManager(pmContext);
        simulationManager.init(conf);

        super.serviceInit(conf);
    }

    @Override
    public void handle(POSUMEvent event) {
        switch (event.getType()) {
            case SIMULATOR_CONNECTED:
                if (!simulationManager.isInState(STATE.STARTED)) {
                    addIfService(simulationManager);
                    simulationManager.start();
                    logger.debug("Simulator connected");
                }
                break;
            case SIMULATION_START:
                logger.debug("Starting simulation");
                pmContext.getCommService().getSimulator().startSimulation();
                break;
            case SIMULATION_FINISH:
                simulationManager.simulationFinished();
                ConcurrentSkipListSet<SimulationResult> results = event.getCastContent();
                logger.debug("Policy scores: " + results);
                SimulationResult bestResult = results.last();
                if (bestResult != null) {
                    logger.info("Switching to best policy: " + bestResult.getPolicyName());
                    MetaSchedulerInterface scheduler = pmContext.getCommService().getScheduler();
                    if (scheduler != null)
                        scheduler.changeToPolicy(bestResult.getPolicyName());
                }
                break;
            default:
                throw new POSUMException("Could not handle event of type " + event.getType());
        }
    }
}

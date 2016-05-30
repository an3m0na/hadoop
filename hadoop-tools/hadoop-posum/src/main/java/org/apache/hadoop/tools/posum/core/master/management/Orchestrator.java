package org.apache.hadoop.tools.posum.core.master.management;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.tools.posum.common.records.field.SimulationResult;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
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
    private boolean switchEnabled;

    public Orchestrator(POSUMMasterContext pmContext) {
        super(Orchestrator.class.getName());
        this.pmContext = pmContext;
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        simulationManager = new SimulationManager(pmContext);
        simulationManager.init(conf);
        switchEnabled = getConfig().getBoolean(POSUMConfiguration.POLICY_SWITCH_ENABLED,
                POSUMConfiguration.POLICY_SWITCH_ENABLED_DEFAULT);
        super.serviceInit(conf);
    }

    @Override
    public void handle(POSUMEvent event) {
        try {
            switch (event.getType()) {
                case SIMULATOR_CONNECTED:
                    if (!simulationManager.isInState(STATE.STARTED)) {
                        addIfService(simulationManager);
                        simulationManager.start();
                        logger.info("Simulator connected");
                    }
                    break;
                case SIMULATION_START:
                    logger.trace("Starting simulation");
                    pmContext.getCommService().getSimulator().startSimulation();
                    break;
                case SIMULATION_FINISH:
                    simulationManager.simulationFinished();
                    ConcurrentSkipListSet<SimulationResult> results = event.getCastContent();
                    logger.trace("Policy scores: " + results);
                    decidePolicyChange(results);
                    break;
                default:
                    throw new POSUMException("Could not handle event of type " + event.getType());
            }
        } catch (Exception e) {
            throw new POSUMException("Could not handle event of type " + event.getType());
        }
    }

    private void decidePolicyChange(ConcurrentSkipListSet<SimulationResult> results) {
        if (!switchEnabled)
            return;
        MetaSchedulerInterface scheduler = pmContext.getCommService().getScheduler();
        if (scheduler == null)
            return;
        SimulationResult bestResult = results.last();
        if (bestResult == null)
            return;
        logger.info("Switching to best policy: " + bestResult.getPolicyName());
        scheduler.changeToPolicy(bestResult.getPolicyName());
    }
}

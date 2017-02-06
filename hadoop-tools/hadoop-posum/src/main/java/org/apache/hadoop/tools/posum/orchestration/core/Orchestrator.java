package org.apache.hadoop.tools.posum.orchestration.core;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.tools.posum.common.records.payload.SimulationResultPayload;
import org.apache.hadoop.tools.posum.common.util.PosumConfiguration;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.tools.posum.orchestration.master.OrchestrationMasterContext;
import org.apache.hadoop.tools.posum.client.scheduler.MetaScheduler;
import org.apache.hadoop.yarn.event.EventHandler;

import java.util.concurrent.ConcurrentSkipListSet;

public class Orchestrator extends CompositeService implements EventHandler<PosumEvent> {

    private static Log logger = LogFactory.getLog(Orchestrator.class);

    private OrchestrationMasterContext pmContext;
    private SimulationManager simulationManager;
    private boolean switchEnabled;

    public Orchestrator(OrchestrationMasterContext pmContext) {
        super(Orchestrator.class.getName());
        this.pmContext = pmContext;
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        simulationManager = new SimulationManager(pmContext);
        simulationManager.init(conf);
        switchEnabled = getConfig().getBoolean(PosumConfiguration.POLICY_SWITCH_ENABLED,
                PosumConfiguration.POLICY_SWITCH_ENABLED_DEFAULT);
        super.serviceInit(conf);
    }

    @Override
    public void handle(PosumEvent event) {
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
                    ConcurrentSkipListSet<SimulationResultPayload> results = event.getCastContent();
                    logger.trace("Policy scores: " + results);
                    decidePolicyChange(results);
                    break;
                default:
                    throw new PosumException("Could not handle event of type " + event.getType());
            }
        } catch (Exception e) {
            throw new PosumException("Could not handle event of type " + event.getType());
        }
    }

    private void decidePolicyChange(ConcurrentSkipListSet<SimulationResultPayload> results) {
        if (!switchEnabled)
            return;
        MetaScheduler scheduler = pmContext.getCommService().getScheduler();
        if (scheduler == null)
            return;
        SimulationResultPayload bestResult = results.last();
        if (bestResult == null)
            return;
        logger.info("Switching to best policy: " + bestResult.getPolicyName());
        scheduler.changeToPolicy(bestResult.getPolicyName());
    }
}

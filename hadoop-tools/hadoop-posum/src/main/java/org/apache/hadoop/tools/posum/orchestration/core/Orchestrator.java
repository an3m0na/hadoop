package org.apache.hadoop.tools.posum.orchestration.core;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.tools.posum.client.data.DatabaseUtils;
import org.apache.hadoop.tools.posum.client.scheduler.MetaScheduler;
import org.apache.hadoop.tools.posum.common.records.payload.SimulationResultPayload;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.tools.posum.orchestration.master.OrchestrationMasterContext;
import org.apache.hadoop.yarn.event.EventHandler;

import java.util.List;

import static org.apache.hadoop.tools.posum.common.util.conf.PosumConfiguration.COST_SCALE_FACTOR;
import static org.apache.hadoop.tools.posum.common.util.conf.PosumConfiguration.COST_SCALE_FACTOR_DEFAULT;
import static org.apache.hadoop.tools.posum.common.util.conf.PosumConfiguration.PENALTY_SCALE_FACTOR;
import static org.apache.hadoop.tools.posum.common.util.conf.PosumConfiguration.PENALTY_SCALE_FACTOR_DEFAULT;
import static org.apache.hadoop.tools.posum.common.util.conf.PosumConfiguration.SLOWDOWN_SCALE_FACTOR;
import static org.apache.hadoop.tools.posum.common.util.conf.PosumConfiguration.SLOWDOWN_SCALE_FACTOR_DEFAULT;

public class Orchestrator extends CompositeService implements EventHandler<PosumEvent> {

  private static Log logger = LogFactory.getLog(Orchestrator.class);

  private OrchestrationMasterContext orchestrationContext;
  private SimulationMonitor simulationMonitor;

  public Orchestrator(OrchestrationMasterContext orchestrationContext) {
    super(Orchestrator.class.getName());
    this.orchestrationContext = orchestrationContext;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    simulationMonitor = new SimulationMonitor(orchestrationContext);
    simulationMonitor.init(conf);
    SimulationScoreComparator simulationScoreComparator = new SimulationScoreComparator();
    simulationScoreComparator.updateScaleFactors(
      getConfig().getDouble(SLOWDOWN_SCALE_FACTOR, SLOWDOWN_SCALE_FACTOR_DEFAULT),
      getConfig().getDouble(PENALTY_SCALE_FACTOR, PENALTY_SCALE_FACTOR_DEFAULT),
      getConfig().getDouble(COST_SCALE_FACTOR, COST_SCALE_FACTOR_DEFAULT)
    );
    orchestrationContext.setSimulationScoreComparator(simulationScoreComparator);
    super.serviceInit(conf);
  }

  @Override
  public void handle(PosumEvent event) {
    try {
      switch (event.getType()) {
        case SIMULATOR_CONNECTED:
          if (!simulationMonitor.isInState(STATE.STARTED)) {
            addIfService(simulationMonitor);
            simulationMonitor.start();
            logger.info("Simulator connected");
          }
          break;
        case SIMULATION_START:
          logger.trace("Starting simulation");
          orchestrationContext.getCommService().getSimulator().startSimulation();
          break;
        case SIMULATION_FINISH:
          simulationMonitor.simulationFinished();
          List<SimulationResultPayload> results = event.getCastContent();
          logger.trace("Policy scores: " + results);
          decidePolicyChange(results);
          break;
        case SYSTEM_RESET:
          logger.info("Sending reset command to all POSUM processes...");
          orchestrationContext.getCommService().getSimulator().reset();
          orchestrationContext.getCommService().getDataMaster().reset();
          DatabaseUtils.storeLogEntry("System reset complete", orchestrationContext.getCommService().getDataMaster());
          logger.info("System reset complete");
          break;
        default:
          throw new PosumException("Could not handle event of type " + event.getType());
      }
    } catch (Exception e) {
      throw new PosumException("Could not handle event of type " + event.getType(), e);
    }
  }

  private void decidePolicyChange(List<SimulationResultPayload> results) {
    if (results == null || results.isEmpty())
      return;
    MetaScheduler scheduler = orchestrationContext.getCommService().getScheduler();
    orchestrationContext.getSimulationScoreComparator().sort(results);
    SimulationResultPayload bestResult = results.get(0);
    if (bestResult == null || scheduler == null)
      return;
    logger.info("Switching to best policy: " + bestResult.getPolicyName());
    scheduler.changeToPolicy(bestResult.getPolicyName());
  }
}

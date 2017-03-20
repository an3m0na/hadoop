package org.apache.hadoop.tools.posum.simulation.core.resourcemanager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.tools.posum.simulation.core.SimulationContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;

import java.lang.reflect.Method;

public class ResourceManagerWrapper extends ResourceManager {
  private static final Log LOG = LogFactory.getLog(ResourceManagerWrapper.class);
  private SimulationContext simulationContext;

  public ResourceManagerWrapper(SimulationContext simulationContext) {
    this.simulationContext = simulationContext;
  }

  protected ResourceScheduler createScheduler() {
    ResourceSchedulerWrapper scheduler = new ResourceSchedulerWrapper(simulationContext);
    scheduler.setConf(getConfig());
    return scheduler;
  }

  @Override
  protected void startWepApp() {
    // do nothing
  }

  @Override
  protected void serviceInit(Configuration configuration) throws Exception {

    super.serviceInit(configuration);

    Method removeService = CompositeService.class.getDeclaredMethod("removeService", Service.class);
    removeService.setAccessible(true);
    removeService.invoke(activeServices, rmContext.getResourceTrackerService());
    removeService.invoke(activeServices, rmContext.getApplicationMasterService());
    removeService.invoke(activeServices, rmContext.getClientRMService());

    removeService(rmContext.getRMAdminService());

  }


}

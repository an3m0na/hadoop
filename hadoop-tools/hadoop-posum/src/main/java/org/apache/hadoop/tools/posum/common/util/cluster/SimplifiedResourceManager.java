package org.apache.hadoop.tools.posum.common.util.cluster;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;

import java.lang.reflect.Method;

public class SimplifiedResourceManager<T extends AbstractYarnScheduler> extends ResourceManager {
  private InjectableResourceScheduler<T> injectableScheduler;

  public SimplifiedResourceManager(InjectableResourceScheduler<T> policy) {
    this.injectableScheduler = policy;
  }

  @Override
  protected ResourceScheduler createScheduler() {
    injectableScheduler.setConf(getConfig());
    return injectableScheduler;
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

  public InjectableResourceScheduler<T> getInjectableScheduler() {
    return injectableScheduler;
  }
}

package org.apache.hadoop.tools.posum.scheduler.portfolio.srtf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.common.util.DatabaseProvider;
import org.apache.hadoop.tools.posum.common.util.PosumConfiguration;
import org.apache.hadoop.tools.posum.scheduler.portfolio.PluginPolicyState;
import org.apache.hadoop.tools.posum.scheduler.portfolio.PluginSchedulerNode;
import org.apache.hadoop.tools.posum.scheduler.portfolio.common.FiCaPluginSchedulerNode;
import org.apache.hadoop.tools.posum.scheduler.portfolio.common.SimpleQueue;
import org.apache.hadoop.tools.posum.scheduler.portfolio.common.SimpleQueuePolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;

import java.util.Comparator;
import java.util.concurrent.ConcurrentSkipListSet;

import static org.apache.hadoop.tools.posum.common.util.Utils.orZero;

public class ShortestRTFirstPolicy extends SimpleQueuePolicy<SRTFAppAttempt, FiCaPluginSchedulerNode, SimpleQueue, ShortestRTFirstPolicy> {
  private static Log logger = LogFactory.getLog(ShortestRTFirstPolicy.class);

  private long lastCheck = 0;
  private long maxCheck;
  private AppWorkCalculator appWorkCalculator;

  public ShortestRTFirstPolicy() {
    super(SRTFAppAttempt.class, FiCaPluginSchedulerNode.class, SimpleQueue.class, ShortestRTFirstPolicy.class);
  }

  @Override
  public void initializePlugin(Configuration conf, DatabaseProvider dbProvider) {
    super.initializePlugin(conf, dbProvider);
    appWorkCalculator = new AppWorkCalculator(dbProvider);
    maxCheck = conf.getLong(PosumConfiguration.REPRIORITIZE_INTERVAL, PosumConfiguration.REPRIORITIZE_INTERVAL_DEFAULT);
  }

  @Override
  protected void updateAppPriority(SRTFAppAttempt app) {
    logger.debug("Updating app priority");
    try {
      appWorkCalculator.updateRemainingTime(app);
    } catch (Exception e) {
      logger.debug("Could not update app priority for : " + app.getApplicationId(), e);
    }
  }

  @Override
  public Comparator<SRTFAppAttempt> getApplicationComparator() {
    return new Comparator<SRTFAppAttempt>() {
      @Override
      public int compare(SRTFAppAttempt o1, SRTFAppAttempt o2) {
        if (o1.getApplicationId().equals(o2.getApplicationId()))
          return 0;
        if (o1.getResourceDeficit() == null)
          return 1;
        if (o2.getResourceDeficit() == null)
          return -1;
        double signum = Math.signum(o1.getResourceDeficit() - o2.getResourceDeficit());
        return signum > 0 ? 1 : -1; // avoid returning 0 for non-equivalent apps
      }
    };
  }

  @Override
  protected boolean checkIfPrioritiesExpired() {
    long now = System.currentTimeMillis();
    if (now - lastCheck > maxCheck) {
      lastCheck = now;
      return true;
    }
    return false;
  }

  @Override
  protected void updateApplicationPriorities() {
    ConcurrentSkipListSet<SRTFAppAttempt> oldOrderedApps = this.orderedApps;
    ConcurrentSkipListSet<SRTFAppAttempt> newOrderedApps = new ConcurrentSkipListSet<>(getApplicationComparator());
    // calculate remaining times for each application and compute sum
    Double invertedSum = 0.0;
    for (SRTFAppAttempt app : oldOrderedApps) {
      updateAppPriority(app);
      long remainingTime = orZero(app.getRemainingTime(getMinimumResourceCapability()));
      if (remainingTime == 0)
        continue; // ignore app (it's either finished or we do not have enough info for it)
      invertedSum += 1.0 / remainingTime;
    }
    for (SRTFAppAttempt app : oldOrderedApps) {
      app.calculateDeficit(getMinimumResourceCapability(), getClusterResource(), invertedSum);
      newOrderedApps.add(app);
    }
    this.orderedApps = newOrderedApps;
  }

  @Override
  protected <T extends SchedulerNode & PluginSchedulerNode> void importState(PluginPolicyState<T> state) {
    super.importState(state);
    updateApplicationPriorities();
  }
}


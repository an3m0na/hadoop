package org.apache.hadoop.tools.posum.scheduler.portfolio;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.common.records.call.FindByQueryCall;
import org.apache.hadoop.tools.posum.common.records.call.JobForAppCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.util.PosumConfiguration;
import org.apache.hadoop.tools.posum.common.util.Utils;
import org.apache.hadoop.tools.posum.common.util.DatabaseProvider;
import org.apache.hadoop.tools.posum.scheduler.portfolio.extca.ExtCaSchedulerNode;
import org.apache.hadoop.tools.posum.scheduler.portfolio.extca.ExtensibleCapacityScheduler;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;

import static org.apache.hadoop.tools.posum.common.util.Utils.orZero;

public class ShortestRTFirstPolicy extends ExtensibleCapacityScheduler<SRTFAppAttempt, ExtCaSchedulerNode> {


  private static Log logger = LogFactory.getLog(ShortestRTFirstPolicy.class);

  private long lastCheck = 0;
  private long maxCheck;

  public ShortestRTFirstPolicy() {
    super(SRTFAppAttempt.class, ExtCaSchedulerNode.class, ShortestRTFirstPolicy.class.getName(), true);
  }

  @Override
  public void initializePlugin(Configuration conf, DatabaseProvider dbProvider) {
    super.initializePlugin(conf, dbProvider);
    maxCheck = conf.getLong(PosumConfiguration.REPRIORITIZE_INTERVAL,
      PosumConfiguration.REPRIORITIZE_INTERVAL_DEFAULT);
  }

  @Override
  protected CapacitySchedulerConfiguration loadCustomCapacityConf(Configuration conf) {
    CapacitySchedulerConfiguration capacityConf = new CapacitySchedulerConfiguration(conf);
    capacityConf.setInt(CapacitySchedulerConfiguration.NODE_LOCALITY_DELAY, 0);
    return capacityConf;
  }

  @Override
  protected String resolveQueue(String queue, ApplicationId applicationId, String user, boolean isAppRecovering, ReservationId reservationID) {
    return YarnConfiguration.DEFAULT_QUEUE_NAME;
  }

  @Override
  protected String resolveMoveQueue(String queue, ApplicationId applicationId, String user) {
    return YarnConfiguration.DEFAULT_QUEUE_NAME;
  }

  protected JobProfile fetchJobProfile(String appId, String user) {
    Database db = dbProvider.getDatabase();
    if (db == null)
      // DataMaster is not connected; do nothing
      return null;
    JobForAppCall getJobProfileForApp = JobForAppCall.newInstance(appId, user);
    JobProfile job = db.execute(getJobProfileForApp).getEntity();
    if (job == null) {
      logger.error("Could not retrieve job info for " + appId);
      return null;
    }
    return job;
  }

  @Override
  protected void updateAppPriority(SRTFAppAttempt app) {
    logger.debug("Updating app priority");
    try {
      String appId = app.getApplicationId().toString();
      JobProfile job = fetchJobProfile(appId, app.getUser());
      if (app.getJobId() == null) {
        if (job == null)
          // something went wrong; do nothing
          return;
        app.setJobId(job.getId());
        app.setSubmitTime(job.getSubmitTime());
      }

      long avgMapDuration = orZero(job.getAvgMapDuration());
      int totalMaps = orZero(job.getTotalMapTasks());
      int completedMaps = orZero(job.getCompletedMaps());

      if (avgMapDuration != 0) {
        Long totalWork = avgMapDuration * totalMaps;
        Long remainingWork = totalWork - avgMapDuration * completedMaps;
        int totalReduceTasks = orZero(job.getTotalReduceTasks());
        if (totalReduceTasks > 0) {
          // there is reduce work to be done; get average task duration
          long avgReduceDuration = orZero(job.getAvgReduceDuration());
          long avgMapSize = orZero(job.getInputBytes()) / completedMaps;
          if (avgReduceDuration == 0 && avgMapSize != 0) {
            // estimate avg reduce time
            long totalReduceInputSize = orZero(job.getMapOutputBytes()) / completedMaps * totalMaps;
            long reducerInputSize = totalReduceInputSize / totalReduceTasks;
            avgReduceDuration = orZero(job.getAvgMapDuration()) * reducerInputSize / avgMapSize;
          }
          remainingWork += avgReduceDuration * (totalReduceTasks - orZero(job.getCompletedReduces()));
          totalWork += avgReduceDuration * totalReduceTasks;
        }
        app.setRemainingWork(remainingWork);
        app.setTotalWork(totalWork);
      }
    } catch (Exception e) {
      logger.debug("Could not update app priority for : " + app.getApplicationId(), e);
    }
  }

  @Override
  public Comparator<FiCaSchedulerApp> getApplicationComparator() {
    return new Comparator<FiCaSchedulerApp>() {
      @Override
      public int compare(FiCaSchedulerApp o1, FiCaSchedulerApp o2) {
        if (o1.getApplicationId().equals(o2.getApplicationId()))
          return 0;
        SRTFAppAttempt srtf1 = (SRTFAppAttempt) o1, srtf2 = (SRTFAppAttempt) o2;
        return srtf1.getResourceDeficit() - srtf2.getResourceDeficit();
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

  protected void updateApplicationPriorities(LeafQueue queue, String applicationSetName) {
    if (!applicationSetName.equals("activeApplications"))
      return;
    Set<FiCaSchedulerApp> apps = Utils.readField(queue, LeafQueue.class, applicationSetName);
    // calculate remaining times for each application and compute sum
    Double invertedSum = 0.0;
    for (Iterator<FiCaSchedulerApp> i = apps.iterator(); i.hasNext(); ) {
      SRTFAppAttempt app = (SRTFAppAttempt) i.next();
      updateAppPriority(app);
      if (app.getRemainingWork() != null)
        invertedSum += 1.0 / app.getRemainingTime(getMinimumResourceCapability());
    }
    for (Iterator<FiCaSchedulerApp> i = apps.iterator(); i.hasNext(); ) {
      SRTFAppAttempt app = (SRTFAppAttempt) i.next();
      // remove, update and add to resort
      i.remove();
      app.calculateDeficit(getMinimumResourceCapability(), getClusterResource(), invertedSum);
      apps.add(app);
    }
  }
}


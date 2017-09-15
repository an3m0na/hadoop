package org.apache.hadoop.tools.posum.scheduler.portfolio;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.common.records.call.FindByIdCall;
import org.apache.hadoop.tools.posum.common.records.call.JobForAppCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.AppProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.util.DatabaseProvider;
import org.apache.hadoop.tools.posum.common.util.PosumConfiguration;
import org.apache.hadoop.tools.posum.scheduler.portfolio.extca.ExtCaSchedulerNode;
import org.apache.hadoop.tools.posum.scheduler.portfolio.extca.ExtensibleCapacityScheduler;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;

import java.util.Comparator;

import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.APP;
import static org.apache.hadoop.tools.posum.common.util.Utils.orZero;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.DOT;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.MAXIMUM_APPLICATIONS_SUFFIX;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.PREFIX;


public class EDLSPolicy<E extends EDLSPolicy> extends ExtensibleCapacityScheduler<EDLSAppAttempt, ExtCaSchedulerNode> {

  protected Log logger;

  protected final String DEADLINE_QUEUE = "deadline", BATCH_QUEUE = "batch", ROOT_QUEUE = "root";
  private long lastCheck = 0;
  private long maxCheck;
  protected float deadlinePriority = PosumConfiguration.DC_PRIORITY_DEFAULT;

  public EDLSPolicy(Class<E> eClass) {
    super(EDLSAppAttempt.class, ExtCaSchedulerNode.class, eClass.getName(), true);
    logger = LogFactory.getLog(eClass);
  }

  @Override
  public void initializePlugin(Configuration conf, DatabaseProvider dbProvider) {
    super.initializePlugin(conf, dbProvider);
    maxCheck = conf.getLong(PosumConfiguration.REPRIORITIZE_INTERVAL,
      PosumConfiguration.REPRIORITIZE_INTERVAL_DEFAULT);
    deadlinePriority = conf.getFloat(PosumConfiguration.DC_PRIORITY, PosumConfiguration.DC_PRIORITY_DEFAULT);
  }

  @Override
  protected CapacitySchedulerConfiguration loadCustomCapacityConf(Configuration conf) {
    CapacitySchedulerConfiguration capacityConf = new CapacitySchedulerConfiguration(conf);
    capacityConf.setQueues(ROOT_QUEUE, new String[]{DEADLINE_QUEUE, BATCH_QUEUE});
    capacityConf.setMaximumCapacity(ROOT_QUEUE + DOT + DEADLINE_QUEUE, 100);
    capacityConf.setMaximumCapacity(ROOT_QUEUE + DOT + BATCH_QUEUE, 100);
    capacityConf.setInt(PREFIX + ROOT_QUEUE + DOT + DEADLINE_QUEUE + DOT + MAXIMUM_APPLICATIONS_SUFFIX, 10000);
    capacityConf.setInt(PREFIX + ROOT_QUEUE + DOT + BATCH_QUEUE + DOT + MAXIMUM_APPLICATIONS_SUFFIX, 10000);
    capacityConf.setInt(CapacitySchedulerConfiguration.NODE_LOCALITY_DELAY, 0);
    return capacityConf;
  }

  @Override
  protected String resolveQueue(String queue,
                                ApplicationId applicationId,
                                String user,
                                boolean isAppRecovering,
                                ReservationId reservationID) {

    JobProfile job = fetchJobProfile(applicationId.toString());
    if (job == null || job.getDeadline() == 0) {
      logger.debug("Adding app to BC queue " + applicationId);
      return BATCH_QUEUE;
    }
    logger.debug("Adding app to DC queue " + applicationId);
    return DEADLINE_QUEUE;
  }

  @Override
  protected String resolveMoveQueue(String queue, ApplicationId applicationId, String user) {
    return resolveQueue(queue, applicationId, user, false, null);
  }

  protected JobProfile fetchJobProfile(String appId) {
    Database db = dbProvider.getDatabase();
    if (db == null)
      // DataMaster is not connected; do nothing
      return null;
    JobProfile job = db.execute(JobForAppCall.newInstance(appId)).getEntity();
    while (job == null || job.getDeadline() == null) {
      try {
        db.awaitUpdate();
      } catch (InterruptedException e) {
        logger.error("Could not retrieve job information for " + appId);
        return null;
      }
      job = db.execute(JobForAppCall.newInstance(appId)).getEntity();
    }
    return job;
  }

  @Override
  protected void updateAppPriority(EDLSAppAttempt app) {
    logger.trace("Updating app priority");
    try {
      if (EDLSAppAttempt.Type.DC.equals(app.getType()))
        // application should already be initialized; do nothing
        return;
      if (dbProvider.getDatabase() == null)
        // DataMaster is not connected; do nothing
        return;
      String appId = app.getApplicationId().toString();
      JobProfile job = fetchJobProfile(appId);
      if (app.getType() == null) {
        // new app
        if (job == null)
          // something went wrong; do nothing
          return;
        app.setJobId(job.getId());
        Long submitTime = job.getSubmitTime();
        if (submitTime == null) {
          AppProfile appProfile = dbProvider.getDatabase().execute(FindByIdCall.newInstance(APP, appId)).getEntity();
          if (appProfile != null)
            submitTime = appProfile.getStartTime();
        }
        app.setSubmitTime(submitTime);
        app.setType(orZero(job.getDeadline()) == 0 ? EDLSAppAttempt.Type.BC : EDLSAppAttempt.Type.DC);
        app.setDeadline(job.getDeadline());
      }
      app.setExecutionTime(orZero(job.getAvgMapDuration()) * job.getCompletedMaps() +
        orZero(job.getAvgReduceDuration()) * job.getCompletedReduces());
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
        EDLSAppAttempt edls1 = (EDLSAppAttempt) o1, edls2 = (EDLSAppAttempt) o2;
        if (EDLSAppAttempt.Type.DC.equals(edls1.getType()) && EDLSAppAttempt.Type.DC.equals(edls2.getType()))
          // both dc jobs, earliest deadline first
          return new Long(edls1.getRemaining() - edls2.getRemaining()).intValue();
        if (EDLSAppAttempt.Type.DC.equals(edls1.getType()))
          // only elds1 is dc, it has priority
          return -1;
        if (EDLSAppAttempt.Type.DC.equals(edls2.getType()))
          // only elds2 is dc, it has priority
          return 1;
        // both are bc jobs
        if (edls1.getSlowdown().equals(edls2.getSlowdown()))
          // apply FIFO
          return edls1.getApplicationId().compareTo(edls2.getApplicationId());
        // largest slowdown first
        return new Double(Math.signum(edls2.getSlowdown() - edls1.getSlowdown())).intValue();
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
}

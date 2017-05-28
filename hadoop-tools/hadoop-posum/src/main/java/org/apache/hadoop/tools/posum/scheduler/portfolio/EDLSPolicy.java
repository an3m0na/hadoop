package org.apache.hadoop.tools.posum.scheduler.portfolio;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.common.records.call.JobForAppCall;
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

import static org.apache.hadoop.tools.posum.common.util.Utils.orZero;


public class EDLSPolicy<E extends EDLSPolicy> extends ExtensibleCapacityScheduler<EDLSAppAttempt, ExtCaSchedulerNode> {

  protected Log logger;

  protected final String DEADLINE_QUEUE = "deadline", BATCH_QUEUE = "default";
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
    capacityConf.setInt(CapacitySchedulerConfiguration.NODE_LOCALITY_DELAY, 0);
    return capacityConf;
  }

  @Override
  protected String resolveQueue(String queue,
                                ApplicationId applicationId,
                                String user,
                                boolean isAppRecovering,
                                ReservationId reservationID) {

    JobProfile job = fetchJobProfile(applicationId.toString(), user);
    logger.info("resolving queue");
    if (job == null) {
      logger.info("job not found");
      return BATCH_QUEUE;
    }
    if (job.getDeadline() == null)
      return DEADLINE_QUEUE;
    logger.info("deadline not found");
    return BATCH_QUEUE;
  }

  @Override
  protected String resolveMoveQueue(String queue, ApplicationId applicationId, String user) {
    return resolveQueue(queue, applicationId, user, false, null);
  }

  protected JobProfile fetchJobProfile(String appId, String user) {
    Database db = dbProvider.getDatabase();
    if (db == null)
      // DataMaster is not connected; do nothing
      return null;
    JobProfile job = db.execute(JobForAppCall.newInstance(appId)).getEntity();
    if (job == null) {
      logger.error("Could not retrieve job info for " + appId);
      return null;
    }
    return job;
  }

  @Override
  protected void updateAppPriority(EDLSAppAttempt app) {
    logger.debug("Updating app priority");
    try {
      if (EDLSAppAttempt.Type.DC.equals(app.getType()))
        // application should already be initialized; do nothing
        return;
      if (dbProvider.getDatabase() == null)
        // DataMaster is not connected; do nothing
        return;
      String appId = app.getApplicationId().toString();
      JobProfile job = fetchJobProfile(appId, app.getUser());
      if (app.getType() == null) {
        if (job == null)
          // something went wrong; do nothing
          return;
        app.setJobId(job.getId());
        app.setSubmitTime(job.getSubmitTime());
        app.setType(job.getDeadline() == null ? EDLSAppAttempt.Type.BC : EDLSAppAttempt.Type.DC);
        app.setDeadline(job.getDeadline());
      }
      app.setExecutionTime(orZero(job.getAvgMapDuration()) * job.getCompletedMaps() +
        job.getAvgReduceDuration() * job.getCompletedReduces());
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

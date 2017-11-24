package org.apache.hadoop.tools.posum.scheduler.portfolio.srtf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.common.records.call.JobForAppCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.util.communication.DatabaseProvider;

import java.text.MessageFormat;

import static org.apache.hadoop.tools.posum.common.util.Utils.orZero;

public class AppWorkCalculator {
  private static Log logger = LogFactory.getLog(AppWorkCalculator.class);

  private DatabaseProvider dbProvider;

  protected AppWorkCalculator(DatabaseProvider dbProvider) {
    this.dbProvider = dbProvider;
  }

  public void updateRemainingTime(SRTFAppAttempt app) {
    String appId = app.getApplicationId().toString();
    JobProfile job = fetchJobProfile(appId);
    if (job == null) {
      logger.debug("Could not update app priority for : " + app.getApplicationId() + " because job cannot be found");
      return;
    }
    if (app.getJobId() == null) {
      app.setJobId(job.getId());
      app.setSubmitTime(job.getSubmitTime());
    }
    Long avgMapDuration = job.getAvgMapDuration();
    int totalMaps = job.getTotalMapTasks();
    int completedMaps = job.getCompletedMaps();
    if (avgMapDuration == null) {
      logger.debug(MessageFormat.format("Not enough information to calculate remaining time for {0}", app.getJobId()));
      return;
    }

    long totalWork = avgMapDuration * totalMaps;
    long remainingWork = totalWork - avgMapDuration * completedMaps;

    int totalReduceTasks = job.getTotalReduceTasks();
    if (totalReduceTasks > 0) {
      Long avgReduceDuration = orZero(job.getAvgReduceDuration());
      if (avgReduceDuration == 0 && job.getTotalSplitSize() != null && job.getMapOutputBytes() != null) {
        // estimate avg reduce time
        double avgMapSize = job.getTotalSplitSize() / completedMaps;
        double avgMapRate = avgMapDuration / avgMapSize;
        long totalReduceInputSize = orZero(job.getMapOutputBytes()) / completedMaps * totalMaps;
        long reducerInputSize = totalReduceInputSize / totalReduceTasks;
        avgReduceDuration = Double.valueOf(reducerInputSize * avgMapRate).longValue();
      }
      remainingWork += avgReduceDuration * (totalReduceTasks - job.getCompletedReduces());
      totalWork += avgReduceDuration * totalReduceTasks;
    }
    app.setRemainingWork(remainingWork);
    app.setTotalWork(totalWork);
    logger.debug(MessageFormat.format("Work for {0}: remaining={1}, total={2}", app.getJobId(), remainingWork, totalWork));
  }

  private JobProfile fetchJobProfile(String appId) {
    Database db = dbProvider.getDatabase();
    if (db == null)
      // DataMaster is not connected; do nothing
      return null;
    JobForAppCall getJobProfileForApp = JobForAppCall.newInstance(appId);
    JobProfile job = db.execute(getJobProfileForApp).getEntity();
    if (job == null) {
      logger.warn("Could not retrieve job info for " + appId);
      return null;
    }
    return job;
  }
}

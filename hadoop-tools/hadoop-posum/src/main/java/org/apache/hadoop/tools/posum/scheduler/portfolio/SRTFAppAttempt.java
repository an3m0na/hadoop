package org.apache.hadoop.tools.posum.scheduler.portfolio;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.scheduler.portfolio.extca.ExtCaAppAttempt;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;

public class SRTFAppAttempt extends ExtCaAppAttempt {
  private static final Log logger = LogFactory.getLog(SRTFAppAttempt.class);

  private Long submitTime;
  private String jobId;
  private Long totalWork;
  private Long remainingWork;
  // FIXME: in the future should be resources, not just memory ints
  private Integer resourceDeficit;
  private Integer desiredResource;

  public SRTFAppAttempt(Configuration posumConf, ApplicationAttemptId applicationAttemptId, String user, Queue queue, ActiveUsersManager activeUsersManager, RMContext rmContext) {
    super(posumConf, applicationAttemptId, user, queue, activeUsersManager, rmContext);
  }

  public SRTFAppAttempt(ExtCaAppAttempt inner) {
    super(inner);
  }

  @Override
  public String toString() {
    return super.toString() +
      "\n      JobId: " + jobId +
      "\n      SubmitTime: " + submitTime +
      "\n      TotalWork: " + totalWork +
      "\n      RemainingWork: " + remainingWork +
      "\n      DesiredResource: " + desiredResource +
      "\n      ResourceDeficit: " + resourceDeficit;
  }

  @Override
  public synchronized void transferStateFromPreviousAttempt(SchedulerApplicationAttempt appAttempt) {
    logger.debug("Transfering state from previous attempt " + appAttempt.getApplicationAttemptId());
    super.transferStateFromPreviousAttempt(appAttempt);
    if (appAttempt instanceof SRTFAppAttempt) {
      SRTFAppAttempt srtfApp = (SRTFAppAttempt) appAttempt;
      setSubmitTime(srtfApp.getSubmitTime());
      setJobId(srtfApp.getJobId());
      setTotalWork(srtfApp.getTotalWork());
      setRemainingWork(srtfApp.getTotalWork());
      setResourceDeficit(srtfApp.getResourceDeficit());
    }
  }

  public Long getSubmitTime() {
    return submitTime;
  }

  public void setSubmitTime(Long submitTime) {
    this.submitTime = submitTime;
  }

  public String getJobId() {
    return jobId;
  }

  public void setJobId(String jobId) {
    this.jobId = jobId;
  }

  public Long getRemainingWork() {
    return remainingWork;
  }

  public void setRemainingWork(Long remainingWork) {
    this.remainingWork = remainingWork;
  }

  public Integer getResourceDeficit() {
    return resourceDeficit;
  }

  public void setResourceDeficit(Integer resourceDeficit) {
    this.resourceDeficit = resourceDeficit;
  }

  public Long getRemainingTime(Resource minAllocation) {
    Integer currentSlots = getCurrentConsumption().getMemory() / minAllocation.getMemory();
    return remainingWork / currentSlots;
  }

  public void calculateDeficit(Resource minAllocation, Resource maxResource, Double normalizer) {
    // if there is not enough information, assign at least a slot in order to start
    Double desired = new Integer(minAllocation.getMemory()).doubleValue();
    if (remainingWork != null) {
      // we have information
      Long remainingTime = getRemainingTime(minAllocation);
      // calculate resource share according to remaining time
      desired = 1.0 / remainingTime / normalizer * maxResource.getMemory();
      // adjust for starvation
      long elapsedTime = System.currentTimeMillis() - submitTime;
      Integer totalSlots = maxResource.getMemory() / minAllocation.getMemory();
      long timeIfAlone = totalWork / totalSlots;
      desired *= (elapsedTime + remainingTime) / timeIfAlone;
    }
    desiredResource = desired.intValue();
    resourceDeficit = getCurrentConsumption().getMemory() - desiredResource;
  }

  public Long getTotalWork() {
    return totalWork;
  }

  public void setTotalWork(Long totalWork) {
    this.totalWork = totalWork;
  }

}

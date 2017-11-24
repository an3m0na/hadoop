package org.apache.hadoop.tools.posum.scheduler.portfolio.dos;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.tools.posum.scheduler.portfolio.PluginApplicationAttempt;
import org.apache.hadoop.tools.posum.scheduler.portfolio.common.FiCaPluginApplicationAttempt;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;

public class DOSAppAttempt extends FiCaPluginApplicationAttempt {
  private static final Log logger = LogFactory.getLog(DOSAppAttempt.class);

  private Long totalInputSize;
  private Integer inputSplits;

  public DOSAppAttempt(ApplicationAttemptId applicationAttemptId, String user, Queue queue, ActiveUsersManager activeUsersManager, RMContext rmContext) {
    super(applicationAttemptId, user, queue, activeUsersManager, rmContext);
  }

  public <T extends SchedulerApplicationAttempt & PluginApplicationAttempt> DOSAppAttempt(T predecessor,
                                                                                          ActiveUsersManager activeUsersManager,
                                                                                          RMContext rmContext) {
    super(predecessor, activeUsersManager, rmContext);
  }

  public Long getTotalInputSize() {
    return totalInputSize;
  }

  public void setTotalInputSize(Long totalInputSize) {
    this.totalInputSize = totalInputSize;
  }

  public Integer getInputSplits() {
    return inputSplits;
  }

  public void setInputSplits(Integer inputSplits) {
    this.inputSplits = inputSplits;
  }

  @Override
  public String toString() {
    return super.toString() + "\n      InputSize: " + totalInputSize;
  }

  @Override
  public synchronized void transferStateFromPreviousAttempt(SchedulerApplicationAttempt appAttempt) {
    logger.debug("Transferring state from previous attempt " + appAttempt.getApplicationAttemptId());
    super.transferStateFromPreviousAttempt(appAttempt);
    if (appAttempt instanceof DOSAppAttempt) {
      DOSAppAttempt dosApp = (DOSAppAttempt) appAttempt;
      setInputSplits(dosApp.getInputSplits());
      setTotalInputSize(dosApp.getTotalInputSize());
    }
  }
}

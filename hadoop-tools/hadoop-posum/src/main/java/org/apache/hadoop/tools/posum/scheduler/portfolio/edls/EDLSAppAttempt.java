package org.apache.hadoop.tools.posum.scheduler.portfolio.edls;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.common.util.conf.PosumConfiguration;
import org.apache.hadoop.tools.posum.scheduler.portfolio.PluginApplicationAttempt;
import org.apache.hadoop.tools.posum.scheduler.portfolio.common.FiCaPluginApplicationAttempt;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;

public class EDLSAppAttempt extends FiCaPluginApplicationAttempt implements Configurable {
  private static final Log logger = LogFactory.getLog(EDLSAppAttempt.class);

  public enum Type {
    DC, //deadline constrained application
    BC // batch application
  }

  private Long submitTime = 0L;
  private Long deadline;
  private Long executionTime = 0L;
  private Long minExecTime = 1L;
  private String jobId;
  private Type type;
  private Configuration conf;

  public EDLSAppAttempt(ApplicationAttemptId applicationAttemptId,
                        String user,
                        Queue queue,
                        ActiveUsersManager activeUsersManager,
                        RMContext rmContext) {
    super(applicationAttemptId, user, queue, activeUsersManager, rmContext);
  }

  public <T extends SchedulerApplicationAttempt & PluginApplicationAttempt> EDLSAppAttempt(T predecessor,
                                                                                           ActiveUsersManager activeUsersManager,
                                                                                           RMContext rmContext) {
    super(predecessor, activeUsersManager, rmContext);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    minExecTime = conf.getLong(PosumConfiguration.MIN_EXEC_TIME, PosumConfiguration.MIN_EXEC_TIME_DEFAULT);
  }

  @Override
  public String toString() {
    return super.toString() +
      "\n      JobId: " + jobId +
      "\n      SubmitTime: " + submitTime +
      "\n      Deadline: " + deadline +
      "\n      Remaining: " + getRemaining() +
      "\n      ExectionTime: " + executionTime +
      "\n      Slowdown: " + getSlowdown();
  }

  @Override
  public synchronized void transferStateFromPreviousAttempt(SchedulerApplicationAttempt appAttempt) {
    logger.debug("Transferring state from previous attempt " + appAttempt.getApplicationAttemptId());
    super.transferStateFromPreviousAttempt(appAttempt);
    if (appAttempt instanceof EDLSAppAttempt) {
      EDLSAppAttempt edlsApp = (EDLSAppAttempt) appAttempt;
      setJobId(edlsApp.getJobId());
      setDeadline(edlsApp.getDeadline());
      setSubmitTime(edlsApp.getSubmitTime());
      setExecutionTime(edlsApp.getExecutionTime());
    }
  }

  public Long getDeadline() {
    return deadline;
  }

  public void setDeadline(Long deadline) {
    this.deadline = deadline;
  }

  public Double getSlowdown() {
    if (executionTime == 0)
      return 0.0;
    long waitTime = System.currentTimeMillis() - submitTime;
    return 1.0 * waitTime / Math.max(executionTime, minExecTime);
  }

  public Long getRemaining() {
    if (deadline == null)
      return -1L;
    return deadline - System.currentTimeMillis();
  }

  public Long getSubmitTime() {
    return submitTime;
  }

  public void setSubmitTime(Long submitTime) {
    this.submitTime = submitTime;
  }

  public void setExecutionTime(Long executionTime) {
    this.executionTime = executionTime;
  }

  public Long getExecutionTime() {
    return executionTime;
  }

  public String getJobId() {
    return jobId;
  }

  public void setJobId(String jobId) {
    this.jobId = jobId;
  }

  public Type getType() {
    return type;
  }

  public void setType(Type type) {
    this.type = type;
  }
}

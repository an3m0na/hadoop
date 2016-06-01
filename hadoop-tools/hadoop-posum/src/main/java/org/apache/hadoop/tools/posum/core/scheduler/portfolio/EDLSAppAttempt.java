package org.apache.hadoop.tools.posum.core.scheduler.portfolio;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.tools.posum.core.scheduler.portfolio.extca.ExtCaAppAttempt;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;

/**
 * Created by ane on 1/22/16.
 */
public class EDLSAppAttempt extends ExtCaAppAttempt {
    private static final Log logger = LogFactory.getLog(EDLSAppAttempt.class);

    public enum Type {
        DC, //deadline constrained application
        BC // batch application
    }

    private Long submitTime;
    private Long deadline = 0L;
    private Long executionTime = 1L;
    private Long minExecTime = 0L;
    private String jobId;
    private Type type;

    public EDLSAppAttempt(Configuration posumConf, ApplicationAttemptId applicationAttemptId, String user, Queue queue, ActiveUsersManager activeUsersManager, RMContext rmContext) {
        super(posumConf, applicationAttemptId, user, queue, activeUsersManager, rmContext);
        if (posumConf != null)
            minExecTime = posumConf.getLong(POSUMConfiguration.MIN_EXEC_TIME,
                    POSUMConfiguration.MIN_EXEC_TIME_DEFAULT);
    }

    public EDLSAppAttempt(ExtCaAppAttempt inner) {
        super(inner);
    }

    @Override
    public String toString() {
        return super.toString() +
                "\n      JobId: " + jobId +
                "\n      SubmitTime: " + submitTime +
                "\n      Deadline: " + deadline +
                "\n      Remaining: " + getRemaining() +
                "\n      Slowdown: " + executionTime;
    }

    @Override
    public synchronized void transferStateFromPreviousAttempt(SchedulerApplicationAttempt appAttempt) {
        logger.debug("Transfering state from previous attempt " + appAttempt.getApplicationAttemptId());
        super.transferStateFromPreviousAttempt(appAttempt);
        if (appAttempt instanceof EDLSAppAttempt) {
            EDLSAppAttempt edlsApp = (EDLSAppAttempt) appAttempt;
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

package org.apache.hadoop.tools.posum.core.scheduler.portfolio;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

    private Long deadline;
    private Double slowdown;

    public EDLSAppAttempt(ApplicationAttemptId applicationAttemptId, String user, Queue queue, ActiveUsersManager activeUsersManager, RMContext rmContext) {
        super(applicationAttemptId, user, queue, activeUsersManager, rmContext);
    }

    public EDLSAppAttempt(ExtCaAppAttempt inner) {
        super(inner);
    }

    @Override
    public String toString() {
        return super.toString() +
                "\n      Deadline: " + deadline +
                "\n      Slowdown: " + slowdown;
    }

    @Override
    public synchronized void transferStateFromPreviousAttempt(SchedulerApplicationAttempt appAttempt) {
        logger.debug("Transfering state from previous attempt " + appAttempt.getApplicationAttemptId());
        super.transferStateFromPreviousAttempt(appAttempt);
        if (appAttempt instanceof EDLSAppAttempt) {
            EDLSAppAttempt edlsApp = (EDLSAppAttempt) appAttempt;
            setDeadline(edlsApp.getDeadline());
            setSlowdown(edlsApp.getSlowdown());
        }
    }

    public Long getDeadline() {
        return deadline;
    }

    public void setDeadline(Long deadline) {
        this.deadline = deadline;
    }

    public Double getSlowdown() {
        return slowdown;
    }

    public void setSlowdown(Double slowdown) {
        this.slowdown = slowdown;
    }
}

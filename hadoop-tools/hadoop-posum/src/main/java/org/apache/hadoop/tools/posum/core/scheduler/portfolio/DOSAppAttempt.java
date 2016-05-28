package org.apache.hadoop.tools.posum.core.scheduler.portfolio;

import org.apache.hadoop.tools.posum.core.scheduler.portfolio.extca.ExtCaAppAttempt;
import org.apache.hadoop.tools.posum.core.scheduler.portfolio.singleq.SQSAppAttempt;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;

/**
 * Created by ane on 1/22/16.
 */
public class DOSAppAttempt extends ExtCaAppAttempt {

    private Long totalInputSize;
    private Integer inputSplits;

    public DOSAppAttempt(ApplicationAttemptId applicationAttemptId, String user, Queue queue, ActiveUsersManager activeUsersManager, RMContext rmContext) {
        super(applicationAttemptId, user, queue, activeUsersManager, rmContext);
    }

    public DOSAppAttempt(ExtCaAppAttempt inner) {
        super(inner);
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
        return "DOSAttempt_" + getApplicationId() + "=" + totalInputSize;
    }

    @Override
    public synchronized void transferStateFromPreviousAttempt(SchedulerApplicationAttempt appAttempt) {
        super.transferStateFromPreviousAttempt(appAttempt);
        if (appAttempt instanceof DOSAppAttempt) {
            DOSAppAttempt dosapp = (DOSAppAttempt) appAttempt;
            setInputSplits(dosapp.getInputSplits());
            setTotalInputSize(dosapp.getTotalInputSize());
        }
    }
}

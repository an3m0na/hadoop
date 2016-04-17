package org.apache.hadoop.tools.posum.core.scheduler.portfolio;

import org.apache.hadoop.tools.posum.core.scheduler.portfolio.singleq.SQSAppAttempt;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;

/**
 * Created by ane on 1/22/16.
 */
public class DOSAppAttempt extends SQSAppAttempt {

    private Long totalInputSize;
    private Integer inputSplits;

    public DOSAppAttempt(ApplicationAttemptId applicationAttemptId, String user, Queue queue, ActiveUsersManager activeUsersManager, RMContext rmContext) {
        super(applicationAttemptId, user, queue, activeUsersManager, rmContext);
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
}

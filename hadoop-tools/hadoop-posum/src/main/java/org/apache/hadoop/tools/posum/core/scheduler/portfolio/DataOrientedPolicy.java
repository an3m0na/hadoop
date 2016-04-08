package org.apache.hadoop.tools.posum.core.scheduler.portfolio;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.core.scheduler.portfolio.singleq.SQSQueue;
import org.apache.hadoop.tools.posum.core.scheduler.portfolio.singleq.SQSchedulerNode;
import org.apache.hadoop.tools.posum.core.scheduler.portfolio.singleq.SingleQueuePolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.*;

import java.util.*;

/**
 * Created by ane on 1/22/16.
 */
public class DataOrientedPolicy extends SingleQueuePolicy<
        DOSAppAttempt,
        SQSchedulerNode,
        SQSQueue,
        DataOrientedPolicy> {

    private static Log logger = LogFactory.getLog(DataOrientedPolicy.class);

    public DataOrientedPolicy() {
        super(DOSAppAttempt.class, SQSchedulerNode.class, SQSQueue.class, DataOrientedPolicy.class);
    }

    @Override
    protected void validateConf(Configuration conf) {
        super.validateConf(conf);
        //TODO maybe add smth
    }

    @Override
    protected Comparator<SchedulerApplication<DOSAppAttempt>> initQueueComparator() {
        return new Comparator<SchedulerApplication<DOSAppAttempt>>() {
            @Override
            public int compare(SchedulerApplication<DOSAppAttempt> a1, SchedulerApplication<DOSAppAttempt> a2) {
                if (a1.getCurrentAppAttempt() == null)
                    return 1;
                if (a2.getCurrentAppAttempt() == null)
                    return -1;
                if (a1.getCurrentAppAttempt().getApplicationId()
                        .equals(a2.getCurrentAppAttempt().getApplicationId()))
                    return 0;
                if (a1.getCurrentAppAttempt().getTotalInputSize() == null)
                    return 1;
                if (a2.getCurrentAppAttempt().getTotalInputSize() == null) {
                    return -1;
                }
                return new Long(a1.getCurrentAppAttempt().getTotalInputSize() -
                        a2.getCurrentAppAttempt().getTotalInputSize()).intValue();
            }
        };
    }

    @Override
    protected synchronized void initScheduler(Configuration conf) {
        super.initScheduler(conf);
        //TODO maybe add smth
    }

    @Override
    protected void updateAppPriority(SchedulerApplication<DOSAppAttempt> app) {
        try {
            //TODO access the database and think of a way to make sure the database is populated
        } catch (Exception e) {
            logger.debug("[DOScheduler] Could not read input size for: " + app.getCurrentAppAttempt().getApplicationId(), e);
        }

    }

    @Override
    protected void assignFromQueue(SQSchedulerNode node) {
        for (SchedulerApplication<DOSAppAttempt> app : orderedApps) {
            assignToApp(node, app);
        }
    }
}


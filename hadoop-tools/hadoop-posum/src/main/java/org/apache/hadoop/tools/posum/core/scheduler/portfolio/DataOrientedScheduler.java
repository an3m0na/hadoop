package org.apache.hadoop.tools.posum.core.scheduler.portfolio;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.core.scheduler.basic.SQSQueue;
import org.apache.hadoop.tools.posum.core.scheduler.basic.SQSchedulerMetrics;
import org.apache.hadoop.tools.posum.core.scheduler.basic.SQSchedulerNode;
import org.apache.hadoop.tools.posum.core.scheduler.basic.SingleQueueScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.*;

import java.util.*;

/**
 * Created by ane on 1/22/16.
 */
public class DataOrientedScheduler extends SingleQueueScheduler<
        DOSAppAttempt,
        SQSchedulerNode,
        SQSQueue,
        DataOrientedScheduler> {

    private static Log logger = LogFactory.getLog(DataOrientedScheduler.class);

    private TreeSet<SchedulerApplication<DOSAppAttempt>> orderedApps =
            new TreeSet<>(new Comparator<SchedulerApplication<DOSAppAttempt>>() {
                @Override
                public int compare(SchedulerApplication<DOSAppAttempt> a1, SchedulerApplication<DOSAppAttempt> a2) {
                    if (a1.getCurrentAppAttempt() == null)
                        return 1;
                    if (a2.getCurrentAppAttempt() == null)
                        return -1;
                    if (a1.getCurrentAppAttempt().getApplicationAttemptId()
                            .equals(a2.getCurrentAppAttempt().getApplicationAttemptId()))
                        return 0;
                    if (a1.getCurrentAppAttempt().getTotalInputSize() == null)
                        return 1;
                    if (a2.getCurrentAppAttempt().getTotalInputSize() == null) {
                        return -1;
                    }
                    return new Long(a1.getCurrentAppAttempt().getTotalInputSize() -
                            a2.getCurrentAppAttempt().getTotalInputSize()).intValue();
                }
            });


    public DataOrientedScheduler() {
        super(DOSAppAttempt.class, SQSchedulerNode.class, SQSQueue.class, DataOrientedScheduler.class);
    }


    @Override
    protected void validateConf(Configuration conf) {
        super.validateConf(conf);
        //TODO maybe add smth
    }

    @Override
    protected synchronized void initScheduler(Configuration conf) {
        super.initScheduler(conf);
        //TODO maybe add smth
    }

    private void readApplicationInfo(DOSAppAttempt attempt) {
        try {
            //TODO access the database and think of a way to make sure the database is populated
        } catch (Exception e) {
            logger.debug("[DOScheduler] Could not read input size for: " + attempt.getApplicationId(), e);
        }

    }

    @Override
    protected void onAppAttemptAdded(SchedulerApplication<DOSAppAttempt> app) {
        orderedApps.remove(app);
        readApplicationInfo(app.getCurrentAppAttempt());
        orderedApps.add(app);
    }

    @Override
    protected void onAppAdded(SchedulerApplication<DOSAppAttempt> app) {
        orderedApps.add(app);
    }

    @Override
    protected void onAppDone(SchedulerApplication<DOSAppAttempt> app) {
        orderedApps.remove(app);
    }

    @Override
    protected void assignFromQueue(SQSchedulerNode node) {
        for (SchedulerApplication<DOSAppAttempt> app : orderedApps) {
            assignToApp(node, app);
        }
    }
}


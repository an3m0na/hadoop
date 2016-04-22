package org.apache.hadoop.tools.posum.core.scheduler.portfolio;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.core.scheduler.portfolio.singleq.SQSQueue;
import org.apache.hadoop.tools.posum.core.scheduler.portfolio.singleq.SQSchedulerNode;
import org.apache.hadoop.tools.posum.core.scheduler.portfolio.singleq.SingleQueuePolicy;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication;

import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by ane on 1/22/16.
 */
public class FifoPolicy extends SingleQueuePolicy<
        FifoAppAttempt,
        SQSchedulerNode,
        SQSQueue,
        FifoPolicy> {

    private ConcurrentHashMap<ApplicationId, Long> submitTimes;

    private static Log logger = LogFactory.getLog(FifoPolicy.class);

    public FifoPolicy() {
        super(FifoAppAttempt.class, SQSchedulerNode.class, SQSQueue.class, FifoPolicy.class);
        submitTimes = new ConcurrentHashMap<>();
    }

    @Override
    protected void validateConf(Configuration conf) {
        super.validateConf(conf);
    }

    @Override
    protected Comparator<SchedulerApplication<FifoAppAttempt>> initQueueComparator() {
        return new Comparator<SchedulerApplication<FifoAppAttempt>>() {
            @Override
            public int compare(SchedulerApplication<FifoAppAttempt> a1, SchedulerApplication<FifoAppAttempt> a2) {
                if (a1.getCurrentAppAttempt() == null)
                    return 1;
                if (a2.getCurrentAppAttempt() == null)
                    return -1;
                if (a1.getCurrentAppAttempt().getApplicationId()
                        .equals(a2.getCurrentAppAttempt().getApplicationId()))
                    return 0;
                if (a1.getCurrentAppAttempt().getSubmitTime() == null)
                    return 1;
                if (a2.getCurrentAppAttempt().getSubmitTime() == null) {
                    return -1;
                }
                return new Long(a1.getCurrentAppAttempt().getSubmitTime() -
                        a2.getCurrentAppAttempt().getSubmitTime()).intValue();
            }
        };
    }

    @Override
    protected synchronized void initScheduler(Configuration conf) {
        super.initScheduler(conf);
    }

    @Override
    protected void updateAppPriority(SchedulerApplication<FifoAppAttempt> app) {
        ApplicationId id = app.getCurrentAppAttempt().getApplicationId();
        //TODO get submit time some other way (from the conf file) since information will be lost on scheduler change
        Long submitTime = submitTimes.get(id);
        if (submitTime == null) {
            submitTime = System.currentTimeMillis();
            submitTimes.put(id, submitTime);
        }
        app.getCurrentAppAttempt().setSubmitTime(submitTime);
    }

    @Override
    protected void assignFromQueue(SQSchedulerNode node) {
        for (SchedulerApplication<FifoAppAttempt> app : orderedApps) {
            assignToApp(node, app);
        }
    }
}


package org.apache.hadoop.tools.posum.core.scheduler.data;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.posum.core.scheduler.basic.SQSQueue;
import org.apache.hadoop.tools.posum.core.scheduler.basic.SQSchedulerMetrics;
import org.apache.hadoop.tools.posum.core.scheduler.basic.SQSchedulerNode;
import org.apache.hadoop.tools.posum.core.scheduler.basic.SingleQueueScheduler;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.UpdatedContainerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.*;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.*;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by ane on 1/22/16.
 */
public class DataOrientedScheduler extends SingleQueueScheduler<
        DOSAppAttempt,
        SQSchedulerNode,
        SQSQueue,
        SQSchedulerMetrics,
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
        super(DOSAppAttempt.class, SQSchedulerNode.class, SQSQueue.class, SQSchedulerMetrics.class, DataOrientedScheduler.class);
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


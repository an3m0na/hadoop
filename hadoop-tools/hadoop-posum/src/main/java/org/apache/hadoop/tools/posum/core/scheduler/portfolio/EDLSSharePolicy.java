package org.apache.hadoop.tools.posum.core.scheduler.portfolio;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobConfProxy;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.tools.posum.core.scheduler.portfolio.extca.ExtCaSchedulerNode;
import org.apache.hadoop.tools.posum.core.scheduler.portfolio.extca.ExtensibleCapacityScheduler;
import org.apache.hadoop.tools.posum.database.client.DBInterface;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;

import java.util.Map;

/**
 * Created by ane on 5/31/16.
 */
public class EDLSSharePolicy extends ExtensibleCapacityScheduler<EDLSAppAttempt, ExtCaSchedulerNode> {

    private static Log logger = LogFactory.getLog(EDLSSharePolicy.class);

    protected final String DEADLINE_FLEX_KEY = "deadline";

    public EDLSSharePolicy() {
        super(EDLSAppAttempt.class, ExtCaSchedulerNode.class, EDLSSharePolicy.class.getName(), true);
    }

    @Override
    protected CapacitySchedulerConfiguration loadCustomCapacityConf(Configuration conf) {
        CapacitySchedulerConfiguration capacityConf = new CapacitySchedulerConfiguration(conf);
        capacityConf.setInt(CapacitySchedulerConfiguration.NODE_LOCALITY_DELAY, 0);
        capacityConf.setQueues("root", new String[]{"deadline", "batch"});
        float deadlinePriority = pluginConf.getFloat(POSUMConfiguration.DEADLINE_CONSTRAINED_PRIORITY,
                POSUMConfiguration.DEADLINE_CONSTRAINED_PRIORITY_DEFAULT);
        capacityConf.setCapacity("deadline", 100 * deadlinePriority);
        capacityConf.setCapacity("batch", 100 * (1 - deadlinePriority));
        return capacityConf;
    }

    @Override
    protected void updateAppPriority(EDLSAppAttempt app) {
        logger.debug("Updating app priority");
        try {
            String appId = app.getApplicationId().toString();
            if (app.getDeadline() != null || app.getSlowdown() != null)
                return;
            DBInterface db = commService.getDB();
            if (db != null) {
                JobProfile job = db.getJobProfileForApp(appId, app.getUser());
                if (job != null) {
                    Map<String, String> flexFields = job.getFlexFields();
                    String deadlineString = flexFields.get("deadline");
                    if (deadlineString != null && deadlineString.length() > 0) {
                        // application flexfield has been previously updated with deadline
                        app.setDeadline(Long.valueOf(deadlineString));
                    } else {
                        // might be first initialization, so try to get deadline from JobConf
                        JobConfProxy confProxy = db.getJobConf(job.getId());
                        deadlineString = confProxy.getEntry(POSUMConfiguration.APP_DEADLINE);
                        if (deadlineString != null && deadlineString.length() > 0) {
                            app.setDeadline(Long.valueOf(deadlineString));
                        } else {
                            // this is a batch job
                            //TODO check counters for slowdown
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.debug("Could not read input size for: " + app.getApplicationId(), e);
        }
    }
}

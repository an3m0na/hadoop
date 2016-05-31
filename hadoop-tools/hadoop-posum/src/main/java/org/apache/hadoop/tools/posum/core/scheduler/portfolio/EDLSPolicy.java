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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;

import java.util.Comparator;
import java.util.Map;

/**
 * Created by ane on 5/31/16.
 */
public class EDLSPolicy<E extends EDLSPolicy> extends ExtensibleCapacityScheduler<EDLSAppAttempt, ExtCaSchedulerNode> {

    protected Log logger;

    protected final String DEADLINE_FLEX_KEY = "deadline";

    public EDLSPolicy(Class<E> eClass) {
        super(EDLSAppAttempt.class, ExtCaSchedulerNode.class, eClass.getName(), true);
        logger = LogFactory.getLog(eClass);
    }

    @Override
    protected CapacitySchedulerConfiguration loadCustomCapacityConf(Configuration conf) {
        CapacitySchedulerConfiguration capacityConf = new CapacitySchedulerConfiguration(conf);
        capacityConf.setInt(CapacitySchedulerConfiguration.NODE_LOCALITY_DELAY, 0);
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
                        app.setDeadline(job.getSubmitTime() + Long.valueOf(deadlineString));
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

    @Override
    public Comparator<FiCaSchedulerApp> getApplicationComparator() {
        return new Comparator<FiCaSchedulerApp>() {
            @Override
            public int compare(FiCaSchedulerApp o1, FiCaSchedulerApp o2) {
                if (o1.getApplicationId().equals(o2.getApplicationId()))
                    return 0;
                EDLSAppAttempt edls1 = (EDLSAppAttempt) o1, edls2 = (EDLSAppAttempt) o2;
                if (edls1.getDeadline() != null && edls2.getDeadline() != null)
                    // both dc jobs, earliest deadline first
                    return new Long(edls1.getDeadline() - edls2.getDeadline()).intValue();
                if (edls1.getDeadline() == null)
                    // only elds2 is dc, it has priority
                    return 1;
                if (edls2.getDeadline() == null)
                    // only elds1 is dc, it has priority
                    return -1;
                if (edls1.getSlowdown() != null && edls2.getSlowdown() != null)
                    // both are bc jobs
                    if (edls1.getSlowdown().equals(edls2.getSlowdown()))
                        // equal importance
                        return 0;
                    else
                        // largest slowdown first
                        return new Double(Math.signum(edls2.getSlowdown() - edls1.getSlowdown())).intValue();
                // something went wrong, so follow FIFO
                return edls1.getApplicationId().compareTo(edls2.getApplicationId());
            }
        };
    }
}

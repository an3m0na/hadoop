package org.apache.hadoop.tools.posum.core.scheduler.portfolio;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.QueueState;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;

/**
 * Created by ane on 5/31/16.
 */
public class EDLSSharePolicy extends EDLSPolicy<EDLSSharePolicy> {

    public EDLSSharePolicy() {
        super(EDLSSharePolicy.class);
    }

    @Override
    protected CapacitySchedulerConfiguration loadCustomCapacityConf(Configuration conf) {
        CapacitySchedulerConfiguration capacityConf = new CapacitySchedulerConfiguration(conf);
        capacityConf.setInt(CapacitySchedulerConfiguration.NODE_LOCALITY_DELAY, 0);
        capacityConf.setQueues("root", new String[]{"default", "deadline", "batch"});
        float deadlinePriority = pluginConf.getFloat(POSUMConfiguration.DC_PRIORITY,
                POSUMConfiguration.DC_PRIORITY_DEFAULT);
        capacityConf.setCapacity("root.default", 0);
        capacityConf.set("root.default.state", QueueState.STOPPED.getStateName());
        capacityConf.setCapacity("root.deadline", 100 * deadlinePriority);
        capacityConf.setCapacity("root.batch", 100 * (1 - deadlinePriority));
        return capacityConf;
    }

    @Override
    protected String resolveQueue(String queue,
                                  ApplicationId applicationId,
                                  String user,
                                  boolean isAppRecovering,
                                  ReservationId reservationID) {

        JobProfile job = fetchJobProfile(applicationId.toString(), user);
        if (job == null) {
            return "batch";
        }
        if (EDLSAppAttempt.Type.DC.name().equals(job.getFlexField(EDLSAppAttempt.Type.class.getName())))
            return "deadline";
        return "batch";
    }

    @Override
    protected String resolveMoveQueue(String queue, ApplicationId applicationId, String user) {
        return resolveQueue(queue, applicationId, user, false, null);
    }
}

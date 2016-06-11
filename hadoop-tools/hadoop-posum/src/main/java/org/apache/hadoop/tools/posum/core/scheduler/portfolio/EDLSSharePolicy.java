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
        capacityConf.setQueues("root", new String[]{DEADLINE_QUEUE, BATCH_QUEUE});
        capacityConf.setCapacity("root." + DEADLINE_QUEUE, 100 * deadlinePriority);
        capacityConf.setCapacity("root." + BATCH_QUEUE, 100 * (1 - deadlinePriority));
        return capacityConf;
    }
}

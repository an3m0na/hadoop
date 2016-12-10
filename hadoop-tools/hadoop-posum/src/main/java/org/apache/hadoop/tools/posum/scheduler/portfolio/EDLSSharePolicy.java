package org.apache.hadoop.tools.posum.scheduler.portfolio;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;

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

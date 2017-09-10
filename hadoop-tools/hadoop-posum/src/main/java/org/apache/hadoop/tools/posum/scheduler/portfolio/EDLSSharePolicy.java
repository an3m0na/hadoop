package org.apache.hadoop.tools.posum.scheduler.portfolio;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.DOT;

public class EDLSSharePolicy extends EDLSPolicy<EDLSSharePolicy> {

  public EDLSSharePolicy() {
    super(EDLSSharePolicy.class);
  }

  @Override
  protected CapacitySchedulerConfiguration loadCustomCapacityConf(Configuration conf) {
    CapacitySchedulerConfiguration capacityConf = super.loadCustomCapacityConf(conf);
    capacityConf.setCapacity("root"+DOT + DEADLINE_QUEUE, 100 * deadlinePriority);
    capacityConf.setCapacity("root" +DOT + BATCH_QUEUE, 100 * (1 - deadlinePriority));
    return capacityConf;
  }
}

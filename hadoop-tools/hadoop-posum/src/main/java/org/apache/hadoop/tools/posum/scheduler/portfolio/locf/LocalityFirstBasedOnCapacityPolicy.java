package org.apache.hadoop.tools.posum.scheduler.portfolio.locf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.scheduler.portfolio.extca.ExtCaAppAttempt;
import org.apache.hadoop.tools.posum.scheduler.portfolio.common.FiCaPluginSchedulerNode;
import org.apache.hadoop.tools.posum.scheduler.portfolio.extca.ExtensibleCapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;

public class LocalityFirstBasedOnCapacityPolicy extends ExtensibleCapacityScheduler<ExtCaAppAttempt, FiCaPluginSchedulerNode> {

  public LocalityFirstBasedOnCapacityPolicy() {
    super(ExtCaAppAttempt.class, FiCaPluginSchedulerNode.class, LocalityFirstBasedOnCapacityPolicy.class.getName(), true);
  }

  @Override
  protected CapacitySchedulerConfiguration loadCustomCapacityConf(Configuration conf) {
    CapacitySchedulerConfiguration capacityConf = new CapacitySchedulerConfiguration(conf);
    capacityConf.setInt(CapacitySchedulerConfiguration.NODE_LOCALITY_DELAY, Integer.MAX_VALUE);
    return capacityConf;
  }
}


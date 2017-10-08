package org.apache.hadoop.tools.posum.scheduler.portfolio.locf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.scheduler.portfolio.common.FiCaPluginApplicationAttempt;
import org.apache.hadoop.tools.posum.scheduler.portfolio.common.FiCaPluginSchedulerNode;
import org.apache.hadoop.tools.posum.scheduler.portfolio.common.ExtensibleCapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;

public class LocalityFirstBasedOnCapacityPolicy extends ExtensibleCapacityScheduler<FiCaPluginApplicationAttempt, FiCaPluginSchedulerNode> {

  public LocalityFirstBasedOnCapacityPolicy() {
    super(FiCaPluginApplicationAttempt.class, FiCaPluginSchedulerNode.class, LocalityFirstBasedOnCapacityPolicy.class.getName());
  }

  @Override
  protected CapacitySchedulerConfiguration loadCustomCapacityConf(Configuration conf) {
    CapacitySchedulerConfiguration capacityConf = new CapacitySchedulerConfiguration(conf);
    capacityConf.setInt(CapacitySchedulerConfiguration.NODE_LOCALITY_DELAY, Integer.MAX_VALUE);
    return capacityConf;
  }
}


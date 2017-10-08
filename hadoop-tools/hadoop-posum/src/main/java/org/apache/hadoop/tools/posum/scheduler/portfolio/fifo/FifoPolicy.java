package org.apache.hadoop.tools.posum.scheduler.portfolio.fifo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.scheduler.portfolio.common.FiCaPluginApplicationAttempt;
import org.apache.hadoop.tools.posum.scheduler.portfolio.common.FiCaPluginSchedulerNode;
import org.apache.hadoop.tools.posum.scheduler.portfolio.common.ExtensibleCapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;

public class FifoPolicy extends ExtensibleCapacityScheduler<FiCaPluginApplicationAttempt, FiCaPluginSchedulerNode> {
  public FifoPolicy() {
    super(FiCaPluginApplicationAttempt.class, FiCaPluginSchedulerNode.class, FifoPolicy.class.getName());
  }

  @Override
  protected CapacitySchedulerConfiguration loadCustomCapacityConf(Configuration conf) {
    CapacitySchedulerConfiguration capacityConf = new CapacitySchedulerConfiguration(conf);
    capacityConf.setInt(CapacitySchedulerConfiguration.NODE_LOCALITY_DELAY, 0);
    return capacityConf;
  }
}


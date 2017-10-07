package org.apache.hadoop.tools.posum.scheduler.portfolio.fifo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.scheduler.portfolio.extca.ExtCaAppAttempt;
import org.apache.hadoop.tools.posum.scheduler.portfolio.common.FiCaPluginSchedulerNode;
import org.apache.hadoop.tools.posum.scheduler.portfolio.extca.ExtensibleCapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;

public class FifoPolicy extends ExtensibleCapacityScheduler<ExtCaAppAttempt, FiCaPluginSchedulerNode> {


  private static Log logger = LogFactory.getLog(FifoPolicy.class);

  public FifoPolicy() {
    super(ExtCaAppAttempt.class, FiCaPluginSchedulerNode.class, FifoPolicy.class.getName(), true);
  }

  @Override
  protected CapacitySchedulerConfiguration loadCustomCapacityConf(Configuration conf) {
    CapacitySchedulerConfiguration capacityConf = new CapacitySchedulerConfiguration(conf);
    capacityConf.setInt(CapacitySchedulerConfiguration.NODE_LOCALITY_DELAY, 0);
    return capacityConf;
  }
}


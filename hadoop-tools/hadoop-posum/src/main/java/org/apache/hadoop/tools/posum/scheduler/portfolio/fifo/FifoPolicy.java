package org.apache.hadoop.tools.posum.scheduler.portfolio.fifo;

import org.apache.hadoop.tools.posum.scheduler.portfolio.common.FiCaPluginSchedulerNode;
import org.apache.hadoop.tools.posum.scheduler.portfolio.common.FiCaPluginApplicationAttempt;
import org.apache.hadoop.tools.posum.scheduler.portfolio.common.SimpleQueue;
import org.apache.hadoop.tools.posum.scheduler.portfolio.common.SimpleQueuePolicy;

public class FifoPolicy extends SimpleQueuePolicy<FiCaPluginApplicationAttempt, FiCaPluginSchedulerNode, SimpleQueue, FifoPolicy> {

  public FifoPolicy() {
    super(FiCaPluginApplicationAttempt.class, FiCaPluginSchedulerNode.class, SimpleQueue.class, FifoPolicy.class);
  }

  @Override
  protected void updateAppPriority(FiCaPluginApplicationAttempt app) {

  }
}

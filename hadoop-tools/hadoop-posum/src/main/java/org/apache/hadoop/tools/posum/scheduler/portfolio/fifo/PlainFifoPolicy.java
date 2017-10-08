package org.apache.hadoop.tools.posum.scheduler.portfolio.fifo;

import org.apache.hadoop.tools.posum.scheduler.portfolio.common.FiCaPluginSchedulerNode;
import org.apache.hadoop.tools.posum.scheduler.portfolio.common.FiCaPluginApplicationAttempt;
import org.apache.hadoop.tools.posum.scheduler.portfolio.common.SimpleQueue;
import org.apache.hadoop.tools.posum.scheduler.portfolio.common.SimpleQueuePolicy;

public class PlainFifoPolicy extends SimpleQueuePolicy<FiCaPluginApplicationAttempt, FiCaPluginSchedulerNode, SimpleQueue, PlainFifoPolicy> {

  public PlainFifoPolicy() {
    super(FiCaPluginApplicationAttempt.class, FiCaPluginSchedulerNode.class, SimpleQueue.class, PlainFifoPolicy.class);
  }

  @Override
  protected void updateAppPriority(FiCaPluginApplicationAttempt app) {

  }
}

package org.apache.hadoop.tools.posum.scheduler.portfolio.fifo;

import org.apache.hadoop.tools.posum.scheduler.portfolio.common.FiCaPluginSchedulerNode;
import org.apache.hadoop.tools.posum.scheduler.portfolio.singleq.SQSAppAttempt;
import org.apache.hadoop.tools.posum.scheduler.portfolio.singleq.SQSQueue;
import org.apache.hadoop.tools.posum.scheduler.portfolio.singleq.SingleQueuePolicy;

public class PlainFifoPolicy extends SingleQueuePolicy<SQSAppAttempt, FiCaPluginSchedulerNode, SQSQueue, PlainFifoPolicy> {

  public PlainFifoPolicy() {
    super(SQSAppAttempt.class, FiCaPluginSchedulerNode.class, SQSQueue.class, PlainFifoPolicy.class);
  }

  @Override
  protected void updateAppPriority(SQSAppAttempt app) {

  }
}

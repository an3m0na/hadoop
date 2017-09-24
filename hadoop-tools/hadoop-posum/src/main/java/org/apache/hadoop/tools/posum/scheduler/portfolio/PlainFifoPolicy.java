package org.apache.hadoop.tools.posum.scheduler.portfolio;

import org.apache.hadoop.tools.posum.scheduler.portfolio.singleq.SQSAppAttempt;
import org.apache.hadoop.tools.posum.scheduler.portfolio.singleq.SQSQueue;
import org.apache.hadoop.tools.posum.scheduler.portfolio.singleq.SQSchedulerNode;
import org.apache.hadoop.tools.posum.scheduler.portfolio.singleq.SingleQueuePolicy;

public class PlainFifoPolicy extends SingleQueuePolicy<SQSAppAttempt, SQSchedulerNode, SQSQueue, PlainFifoPolicy> {

  public PlainFifoPolicy() {
    super(SQSAppAttempt.class, SQSchedulerNode.class, SQSQueue.class, PlainFifoPolicy.class);
  }

  @Override
  protected void updateAppPriority(SQSAppAttempt app) {

  }
}

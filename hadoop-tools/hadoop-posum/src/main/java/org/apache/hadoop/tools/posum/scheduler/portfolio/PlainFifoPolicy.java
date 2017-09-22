package org.apache.hadoop.tools.posum.scheduler.portfolio;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.tools.posum.scheduler.portfolio.singleq.SQSAppAttempt;
import org.apache.hadoop.tools.posum.scheduler.portfolio.singleq.SQSQueue;
import org.apache.hadoop.tools.posum.scheduler.portfolio.singleq.SQSchedulerNode;
import org.apache.hadoop.tools.posum.scheduler.portfolio.singleq.SingleQueuePolicy;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerAppUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.Comparator;

public class PlainFifoPolicy extends SingleQueuePolicy<SQSAppAttempt, SQSchedulerNode, SQSQueue, PlainFifoPolicy> {
  private static Log LOG = LogFactory.getLog(PlainFifoPolicy.class);

  public PlainFifoPolicy() {
    super(SQSAppAttempt.class, SQSchedulerNode.class, SQSQueue.class, PlainFifoPolicy.class);
  }

  @Override
  protected Comparator<SchedulerApplication<SQSAppAttempt>> getApplicationComparator() {
    // must be defined but is not used
    return new Comparator<SchedulerApplication<SQSAppAttempt>>() {
      @Override
      public int compare(SchedulerApplication<SQSAppAttempt> o1, SchedulerApplication<SQSAppAttempt> o2) {
        if (o1 == null || o1.getCurrentAppAttempt() == null)
          return 1;
        if (o2 == null || o2.getCurrentAppAttempt() == null)
          return -1;
        return o1.getCurrentAppAttempt().getApplicationId().compareTo(o2.getCurrentAppAttempt().getApplicationId());
      }
    };
  }

  @Override
  protected void assignFromQueue(SQSchedulerNode node) {
    // Try to assign containers to applications in fifo order
    for (SchedulerApplication<SQSAppAttempt> app : orderedApps) {
      SQSAppAttempt application = app.getCurrentAppAttempt();
      if (application == null) {
        continue;
      }

      LOG.trace("pre-assignContainers");
      application.showRequests();
      synchronized (application) {
        // Check if this resource is on the blacklist
        if (SchedulerAppUtils.isBlacklisted(application, node, LOG)) {
          continue;
        }

        for (Priority priority : application.getPriorities()) {
          int maxContainers =
            getMaxAllocatableContainers(application, priority, node,
              NodeType.OFF_SWITCH);
          // Ensure the application needs containers of this priority
          if (maxContainers > 0) {
            int assignedContainers =
              assignContainersOnNode(node, application, priority);
            // Do not assign out of order w.r.t priorities
            if (assignedContainers == 0) {
              break;
            }
          }
        }
      }

      LOG.trace("post-assignContainers");
      application.showRequests();

      // Done
      if (Resources.lessThan(getResourceCalculator(), clusterResource,
        node.getAvailableResource(), minimumAllocation)) {
        break;
      }
    }
  }

  @Override
  protected void updateAppPriority(SchedulerApplication<SQSAppAttempt> app) {

  }
}

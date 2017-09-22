package org.apache.hadoop.tools.posum.scheduler.portfolio;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.tools.posum.scheduler.portfolio.singleq.SQSAppAttempt;
import org.apache.hadoop.tools.posum.scheduler.portfolio.singleq.SQSQueue;
import org.apache.hadoop.tools.posum.scheduler.portfolio.singleq.SQSchedulerNode;
import org.apache.hadoop.tools.posum.scheduler.portfolio.singleq.SingleQueuePolicy;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerAppUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication;
import org.apache.hadoop.yarn.util.resource.Resources;

import java.util.Comparator;

public class LocalityFirstPolicy extends SingleQueuePolicy<SQSAppAttempt, SQSchedulerNode, SQSQueue, LocalityFirstPolicy> {
  private static Log LOG = LogFactory.getLog(LocalityFirstPolicy.class);

  public LocalityFirstPolicy() {
    super(SQSAppAttempt.class, SQSchedulerNode.class, SQSQueue.class, LocalityFirstPolicy.class);
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
    assignByLocality(node, NodeType.NODE_LOCAL);
    if (outOfResources(node))
      return;
    assignByLocality(node, NodeType.RACK_LOCAL);
    if (outOfResources(node))
      return;
    assignByLocality(node, NodeType.OFF_SWITCH);
  }

  private boolean outOfResources(SQSchedulerNode node) {
    return Resources.lessThan(getResourceCalculator(), clusterResource,
      node.getAvailableResource(), minimumAllocation);
  }

  private void assignByLocality(SQSchedulerNode node, NodeType localityLevel) {
    for (SchedulerApplication<SQSAppAttempt> app : orderedApps) {
      SQSAppAttempt application = app.getCurrentAppAttempt();
      if (application == null) {
        continue;
      }

      LOG.trace("pre-assignContainers-" + localityLevel + " for " + application.getApplicationAttemptId());
      application.showRequests();
      synchronized (application) {
        // Check if this resource is on the blacklist
        if (SchedulerAppUtils.isBlacklisted(application, node, LOG)) {
          continue;
        }

        for (Priority priority : application.getPriorities()) {
          int maxContainers = getMaxAllocatableContainers(application, priority, node, localityLevel);
          if (maxContainers > 0) {
            int numContainers = 0;
            switch (localityLevel) {
              case NODE_LOCAL:
                application.addSchedulingOpportunity(priority);
                numContainers = assignNodeLocalContainers(node, application, priority);
                break;
              case RACK_LOCAL:
                if (!hasNodeLocalRequests(application, priority) || application.getSchedulingOpportunities(priority) >= getNumClusterNodes())
                  numContainers = assignRackLocalContainers(node, application, priority);
                break;
              case OFF_SWITCH:
                if (!hasRackLocalRequests(application, priority) || application.getSchedulingOpportunities(priority) >= getNumClusterNodes())
                  numContainers = assignOffSwitchContainers(node, application, priority);
            }
            if (numContainers == 0)
              break; // do not assign out of order w.r.t priorities
            else
              application.resetSchedulingOpportunities(priority);
          }
        }
      }
      LOG.trace("post-assignContainers-" + localityLevel + " for " + application.getApplicationAttemptId());
      application.showRequests();

      if (outOfResources(node)) {
        break;
      }
    }
  }

  private boolean hasRackLocalRequests(SQSAppAttempt application, Priority priority) {
    for (SQSchedulerNode node : nodes.values()) {
      ResourceRequest request = application.getResourceRequest(priority, node.getRackName());
      if (request != null && request.getNumContainers() > 0)
        return true;
    }
    return false;
  }

  private boolean hasNodeLocalRequests(SQSAppAttempt application, Priority priority) {
    for (SQSchedulerNode node : nodes.values()) {
      ResourceRequest request = application.getResourceRequest(priority, node.getNodeName());
      if (request != null && request.getNumContainers() > 0)
        return true;
    }
    return false;
  }

  @Override
  protected void updateAppPriority(SchedulerApplication<SQSAppAttempt> app) {

  }
}

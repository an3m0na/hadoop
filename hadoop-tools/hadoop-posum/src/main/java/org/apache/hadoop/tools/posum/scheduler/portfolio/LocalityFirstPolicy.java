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
import org.apache.hadoop.yarn.util.resource.Resources;

public class LocalityFirstPolicy extends SingleQueuePolicy<SQSAppAttempt, SQSchedulerNode, SQSQueue, LocalityFirstPolicy> {
  private static Log LOG = LogFactory.getLog(LocalityFirstPolicy.class);

  public LocalityFirstPolicy() {
    super(SQSAppAttempt.class, SQSchedulerNode.class, SQSQueue.class, LocalityFirstPolicy.class);
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
    for (SQSAppAttempt app : orderedApps) {
      LOG.trace("pre-assignContainers-" + localityLevel + " for " + app.getApplicationAttemptId());
      app.showRequests();
      synchronized (app) {
        // Check if this resource is on the blacklist
        if (SchedulerAppUtils.isBlacklisted(app, node, LOG)) {
          continue;
        }

        boolean amNotStarted = !hasAMResources(app);
        if (amNotStarted && !canAMStart(app)) {
          continue;
        }

        for (Priority priority : app.getPriorities()) {
          int maxContainers = getMaxAllocatableContainers(app, priority, node, localityLevel);
          if (maxContainers > 0) {
            int numContainers = 0;
            switch (localityLevel) {
              case NODE_LOCAL:
                app.addSchedulingOpportunity(priority);
                numContainers = assignNodeLocalContainers(node, app, priority);
                break;
              case RACK_LOCAL:
                if (!hasNodeLocalRequests(app, priority) || app.getSchedulingOpportunities(priority) >= getNumClusterNodes())
                  numContainers = assignRackLocalContainers(node, app, priority);
                break;
              case OFF_SWITCH:
                if (!hasRackLocalRequests(app, priority) || app.getSchedulingOpportunities(priority) >= getNumClusterNodes())
                  numContainers = assignOffSwitchContainers(node, app, priority);
            }
            if (numContainers == 0)
              break; // do not assign out of order w.r.t priorities
            else {
              app.resetSchedulingOpportunities(priority);
              if (amNotStarted && hasAMResources(app))
                Resources.addTo(usedAMResource, app.getAMResource());
            }
          }
        }
      }
      LOG.trace("post-assignContainers-" + localityLevel + " for " + app.getApplicationAttemptId());
      app.showRequests();

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
  protected void updateAppPriority(SQSAppAttempt app) {

  }
}

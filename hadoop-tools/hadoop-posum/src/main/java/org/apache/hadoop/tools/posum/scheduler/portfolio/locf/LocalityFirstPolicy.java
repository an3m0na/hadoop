package org.apache.hadoop.tools.posum.scheduler.portfolio.locf;

import org.apache.hadoop.tools.posum.scheduler.portfolio.common.FiCaPluginSchedulerNode;
import org.apache.hadoop.tools.posum.scheduler.portfolio.common.FiCaPluginApplicationAttempt;
import org.apache.hadoop.tools.posum.scheduler.portfolio.common.SimpleQueue;
import org.apache.hadoop.tools.posum.scheduler.portfolio.common.SimpleQueuePolicy;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerAppUtils;
import org.apache.hadoop.yarn.util.resource.Resources;

public class LocalityFirstPolicy extends SimpleQueuePolicy<FiCaPluginApplicationAttempt, FiCaPluginSchedulerNode, SimpleQueue, LocalityFirstPolicy> {

  public LocalityFirstPolicy() {
    super(FiCaPluginApplicationAttempt.class, FiCaPluginSchedulerNode.class, SimpleQueue.class, LocalityFirstPolicy.class);
  }

  @Override
  protected void assignFromQueue(FiCaPluginSchedulerNode node) {
    assignByLocality(node, NodeType.NODE_LOCAL);
    if (outOfResources(node))
      return;
    assignByLocality(node, NodeType.RACK_LOCAL);
    if (outOfResources(node))
      return;
    assignByLocality(node, NodeType.OFF_SWITCH);
  }

  private boolean outOfResources(FiCaPluginSchedulerNode node) {
    return Resources.lessThan(getResourceCalculator(), clusterResource,
      node.getAvailableResource(), minimumAllocation);
  }

  private void assignByLocality(FiCaPluginSchedulerNode node, NodeType localityLevel) {
    for (FiCaPluginApplicationAttempt app : orderedApps) {
      logger.trace("pre-assignContainers-" + localityLevel + " for " + app.getApplicationAttemptId());
      app.showRequests();
      synchronized (app) {
        // Check if this resource is on the blacklist
        if (SchedulerAppUtils.isBlacklisted(app, node, logger)) {
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
      logger.trace("post-assignContainers-" + localityLevel + " for " + app.getApplicationAttemptId());
      app.showRequests();

      if (outOfResources(node)) {
        break;
      }
    }
  }

  private boolean hasRackLocalRequests(FiCaPluginApplicationAttempt application, Priority priority) {
    for (FiCaPluginSchedulerNode node : nodes.values()) {
      ResourceRequest request = application.getResourceRequest(priority, node.getRackName());
      if (request != null && request.getNumContainers() > 0)
        return true;
    }
    return false;
  }

  private boolean hasNodeLocalRequests(FiCaPluginApplicationAttempt application, Priority priority) {
    for (FiCaPluginSchedulerNode node : nodes.values()) {
      ResourceRequest request = application.getResourceRequest(priority, node.getNodeName());
      if (request != null && request.getNumContainers() > 0)
        return true;
    }
    return false;
  }

  @Override
  protected void updateAppPriority(FiCaPluginApplicationAttempt app) {

  }
}

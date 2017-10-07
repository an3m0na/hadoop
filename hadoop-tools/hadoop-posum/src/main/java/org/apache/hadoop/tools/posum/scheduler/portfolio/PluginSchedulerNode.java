package org.apache.hadoop.tools.posum.scheduler.portfolio;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;

public interface PluginSchedulerNode {
  SchedulerNode getCoreNode();
}

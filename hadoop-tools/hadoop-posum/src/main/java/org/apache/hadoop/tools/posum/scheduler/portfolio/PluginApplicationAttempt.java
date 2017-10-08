package org.apache.hadoop.tools.posum.scheduler.portfolio;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;

public interface PluginApplicationAttempt {
  SchedulerApplicationAttempt getCoreApp();
}

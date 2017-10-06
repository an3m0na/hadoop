package org.apache.hadoop.tools.posum.scheduler.portfolio;

import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.common.util.DatabaseProvider;
import org.apache.hadoop.tools.posum.common.util.InjectableResourceScheduler;
import org.apache.hadoop.tools.posum.scheduler.TestScheduler;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;


public abstract class TestPolicy<T extends PluginPolicy> extends TestScheduler {
  private Class<? extends PluginPolicy> schedulerClass;

  protected TestPolicy(Class<T> schedulerClass) {
    this.schedulerClass = schedulerClass;
  }

  @Override
  protected InjectableResourceScheduler initScheduler() {
    return new InjectableResourceScheduler<>(schedulerClass, new DatabaseProvider() {
      @Override
      public Database getDatabase() {
        return db;
      }
    });
  }
}

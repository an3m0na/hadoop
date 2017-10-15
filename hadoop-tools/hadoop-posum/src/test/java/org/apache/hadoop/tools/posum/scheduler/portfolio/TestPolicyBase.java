package org.apache.hadoop.tools.posum.scheduler.portfolio;

import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.common.util.DatabaseProvider;
import org.apache.hadoop.tools.posum.common.util.InjectableResourceScheduler;
import org.apache.hadoop.tools.posum.scheduler.TestSchedulerBase;


public abstract class TestPolicyBase<T extends PluginPolicy> extends TestSchedulerBase {
  private Class<? extends PluginPolicy> schedulerClass;

  protected TestPolicyBase(Class<T> schedulerClass) {
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

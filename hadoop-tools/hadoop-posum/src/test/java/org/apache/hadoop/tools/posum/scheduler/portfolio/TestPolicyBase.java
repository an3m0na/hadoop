package org.apache.hadoop.tools.posum.scheduler.portfolio;

import org.apache.hadoop.tools.posum.common.util.cluster.InjectableResourceScheduler;
import org.apache.hadoop.tools.posum.scheduler.TestSchedulerBase;

import static org.apache.hadoop.tools.posum.client.data.DatabaseUtils.newProvider;


public abstract class TestPolicyBase<T extends PluginPolicy> extends TestSchedulerBase {
  private Class<? extends PluginPolicy> schedulerClass;

  protected TestPolicyBase(Class<T> schedulerClass) {
    this.schedulerClass = schedulerClass;
  }

  @Override
  protected InjectableResourceScheduler initScheduler() {
    return new InjectableResourceScheduler<>(schedulerClass, newProvider(db));
  }
}

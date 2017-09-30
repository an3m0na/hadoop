package org.apache.hadoop.tools.posum.scheduler.core;

import org.apache.hadoop.tools.posum.common.util.InjectableResourceScheduler;
import org.apache.hadoop.tools.posum.scheduler.TestScheduler;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.when;

public class TestPortfolioMetaScheduler extends TestScheduler {

  @Override
  protected InjectableResourceScheduler initScheduler() {
    MetaSchedulerCommService mockCommService = Mockito.mock(MetaSchedulerCommService.class);
    when(mockCommService.getDatabase()).thenReturn(db);
    return new InjectableResourceScheduler<>(new PortfolioMetaScheduler(conf, mockCommService));
  }

  @Test
  public void smokeTest() throws Exception {
    defaultSmokeTest();
  }

}

package org.apache.hadoop.tools.posum.scheduler.portfolio.srtf;

import org.apache.hadoop.tools.posum.scheduler.portfolio.TestPolicy;
import org.apache.hadoop.tools.posum.test.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class TestShortestRTFirstPolicy extends TestPolicy<ShortestRTFirstPolicy> {

  public TestShortestRTFirstPolicy() {
    super(ShortestRTFirstPolicy.class);
  }

  @Test
  public void smokeTest() throws Exception {
    defaultSmokeTest();
  }

}

package org.apache.hadoop.tools.posum.scheduler.portfolio.locf;

import org.apache.hadoop.mapreduce.v2.api.records.Locality;
import org.apache.hadoop.tools.posum.common.util.conf.PosumConfiguration;
import org.apache.hadoop.tools.posum.scheduler.portfolio.TestPolicyBase;
import org.apache.hadoop.tools.posum.test.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@Category(IntegrationTest.class)
public class TestLocalityFirst extends TestPolicyBase {

  public TestLocalityFirst() {
    super(LocalityFirstPolicy.class);
  }

  @Test
  public void smokeTest() throws Exception {
    defaultSmokeTest();
  }

  @Test
  public void testAMLimit() throws Exception {
    conf.setFloat(PosumConfiguration.MAX_AM_RATIO, 0.5f); // max 3 apps can run
    startRM();
    registerNodes(2);

    submitApp(1);
    assertTrue(waitForAMContainer(getApp(1), 0));
    submitApp(2);
    assertTrue(waitForAMContainer(getApp(2), 0));

    submitApp(3);
    assertFalse(waitForAMContainer(getApp(3), 0)); // would exceed am limit
    assertFalse(waitForAMContainer(getApp(3), 1)); // would exceed am limit

    assertTrue(finishApp(2));
    assertTrue(waitForAMContainer(getApp(3), 0));
  }

  @Test
  public void testLocality() throws Exception {
    startRM();
    registerNodes(10);

    submitAppToNode(1, 0, Locality.NODE_LOCAL);
    submitAppToNode(2, 5, Locality.NODE_LOCAL);
    submitAppToNode(3, 8, Locality.NODE_LOCAL);
    submitAppToNode(4, 0, Locality.NODE_LOCAL);
    submitAppToNode(5, 0, Locality.NODE_LOCAL);
    submitAppToNode(6, 0, Locality.RACK_LOCAL);
    submitAppToNode(7, 0, Locality.NODE_LOCAL);

    Thread.sleep(1000);

    for (int i = 0; i < 10; i++) {
      sendNodeUpdate(i);
    }

    assertThat(getAMNodeIndex(1), is(0));
    assertThat(getAMNodeIndex(2), is(5));
    assertThat(getAMNodeIndex(3), is(8));
    assertThat(getAMNodeIndex(4), is(0));
    assertThat(getAMNodeIndex(5), is(-1));
    assertThat(getAMNodeIndex(6), is(1));

    assertTrue(finishApp(4));

    Thread.sleep(1000);

    for (int i = 0; i < 10; i++) {
      sendNodeUpdate(i);
    }

    assertThat(getAMNodeIndex(5), is(0));
    assertThat(getAMNodeIndex(7), is(1));
  }
}

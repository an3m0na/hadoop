package org.apache.hadoop.tools.posum.scheduler.portfolio.edls;

import org.apache.hadoop.tools.posum.common.records.call.UpdateOrStoreCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.util.conf.PosumConfiguration;
import org.apache.hadoop.tools.posum.scheduler.portfolio.TestPolicyBase;
import org.apache.hadoop.tools.posum.test.IntegrationTest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;

import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.JOB;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@Category(IntegrationTest.class)
public class TestEDLSPriority extends TestPolicyBase {

  public TestEDLSPriority() {
    super(EDLSPriorityPolicy.class);
  }

  @Test
  public void smokeTest() throws Exception {
    conf.setFloat(PosumConfiguration.DC_PRIORITY, 0f); // unrestricted batches
    conf.setFloat(PosumConfiguration.MAX_AM_RATIO, 1.5f); // max 3 apps can be active from each queue (needed because they already have 50% each)

    startRM();
    registerNodes(2);

    submitApp(1);
    assertTrue(waitForAMContainer(getApp(1), 0));
    submitApp(2);
    assertTrue(waitForAMContainer(getApp(2), 0));

    submitApp(3);
    assertFalse(waitForAMContainer(getApp(3), 0));
    assertTrue(waitForAMContainer(getApp(3), 1));

    submitApp(4);
    assertFalse(waitForAMContainer(getApp(4), 0));
    assertFalse(waitForAMContainer(getApp(4), 1));

    assertThat(countAppsInQueue("batch"), is(4));
    assertThat(countAppsInQueue("deadline"), is(0));
    assertThat(countRMApps(), is(4));

    assertTrue(finishApp(1));
    assertTrue(waitForAMContainer(getApp(4), 0));
  }

  @Test
  public void testDeadlinesOnly() throws Exception {
    conf.setFloat(PosumConfiguration.DC_PRIORITY, 1f); // batches are allowed only if no dcs can be scheduled
    conf.setFloat(PosumConfiguration.MAX_AM_RATIO, 1.5f); // max 3 apps can be active from each queue (needed because they already have 50% each)
    startRM();
    registerNodes(2);

    submitApp(1, 10);
    assertTrue(waitForAMContainer(getApp(1), 0));
    submitApp(2);
    assertTrue(waitForAMContainer(getApp(2), 0));
    submitApp(3, 20);
    assertFalse(waitForAMContainer(getApp(3), 0));
    assertTrue(waitForAMContainer(getApp(3), 1));
    assertThat(countRMApps(), is(3));
    assertThat(countAppsInQueue("batch"), is(1));
    assertThat(countAppsInQueue("deadline"), is(2));

    submitApp(4);
    submitApp(5, 30);
    submitApp(6, 40);
    submitApp(7, 50);
    assertFalse(waitForAMContainer(getApp(4), 0));
    assertFalse(waitForAMContainer(getApp(4), 1));
    assertFalse(waitForAMContainer(getApp(5), 0));
    assertTrue(waitForAMContainer(getApp(5), 1));
    assertFalse(waitForAMContainer(getApp(6), 0));
    assertFalse(waitForAMContainer(getApp(6), 1));
    assertFalse(waitForAMContainer(getApp(7), 0));
    assertFalse(waitForAMContainer(getApp(7), 1));

    assertTrue(finishApp(1));
    // app 4 should not be allocated because there are still dc jobs waiting
    assertFalse(waitForAMContainer(getApp(4), 0));
    assertFalse(waitForAMContainer(getApp(4), 1));
    assertTrue(waitForAMContainer(getApp(6), 0));
    assertFalse(waitForAMContainer(getApp(7), 0));
    assertFalse(waitForAMContainer(getApp(7), 1));

    assertTrue(finishApp(3));
    // app 4 should not be allocated because there are still dc jobs have priority over the only slot available
    assertFalse(waitForAMContainer(getApp(4), 0));
    assertFalse(waitForAMContainer(getApp(4), 1));
    assertFalse(waitForAMContainer(getApp(7), 0));
    assertTrue(waitForAMContainer(getApp(7), 1));

    assertTrue(finishApp(6));
    // app 4 can now run
    assertFalse(waitForAMContainer(getApp(4), 1));
    assertTrue(waitForAMContainer(getApp(4), 0));

    assertThat(countRMApps(), is(7));
    assertThat(countAppsInQueue("batch"), is(2));
    assertThat(countAppsInQueue("deadline"), is(2));
  }

  @Test
  public void testBatchPriorities() throws YarnException, InterruptedException, IOException {
    conf.setFloat(PosumConfiguration.MAX_AM_RATIO, 0.5f); // each queue is allowed 1 active app (because they already have 50% each) => forced to be sequential
    startRM();
    registerNodes(2);

    submitApp(1);
    submitApp(2);
    submitApp(3);
    submitApp(4);

    assertFalse(waitForAMContainer(getApp(2), 1));
    assertFalse(waitForAMContainer(getApp(3), 1));
    assertFalse(waitForAMContainer(getApp(4), 1));
    assertTrue(waitForAMContainer(getApp(1), 1));

    JobProfile job = getJobForApp(2);
    job.setCompletedMaps(1);
    job.setAvgMapDuration(50000L);
    db.execute(UpdateOrStoreCall.newInstance(JOB, job));

    job = getJobForApp(3);
    job.setCompletedMaps(1);
    job.setAvgMapDuration(100000L);
    db.execute(UpdateOrStoreCall.newInstance(JOB, job));

    job = getJobForApp(4);
    job.setCompletedMaps(1);
    job.setAvgMapDuration(10000L);
    db.execute(UpdateOrStoreCall.newInstance(JOB, job));

    sendNodeUpdate(1);

    assertTrue(finishApp(1));
    assertFalse(waitForAMContainer(getApp(2), 1));
    assertFalse(waitForAMContainer(getApp(3), 1));
    assertTrue(waitForAMContainer(getApp(4), 1));

    assertTrue(finishApp(4));
    assertFalse(waitForAMContainer(getApp(3), 1));
    assertTrue(waitForAMContainer(getApp(2), 1));
  }

  @Test
  public void testDeadlinePriorities() throws YarnException, InterruptedException, IOException {
    conf.setFloat(PosumConfiguration.MAX_AM_RATIO, 0.5f); // each queue is allowed 1 active app (because they already have 50% each) => forced to be sequential
    startRM();
    registerNodes(2);

    long now = System.currentTimeMillis();

    submitApp(1, now + 10000);
    submitApp(2, now + 40000);
    submitApp(3, now + 100000);
    submitApp(4, now + 25000);

    assertFalse(waitForAMContainer(getApp(2), 1));
    assertFalse(waitForAMContainer(getApp(3), 1));
    assertFalse(waitForAMContainer(getApp(4), 1));
    assertTrue(waitForAMContainer(getApp(1), 1));

    assertTrue(finishApp(1));
    assertFalse(waitForAMContainer(getApp(2), 1));
    assertFalse(waitForAMContainer(getApp(3), 1));
    assertTrue(waitForAMContainer(getApp(4), 1));

    assertTrue(finishApp(4));
    assertFalse(waitForAMContainer(getApp(3), 1));
    assertTrue(waitForAMContainer(getApp(2), 1));
  }
}

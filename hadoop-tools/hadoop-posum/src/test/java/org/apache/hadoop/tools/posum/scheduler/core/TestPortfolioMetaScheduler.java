package org.apache.hadoop.tools.posum.scheduler.core;

import org.apache.hadoop.mapreduce.v2.api.records.Locality;
import org.apache.hadoop.tools.posum.common.records.call.UpdateOrStoreCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.util.cluster.InjectableResourceScheduler;
import org.apache.hadoop.tools.posum.common.util.conf.PosumConfiguration;
import org.apache.hadoop.tools.posum.scheduler.TestSchedulerBase;
import org.apache.hadoop.tools.posum.scheduler.portfolio.srtf.SRTFAppAttempt;
import org.apache.hadoop.tools.posum.test.IntegrationTest;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import java.io.IOException;

import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.JOB;
import static org.apache.hadoop.tools.posum.common.util.conf.PolicyPortfolio.StandardPolicy.EDLS_PR;
import static org.apache.hadoop.tools.posum.common.util.conf.PolicyPortfolio.StandardPolicy.EDLS_SH;
import static org.apache.hadoop.tools.posum.common.util.conf.PolicyPortfolio.StandardPolicy.LOCF;
import static org.apache.hadoop.tools.posum.common.util.conf.PolicyPortfolio.StandardPolicy.SRTF;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@Category(IntegrationTest.class)
public class TestPortfolioMetaScheduler extends TestSchedulerBase {
  private PortfolioMetaScheduler scheduler;

  @Override
  protected InjectableResourceScheduler initScheduler() {
    conf.setBoolean(PosumConfiguration.SCHEDULER_METRICS_ON, false);
    MetaSchedulerCommService mockCommService = Mockito.mock(MetaSchedulerCommService.class);
    when(mockCommService.getDatabase()).thenReturn(db);
    scheduler = new PortfolioMetaScheduler(conf, mockCommService);
    return new InjectableResourceScheduler<>(scheduler);
  }

  @Test
  public void smokeTest() throws Exception {
    defaultSmokeTest();
  }

  @Test
  public void changeEdlsPriorityToShareAndBack() throws Exception {
    conf.set(PosumConfiguration.DEFAULT_POLICY, EDLS_PR.name());
    conf.setFloat(PosumConfiguration.DC_PRIORITY, 0.999f); // deadlines always have priority
    conf.setFloat(PosumConfiguration.MAX_AM_RATIO, 1f); // max 2 apps can be active from each queue for EDLS_PR(because they already have 50% each), unlimited (max slots) apps for EDLS_SH
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
    // dc apps cannot not run because of exceeding AM quota, so app 4 can start
    assertFalse(waitForAMContainer(getApp(5), 0));
    assertFalse(waitForAMContainer(getApp(5), 1));
    assertFalse(waitForAMContainer(getApp(6), 0));
    assertFalse(waitForAMContainer(getApp(6), 1));
    assertFalse(waitForAMContainer(getApp(7), 0));
    assertFalse(waitForAMContainer(getApp(7), 1));
    assertFalse(waitForAMContainer(getApp(4), 0));
    assertTrue(waitForAMContainer(getApp(4), 1));

    assertTrue(finishApp(1));
    assertFalse(waitForAMContainer(getApp(5), 1));
    assertTrue(waitForAMContainer(getApp(5), 0));
    assertFalse(waitForAMContainer(getApp(6), 0));
    assertFalse(waitForAMContainer(getApp(6), 1));
    assertFalse(waitForAMContainer(getApp(7), 0));
    assertFalse(waitForAMContainer(getApp(7), 1));

    assertThat(countRMApps(), is(7));
    assertThat(countAppsInQueue("batch"), is(2));
    assertThat(countAppsInQueue("deadline"), is(4));

    scheduler.changeToPolicy(EDLS_SH.name());

    assertTrue(finishApp(4));

    submitApp(8);
    submitApp(9, 10);

    assertThat(countRMApps(), is(9));
    assertThat(countAppsInQueue("batch"), is(2));
    assertThat(countAppsInQueue("deadline"), is(5));

    // now app 8 cannot run before all batch apps are done
    // but now dc apps no longer have a quota
    // app 9 cannot should take precedence
    assertFalse(waitForAMContainer(getApp(6), 0));
    assertFalse(waitForAMContainer(getApp(6), 1));
    assertFalse(waitForAMContainer(getApp(7), 0));
    assertFalse(waitForAMContainer(getApp(7), 1));
    assertFalse(waitForAMContainer(getApp(8), 1));
    assertFalse(waitForAMContainer(getApp(8), 0));
    assertFalse(waitForAMContainer(getApp(9), 0));
    assertTrue(waitForAMContainer(getApp(9), 1));

    assertTrue(finishApp(3));

    // app 8 cannot run before all batch apps are done
    assertFalse(waitForAMContainer(getApp(8), 0));
    assertFalse(waitForAMContainer(getApp(8), 1));
    assertFalse(waitForAMContainer(getApp(6), 0));
    assertTrue(waitForAMContainer(getApp(6), 1));
    assertFalse(waitForAMContainer(getApp(7), 0));
    assertFalse(waitForAMContainer(getApp(7), 1));

    assertTrue(finishApp(5));

    scheduler.changeToPolicy(EDLS_PR.name());

    assertThat(countRMApps(), is(9));
    assertThat(countAppsInQueue("batch"), is(2));
    assertThat(countAppsInQueue("deadline"), is(3));

    // now app 8 can run because 7 would exceed dc quota
    assertFalse(waitForAMContainer(getApp(7), 0));
    assertFalse(waitForAMContainer(getApp(7), 1));
    assertFalse(waitForAMContainer(getApp(8), 1));
    assertTrue(waitForAMContainer(getApp(8), 0));

    // 7 would still exceed quota
    assertTrue(finishApp(2));
    assertFalse(waitForAMContainer(getApp(7), 0));
    assertFalse(waitForAMContainer(getApp(7), 1));

    assertTrue(finishApp(9));
    assertTrue(waitForAMContainer(getApp(7), 0));

    assertTrue(finishApp(6));
    assertTrue(finishApp(8));
    assertTrue(finishApp(7));

    assertThat(countRMApps(), is(9));
    assertThat(countAppsInQueue("batch"), is(0));
    assertThat(countAppsInQueue("deadline"), is(0));
  }

  @Test
  public void changeSrtfToLocfAndBack() throws YarnException, InterruptedException, IOException {
    conf.set(PosumConfiguration.DEFAULT_POLICY, SRTF.name());
    conf.setFloat(PosumConfiguration.MAX_AM_RATIO, 1f); // max 2 apps can be active from each queue for EDLS_PR(because they already have 50% each), unlimited (max slots) apps for EDLS_SH
    conf.setLong(PosumConfiguration.REPRIORITIZE_INTERVAL, 0); // always re-evaluate priorities

    startRM();
    registerNodes(10);

    submitApp(1);
    submitApp(2);
    submitApp(3);

    JobProfile job1 = getJobForApp(1);
    JobProfile job2 = getJobForApp(2);
    JobProfile job3 = getJobForApp(3);

    job1.setTotalMapTasks(1);
    job1.setTotalReduceTasks(1);
    job1.setCompletedMaps(1);
    job1.setAvgMapDuration(20000L);
    job1.setTotalSplitSize(10000000L);
    job1.setMapOutputBytes(50000L);

    job2.setTotalMapTasks(2);
    job2.setCompletedMaps(1);
    job2.setAvgMapDuration(16000000L);
    job2.setTotalSplitSize(50000000L);
    job2.setMapOutputBytes(2000L);

    job3.setTotalMapTasks(2);
    job3.setTotalReduceTasks(3);
    job3.setCompletedMaps(1);
    job3.setAvgMapDuration(300L);
    job3.setTotalSplitSize(9000L);
    job3.setMapOutputBytes(1200000L);

    db.execute(UpdateOrStoreCall.newInstance(JOB, job1));
    db.execute(UpdateOrStoreCall.newInstance(JOB, job2));
    db.execute(UpdateOrStoreCall.newInstance(JOB, job3));

    sendNodeUpdate(0);
    sendNodeUpdate(1);

    Thread.sleep(1000);

    assertThat(getAMNodeIndex(3), is(0));
    assertThat(getAMNodeIndex(2), is(0));
    assertThat(getAMNodeIndex(1), is(1));

    sendNodeUpdate(1);

    Thread.sleep(1000);

    double deficit1 = ((SRTFAppAttempt) getAttempt(1)).getResourceDeficit();
    double deficit2 = ((SRTFAppAttempt) getAttempt(2)).getResourceDeficit();
    double deficit3 = ((SRTFAppAttempt) getAttempt(3)).getResourceDeficit();

    assertThat(deficit1, lessThan(deficit3));
    assertThat(deficit3, lessThan(deficit2));

    Container container1 = requestAllocation(1);
    Container container2 = requestAllocation(2, 1, Locality.NODE_LOCAL);
    Container container3 = requestAllocation(3);

    scheduler.changeToPolicy(LOCF.name());

    sendNodeUpdate(1);
    sendNodeUpdate(2);

    Thread.sleep(1000);

    if (container1 == null)
      container1 = checkAllocation(getApp(1));
    if (container2 == null)
      container2 = checkAllocation(getApp(2));
    if (container3 == null)
      container3 = checkAllocation(getApp(3));

    assertThat(getNmIndex(container1), is(2));
    assertThat(getNmIndex(container2), is(1));
    assertThat(getNmIndex(container3), is(2));

    container1 = requestAllocation(1);
    container2 = requestAllocation(2, 3, Locality.NODE_LOCAL);
    container3 = requestAllocation(3);

    scheduler.changeToPolicy(SRTF.name());

    sendNodeUpdate(3);
    sendNodeUpdate(1); // is full so should not matter

    Thread.sleep(1000);

    deficit1 = ((SRTFAppAttempt) getAttempt(1)).getResourceDeficit();
    deficit2 = ((SRTFAppAttempt) getAttempt(2)).getResourceDeficit();
    deficit3 = ((SRTFAppAttempt) getAttempt(3)).getResourceDeficit();

    // stats should be the same
    assertThat(deficit1, lessThan(deficit3));
    assertThat(deficit3, lessThan(deficit2));

    if (container1 == null)
      container1 = checkAllocation(getApp(1));
    if (container2 == null)
      container2 = checkAllocation(getApp(2));
    if (container3 == null)
      container3 = checkAllocation(getApp(3));

    assertThat(getNmIndex(container1), is(3));
    assertThat(getNmIndex(container2), is(-1)); // app 2 no longer has priority
    assertThat(getNmIndex(container3), is(3));

    finishApp(1);
    Thread.sleep(1000);
    sendNodeUpdate(1);
    Thread.sleep(1000);

    assertThat(getNmIndex(checkAllocation(getApp(2))), is(1));
  }

  @Test
  public void testFullCircle() throws YarnException, InterruptedException, IOException {
    conf.set(PosumConfiguration.DEFAULT_POLICY, EDLS_PR.name());
    conf.setFloat(PosumConfiguration.DC_PRIORITY, 0.999f); // deadlines always have priority
    conf.setFloat(PosumConfiguration.MAX_AM_RATIO, 0.75f); // max 3 active apps per queue (or only 1 if EDLS_PR)
    conf.setLong(PosumConfiguration.REPRIORITIZE_INTERVAL, 0); // always re-evaluate priorities

    startRM();
    registerNodes(3);

    submitApp(1, 40);
    submitAppToNode(2, 2, Locality.NODE_LOCAL);
    submitApp(3, 10);
    submitAppToNode(4, 0, Locality.NODE_LOCAL);

    JobProfile job1 = getJobForApp(1);
    JobProfile job2 = getJobForApp(2);
    JobProfile job3 = getJobForApp(3);

    job1.setTotalMapTasks(1);
    job1.setTotalReduceTasks(1);
    job1.setCompletedMaps(1);
    job1.setAvgMapDuration(20000L);
    job1.setTotalSplitSize(10000000L);
    job1.setMapOutputBytes(50000L);

    job2.setTotalMapTasks(2);
    job2.setCompletedMaps(1);
    job2.setAvgMapDuration(16000000L);
    job2.setTotalSplitSize(50000000L);
    job2.setMapOutputBytes(2000L);

    job3.setTotalMapTasks(2);
    job3.setTotalReduceTasks(3);
    job3.setCompletedMaps(1);
    job3.setAvgMapDuration(300L);
    job3.setTotalSplitSize(9000L);
    job3.setMapOutputBytes(1200000L);

    db.execute(UpdateOrStoreCall.newInstance(JOB, job1));
    db.execute(UpdateOrStoreCall.newInstance(JOB, job2));
    db.execute(UpdateOrStoreCall.newInstance(JOB, job3));

    sendNodeUpdate(0);
    sendNodeUpdate(1);

    Thread.sleep(1000);

    assertThat(getAMNodeIndex(3), is(0)); // both dc and low deadline; should go first
    assertThat(getAMNodeIndex(1), is(1)); // would exceed dc limit
    assertThat(getAMNodeIndex(2), is(-1)); // not 0 because it was already assigned one off-switch container
    assertThat(getAMNodeIndex(4), is(-1)); // would exceed bc limit

    assertThat(countAppsInQueue("batch"), is(2));
    assertThat(countAppsInQueue("deadline"), is(2));
    assertThat(countRMApps(), is(4));

    scheduler.changeToPolicy(LOCF.name());

    assertThat(countAppsInQueue("default"), is(4));
    assertThat(countRMApps(), is(4));

    sendNodeUpdate(0);
    sendNodeUpdate(1);

    Thread.sleep(1000);

    assertThat(getAMNodeIndex(4), is(0)); // node local, so it has priority
    assertThat(getAMNodeIndex(2), is(-1)); // still waiting for node update of 2

    Container container1 = requestAllocation(1);
    Container container2 = checkAllocation(getApp(2));
    Container container3 = requestAllocation(3);

    scheduler.changeToPolicy(SRTF.name());

    assertThat(countAppsInQueue("default"), is(4));
    assertThat(countRMApps(), is(4));

    double deficit1 = ((SRTFAppAttempt) getAttempt(1)).getResourceDeficit();
    double deficit2 = ((SRTFAppAttempt) getAttempt(2)).getResourceDeficit();
    double deficit3 = ((SRTFAppAttempt) getAttempt(3)).getResourceDeficit();

    assertThat(deficit2, is(-1024.0));
    assertThat(deficit1, lessThan(deficit3));

    sendNodeUpdate(0);
    sendNodeUpdate(1);
    sendNodeUpdate(2);

    Thread.sleep(1000);

    if (container1 == null)
      container1 = checkAllocation(getApp(1));
    if (container2 == null)
      container2 = checkAllocation(getApp(2));
    if (container3 == null)
      container3 = checkAllocation(getApp(3));

    assertThat(getNmIndex(container1), is(1));
    assertThat(getNmIndex(container2), is(2));
    assertThat(getNmIndex(container3), is(2));

    scheduler.changeToPolicy(EDLS_SH.name());

    submitAppToNode(5, 2, Locality.NODE_LOCAL);

    assertThat(countAppsInQueue("batch"), is(3));
    assertThat(countAppsInQueue("deadline"), is(2));
    assertThat(countRMApps(), is(5));

    sendNodeUpdate(0);
    sendNodeUpdate(1);
    sendNodeUpdate(2);

    Thread.sleep(1000);

    assertThat(getAMNodeIndex(5), is(-1)); // every node is full

    finishApp(2);
    finishApp(1);

    Thread.sleep(1000);

    sendNodeUpdate(0);
    sendNodeUpdate(1);
    sendNodeUpdate(2);

    assertThat(countAppsInQueue("batch"), is(2));
    assertThat(countAppsInQueue("deadline"), is(1));

    assertThat(getAMNodeIndex(5), is(-1)); // locality doesn't matter bc limit would be exceeded

    scheduler.changeToPolicy(EDLS_PR.name());

    sendNodeUpdate(0);
    sendNodeUpdate(1);
    sendNodeUpdate(2);

    assertThat(getAMNodeIndex(5), is(1)); // now it can finally run

    assertThat(countAppsInQueue("batch"), is(2));
    assertThat(countAppsInQueue("deadline"), is(1));
    assertThat(countRMApps(), is(5));
  }
}

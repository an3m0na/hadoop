package org.apache.hadoop.tools.posum.scheduler.portfolio.srtf;

import org.apache.hadoop.tools.posum.common.records.call.UpdateOrStoreCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.util.PosumConfiguration;
import org.apache.hadoop.tools.posum.scheduler.portfolio.TestPolicyBase;
import org.apache.hadoop.tools.posum.test.IntegrationTest;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;

import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.JOB;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

@Category(IntegrationTest.class)
public class TestShortestRTFirstPolicy extends TestPolicyBase<ShortestRTFirstPolicy> {

  public TestShortestRTFirstPolicy() {
    super(ShortestRTFirstPolicy.class);
  }

  @Test
  public void smokeTest() throws Exception {
    defaultSmokeTest();
  }

  @Test
  public void testPriorities() throws YarnException, InterruptedException, IOException {
    conf.setFloat(PosumConfiguration.MAX_AM_RATIO, 1f); // unlimited active apps
    conf.setLong(PosumConfiguration.REPRIORITIZE_INTERVAL, 0); // unlimited active apps

    startRM();
    registerNodes(3);

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
    Container container2 = requestAllocation(2);
    Container container3 = requestAllocation(3);

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
  }

}

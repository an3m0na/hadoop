package org.apache.hadoop.tools.posum.scheduler.portfolio.srtf;

import org.apache.hadoop.tools.posum.scheduler.portfolio.singleq.SQSchedulerNode;
import org.apache.hadoop.tools.posum.test.Utils;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestSRTFAppAttempt {
  private static final Resource MINIMUM_ALLOCATION = Resource.newInstance(1, 1);
  private static final Resource CLUSTER_RESOURCE = Resource.newInstance(10, 10);
  private static final Priority PRIORITY = Priority.newInstance(1);
  private static final ResourceRequest AM_REQUEST = ResourceRequest.newInstance(PRIORITY, "*", MINIMUM_ALLOCATION, 1);
  private static final SQSchedulerNode NODE = mock(SQSchedulerNode.class);
  private static final Container CONTAINER = mock(Container.class, RETURNS_DEEP_STUBS);
  private static final RMContext RM_CONTEXT = mock(RMContext.class, RETURNS_DEEP_STUBS);
  private static final Queue QUEUE = mock(Queue.class);
  private static final ApplicationAttemptId ATTEMPT_ID = ApplicationAttemptId.newInstance(Utils.APP1_ID, 1);
  private static final ActiveUsersManager USERS_MANAGER = mock(ActiveUsersManager.class);
  private static final double NORMALIZER = 0.0075; // 1/400 + 1/30000 + 1/200

  @Before
  public void setUp() throws Exception {
    when(QUEUE.getMetrics()).thenReturn(mock(QueueMetrics.class));
    when(RM_CONTEXT.getRMApps()).thenReturn(new ConcurrentHashMap<ApplicationId, RMApp>());
    when(RM_CONTEXT.getScheduler().getMinimumResourceCapability()).thenReturn(MINIMUM_ALLOCATION);
    when(RM_CONTEXT.getDispatcher().getEventHandler()).thenReturn(mock(EventHandler.class));
    when(CONTAINER.getResource()).thenReturn(MINIMUM_ALLOCATION);
    when(CONTAINER.getId().getApplicationAttemptId()).thenReturn(ATTEMPT_ID);
  }

  @Test
  public void test() {
    SRTFAppAttempt subject = new SRTFAppAttempt(null, ATTEMPT_ID, "user", QUEUE, USERS_MANAGER, RM_CONTEXT);
    assertThat(subject.getRemainingTime(MINIMUM_ALLOCATION), nullValue()); // not yet initialized
    assertThat(subject.getResourceDeficit(), nullValue()); // default for not yet running apps
    long submitTime = System.currentTimeMillis();
    subject.setSubmitTime(submitTime - 1000);
    subject.setRemainingWork(200L);
    subject.setTotalWork(5000L);
    subject.calculateDeficit(MINIMUM_ALLOCATION, CLUSTER_RESOURCE, NORMALIZER);

    assertThat(subject.getRemainingTime(MINIMUM_ALLOCATION), nullValue()); // am is not yet running
    assertThat(subject.getResourceDeficit(), is(-1.0)); // default for not yet running apps

    subject.getAppSchedulingInfo().updateResourceRequests(Collections.singletonList(AM_REQUEST), false);
    subject.allocate(NodeType.OFF_SWITCH, NODE, PRIORITY, AM_REQUEST, CONTAINER);

    subject.calculateDeficit(MINIMUM_ALLOCATION, CLUSTER_RESOURCE, NORMALIZER);

    assertThat(subject.getRemainingTime(MINIMUM_ALLOCATION), is(200L));
    assertThat(subject.getResourceDeficit(), Matchers.closeTo(-16, 2));
  }
}

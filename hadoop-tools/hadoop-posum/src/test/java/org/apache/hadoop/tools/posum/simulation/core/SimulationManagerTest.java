package org.apache.hadoop.tools.posum.simulation.core;

import org.apache.hadoop.tools.posum.client.data.DataStore;
import org.apache.hadoop.tools.posum.common.records.payload.SimulationResultPayload;
import org.apache.hadoop.tools.posum.scheduler.portfolio.ShortestRTFirstPolicy;
import org.apache.hadoop.tools.posum.simulation.predictor.JobBehaviorPredictor;
import org.apache.hadoop.tools.posum.test.Utils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.Map;


public class SimulationManagerTest {
  private static final Class<? extends ResourceScheduler> SCHEDULER_CLASS = ShortestRTFirstPolicy.class;
  private static final String SCHEDULER_NAME= "SRTF";
  private static final Map<String, String> TOPOLOGY;
  static {
    TOPOLOGY = new HashMap<>(4);
    TOPOLOGY.put("node323.cm.cluster", "rack1");
    TOPOLOGY.put("node324.cm.cluster", "rack2");
    TOPOLOGY.put("node325.cm.cluster", "rack2");
    TOPOLOGY.put("node326.cm.cluster", "rack2");
  }

  private SimulationManager testSubject;

  private DataStore dataStoreMock;
  @Mock
  private JobBehaviorPredictor predictorMock;


  @Before
  public void init() {
    MockitoAnnotations.initMocks(this);
    dataStoreMock = Utils.mockDefaultWorkload();
    testSubject = new SimulationManager(predictorMock, SCHEDULER_NAME, SCHEDULER_CLASS, dataStoreMock, TOPOLOGY);
  }

  @Test
  public void callTest() throws Exception {
    SimulationResultPayload ret = testSubject.call();
    //TODO test intended behavior
  }


} 

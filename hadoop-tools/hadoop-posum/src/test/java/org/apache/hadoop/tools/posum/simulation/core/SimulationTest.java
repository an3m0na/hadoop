package org.apache.hadoop.tools.posum.simulation.core;

import org.apache.hadoop.tools.posum.client.data.DataStore;
import org.apache.hadoop.tools.posum.common.records.payload.SimulationResultPayload;
import org.apache.hadoop.tools.posum.simulation.predictor.JobBehaviorPredictor;
import org.apache.hadoop.tools.posum.test.Utils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;


public class SimulationTest {
  private Simulation testSubject;

  private DataStore dataStoreMock;
  @Mock
  private JobBehaviorPredictor predictorMock;


  @Before
  public void init() {
    MockitoAnnotations.initMocks(this);
    dataStoreMock = Utils.mockDefaultWorkload();
    testSubject = new Simulation(predictorMock, null, dataStoreMock);
  }

  @Test
  public void callTest() throws Exception {
    SimulationResultPayload ret = testSubject.call();
    //TODO test intended behavior
  }


} 

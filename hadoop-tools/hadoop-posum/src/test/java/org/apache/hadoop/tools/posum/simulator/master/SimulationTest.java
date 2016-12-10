package org.apache.hadoop.tools.posum.simulator.master; 

import org.apache.hadoop.tools.posum.common.records.payload.SimulationResultPayload;
import org.apache.hadoop.tools.posum.database.client.DataBroker;
import org.apache.hadoop.tools.posum.simulator.predictor.JobBehaviorPredictor;
import org.apache.hadoop.tools.posum.test.Utils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
 

public class SimulationTest { 
    private Simulation testSubject; 

    private DataBroker dataBrokerMock;
    @Mock
    private JobBehaviorPredictor predictorMock;
 

    @Before 
    public void init() { 
        MockitoAnnotations.initMocks(this);
        dataBrokerMock = Utils.mockDefaultWorkload();
        testSubject = new Simulation(predictorMock, null, dataBrokerMock);
    } 

    @Test
    public void callTest() throws Exception { 
        SimulationResultPayload ret = testSubject.call();
        //TODO test intended behavior
    }

 
} 

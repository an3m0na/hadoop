package org.apache.hadoop.tools.posum.simulation.core;

import org.apache.hadoop.tools.posum.client.data.DataStore;
import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.common.records.call.FindByQueryCall;
import org.apache.hadoop.tools.posum.common.records.call.StoreAllCall;
import org.apache.hadoop.tools.posum.common.records.call.TransactionCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.DatabaseReference;
import org.apache.hadoop.tools.posum.common.records.payload.SimulationResultPayload;
import org.apache.hadoop.tools.posum.data.mock.data.MockDataStoreImpl;
import org.apache.hadoop.tools.posum.scheduler.portfolio.ShortestRTFirstPolicy;
import org.apache.hadoop.tools.posum.simulation.predictor.JobBehaviorPredictor;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.APP;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.APP_HISTORY;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.COUNTER;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.COUNTER_HISTORY;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.JOB;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.JOB_CONF;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.JOB_CONF_HISTORY;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.JOB_HISTORY;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.TASK;
import static org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection.TASK_HISTORY;
import static org.apache.hadoop.tools.posum.test.Utils.JOB1;
import static org.apache.hadoop.tools.posum.test.Utils.JOB2;
import static org.apache.hadoop.tools.posum.test.Utils.NODE1;
import static org.apache.hadoop.tools.posum.test.Utils.NODE2;
import static org.apache.hadoop.tools.posum.test.Utils.RACK1;
import static org.apache.hadoop.tools.posum.test.Utils.TASK11;
import static org.apache.hadoop.tools.posum.test.Utils.TASK12;
import static org.apache.hadoop.tools.posum.test.Utils.TASK21;
import static org.apache.hadoop.tools.posum.test.Utils.TASK22;


public class SimulationManagerTest {
  private static final Class<? extends ResourceScheduler> SCHEDULER_CLASS = ShortestRTFirstPolicy.class;
  private static final String SCHEDULER_NAME= "SRTF";
  private static final Map<String, String> TOPOLOGY;
  static {
    TOPOLOGY = new HashMap<>(4);
//    TOPOLOGY.put("node323.cm.cluster", "rack1");
//    TOPOLOGY.put("node324.cm.cluster", "rack2");
//    TOPOLOGY.put("node325.cm.cluster", "rack2");
//    TOPOLOGY.put("node326.cm.cluster", "rack2");
    TOPOLOGY.put(NODE1, RACK1);
    TOPOLOGY.put(NODE2, RACK1);
  }

  private SimulationManager testSubject;

  private DataStore dataStoreMock;
  @Mock
  private JobBehaviorPredictor predictorMock;


  @Before
  public void init() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void simpleTest() throws Exception {
    dataStoreMock = new MockDataStoreImpl();
    Database sourceDb = Database.from(dataStoreMock, DatabaseReference.getSimulation());
    TransactionCall transaction = TransactionCall.newInstance()
      .addCall(StoreAllCall.newInstance(JOB, Arrays.asList(JOB1, JOB2)))
      .addCall(StoreAllCall.newInstance(TASK, Arrays.asList(TASK11, TASK12)))
      .addCall(StoreAllCall.newInstance(TASK, Arrays.asList(TASK21, TASK22)));
    sourceDb.execute(transaction);

    testSubject = new SimulationManager(predictorMock, SCHEDULER_NAME, SCHEDULER_CLASS, dataStoreMock, TOPOLOGY);

    SimulationResultPayload ret = testSubject.call();
    //TODO test intended behavior
    System.out.println("-----------------------Before-----------------");
    System.out.println(sourceDb.execute(FindByQueryCall.newInstance(APP, null)).getEntities());
    System.out.println(sourceDb.execute(FindByQueryCall.newInstance(JOB, null)).getEntities());
    System.out.println(sourceDb.execute(FindByQueryCall.newInstance(JOB_CONF, null)).getEntities());
    System.out.println(sourceDb.execute(FindByQueryCall.newInstance(TASK, null)).getEntities());
    System.out.println(sourceDb.execute(FindByQueryCall.newInstance(COUNTER, null)).getEntities());

    Database db = Database.from(dataStoreMock, DatabaseReference.get(DatabaseReference.Type.SIMULATION, SCHEDULER_NAME));
    System.out.println("-----------------------After-----------------");
    System.out.println(db.execute(FindByQueryCall.newInstance(APP_HISTORY, null)).getEntities());
    System.out.println(db.execute(FindByQueryCall.newInstance(JOB_HISTORY, null)).getEntities());
    System.out.println(db.execute(FindByQueryCall.newInstance(JOB_CONF_HISTORY, null)).getEntities());
    System.out.println(db.execute(FindByQueryCall.newInstance(TASK_HISTORY, null)).getEntities());
    System.out.println(db.execute(FindByQueryCall.newInstance(COUNTER_HISTORY, null)).getEntities());

  }


} 

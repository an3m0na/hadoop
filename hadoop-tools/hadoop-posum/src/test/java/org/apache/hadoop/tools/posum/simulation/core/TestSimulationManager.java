package org.apache.hadoop.tools.posum.simulation.core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.tools.posum.client.data.DataStore;
import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.common.records.call.FindByQueryCall;
import org.apache.hadoop.tools.posum.common.records.call.StoreAllCall;
import org.apache.hadoop.tools.posum.common.records.call.TransactionCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.DatabaseReference;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobConfProxy;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.dataentity.TaskProfile;
import org.apache.hadoop.tools.posum.common.records.payload.SimulationResultPayload;
import org.apache.hadoop.tools.posum.data.mock.data.MockDataStoreImpl;
import org.apache.hadoop.tools.posum.scheduler.portfolio.FifoPolicy;
import org.apache.hadoop.tools.posum.simulation.predictor.JobBehaviorPredictor;
import org.apache.hadoop.tools.posum.simulation.predictor.TaskPredictionInput;
import org.apache.hadoop.tools.posum.simulation.predictor.TaskPredictionOutput;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.util.Records;
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
import static org.apache.hadoop.tools.posum.test.Utils.DURATION_UNIT;
import static org.apache.hadoop.tools.posum.test.Utils.JOB1;
import static org.apache.hadoop.tools.posum.test.Utils.JOB2;
import static org.apache.hadoop.tools.posum.test.Utils.NODE1;
import static org.apache.hadoop.tools.posum.test.Utils.NODE2;
import static org.apache.hadoop.tools.posum.test.Utils.RACK1;
import static org.apache.hadoop.tools.posum.test.Utils.TASK11;
import static org.apache.hadoop.tools.posum.test.Utils.TASK12;
import static org.apache.hadoop.tools.posum.test.Utils.TASK21;
import static org.apache.hadoop.tools.posum.test.Utils.TASK22;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;


public class TestSimulationManager {
  private static final Class<? extends ResourceScheduler> SCHEDULER_CLASS = FifoPolicy.class;
  private static final String SCHEDULER_NAME = "FIFO";
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

    JobConfProxy jobConf1 = Records.newRecord(JobConfProxy.class);
    jobConf1.setId(JOB1.getId());
    Configuration innerConf = new Configuration();
    innerConf.setFloat(MRJobConfig.COMPLETED_MAPS_FOR_REDUCE_SLOWSTART, 0.8f);
    jobConf1.setConf(innerConf);

    JobConfProxy jobConf2 = jobConf1.copy();
    jobConf2.setId(JOB2.getId());

    TransactionCall transaction = TransactionCall.newInstance()
      .addCall(StoreAllCall.newInstance(JOB, Arrays.asList(JOB1, JOB2)))
      .addCall(StoreAllCall.newInstance(JOB_CONF, Arrays.asList(jobConf1, jobConf2)))
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

  @Test
  public void simplePredictionTest() throws Exception {
    dataStoreMock = new MockDataStoreImpl();
    Database sourceDb = Database.from(dataStoreMock, DatabaseReference.getSimulation());

    when(predictorMock.predictTaskBehavior(any(TaskPredictionInput.class)))
      .thenReturn(new TaskPredictionOutput(DURATION_UNIT * 2));

    JobProfile job1 = JOB1.copy(), job2 = JOB2.copy();
    job1.setAvgMapDuration(DURATION_UNIT * 5);
    job1.setTotalMapTasks(job1.getTotalMapTasks() + 1);
    job1.setCompletedMaps(1);
    job1.setInputBytes(10000L);
    job1.setMapOutputBytes(500L);
    job2.setAvgMapDuration(DURATION_UNIT);
    job2.setTotalMapTasks(job2.getTotalMapTasks() + 1);
    job2.setCompletedMaps(1);
    job2.setInputBytes(50000L);
    job2.setMapOutputBytes(30L);

    JobConfProxy jobConf1 = Records.newRecord(JobConfProxy.class);
    jobConf1.setId(job1.getId());
    Configuration innerConf = new Configuration();
    innerConf.setFloat(MRJobConfig.COMPLETED_MAPS_FOR_REDUCE_SLOWSTART, 0.8f);
    jobConf1.setConf(innerConf);

    JobConfProxy jobConf2 = jobConf1.copy();
    jobConf2.setId(job2.getId());

    TaskProfile task11 = TASK11.copy(), task12 = TASK12.copy(), task21 = TASK21.copy(), task22 = TASK22.copy();
    task11.setStartTime(null);
    task11.setFinishTime(null);
    task12.setStartTime(null);
    task12.setFinishTime(null);
    task21.setStartTime(null);
    task21.setFinishTime(null);
    task22.setStartTime(null);
    task22.setFinishTime(null);

    TransactionCall transaction = TransactionCall.newInstance()
      .addCall(StoreAllCall.newInstance(JOB, Arrays.asList(job1, job2)))
      .addCall(StoreAllCall.newInstance(JOB_CONF, Arrays.asList(jobConf1, jobConf2)))
      .addCall(StoreAllCall.newInstance(TASK, Arrays.asList(task11, task12)))
      .addCall(StoreAllCall.newInstance(TASK, Arrays.asList(task21, task22)));
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

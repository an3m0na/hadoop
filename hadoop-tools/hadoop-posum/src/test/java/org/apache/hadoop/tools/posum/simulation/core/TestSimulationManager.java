package org.apache.hadoop.tools.posum.simulation.core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.util.MRBuilderUtils;
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
import org.apache.hadoop.tools.posum.common.util.conf.PolicyPortfolio;
import org.apache.hadoop.tools.posum.common.util.conf.PosumConfiguration;
import org.apache.hadoop.tools.posum.data.core.DataStoreImpl;
import org.apache.hadoop.tools.posum.data.mock.data.MockDataStoreImpl;
import org.apache.hadoop.tools.posum.scheduler.portfolio.PluginPolicy;
import org.apache.hadoop.tools.posum.scheduler.portfolio.edls.EDLSSharePolicy;
import org.apache.hadoop.tools.posum.simulation.predictor.JobBehaviorPredictor;
import org.apache.hadoop.tools.posum.simulation.predictor.TaskPredictionInput;
import org.apache.hadoop.tools.posum.simulation.predictor.TaskPredictionOutput;
import org.apache.hadoop.tools.posum.simulation.predictor.optimal.OptimalPredictor;
import org.apache.hadoop.tools.posum.test.IntegrationTest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.net.NetworkTopology.DEFAULT_RACK;
import static org.apache.hadoop.tools.posum.client.data.DatabaseUtils.ID_FIELD;
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
import static org.apache.hadoop.tools.posum.common.util.conf.PolicyPortfolio.StandardPolicy.SRTF;
import static org.apache.hadoop.tools.posum.test.Utils.APP1;
import static org.apache.hadoop.tools.posum.test.Utils.APP2;
import static org.apache.hadoop.tools.posum.test.Utils.DURATION_UNIT;
import static org.apache.hadoop.tools.posum.test.Utils.JOB1;
import static org.apache.hadoop.tools.posum.test.Utils.JOB1_ID;
import static org.apache.hadoop.tools.posum.test.Utils.JOB2;
import static org.apache.hadoop.tools.posum.test.Utils.NODE1;
import static org.apache.hadoop.tools.posum.test.Utils.NODE2;
import static org.apache.hadoop.tools.posum.test.Utils.TASK11;
import static org.apache.hadoop.tools.posum.test.Utils.TASK12;
import static org.apache.hadoop.tools.posum.test.Utils.TASK21;
import static org.apache.hadoop.tools.posum.test.Utils.TASK22;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.number.IsCloseTo.closeTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;


@RunWith(MockitoJUnitRunner.class)
@Category(IntegrationTest.class)
public class TestSimulationManager {
  private static final Class<? extends PluginPolicy> SCHEDULER_CLASS = EDLSSharePolicy.class;
  private static final String SCHEDULER_NAME = "EDLS_SH";
  private static final Map<String, String> SMALL_TOPOLOGY;
  private static final Map<String, String> REAL_TOPOLOGY;
  private static final Configuration EMPTY_CONF = PosumConfiguration.newInstance();
  private static final Configuration REAL_CONF = PosumConfiguration.newInstance();

  static {
    SMALL_TOPOLOGY = new HashMap<>(2);
    SMALL_TOPOLOGY.put(NODE1, DEFAULT_RACK);
    SMALL_TOPOLOGY.put(NODE2, DEFAULT_RACK);

    REAL_TOPOLOGY = new HashMap<>(8);
    REAL_TOPOLOGY.put("node409.cm.cluster", DEFAULT_RACK);
    REAL_TOPOLOGY.put("node410.cm.cluster", DEFAULT_RACK);
    REAL_TOPOLOGY.put("node411.cm.cluster", DEFAULT_RACK);
    REAL_TOPOLOGY.put("node412.cm.cluster", DEFAULT_RACK);
    REAL_TOPOLOGY.put("node413.cm.cluster", DEFAULT_RACK);
    REAL_TOPOLOGY.put("node414.cm.cluster", DEFAULT_RACK);
    REAL_TOPOLOGY.put("node415.cm.cluster", DEFAULT_RACK);
    REAL_TOPOLOGY.put("node416.cm.cluster", DEFAULT_RACK);

    REAL_CONF.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 3072);
    REAL_CONF.setInt(YarnConfiguration.NM_VCORES, 16);
    REAL_CONF.setInt(YarnConfiguration.NM_PMEM_MB, 49152);
    REAL_CONF.setDouble(YarnConfiguration.NM_VMEM_PMEM_RATIO, 2.1);
  }

  private SimulationManager testSubject;

  private DataStore dataStoreMock;
  @Mock
  private JobBehaviorPredictor predictorMock;

  @Test
  public void testSmallTrace() throws Exception {
    dataStoreMock = new MockDataStoreImpl();
    Database sourceDb = Database.from(dataStoreMock, DatabaseReference.getSimulation());

    JobProfile job1 = JOB1.copy();
    job1.setDeadline(0L);
    JobProfile job2 = JOB2.copy();
    job2.setDeadline(0L);

    JobConfProxy jobConf1 = Records.newRecord(JobConfProxy.class);
    jobConf1.setId(job1.getId());
    Configuration innerConf = new Configuration();
    innerConf.setFloat(MRJobConfig.COMPLETED_MAPS_FOR_REDUCE_SLOWSTART, 0.8f);
    jobConf1.setConf(innerConf);

    JobConfProxy jobConf2 = jobConf1.copy();
    jobConf2.setId(job2.getId());

    TransactionCall transaction = TransactionCall.newInstance()
      .addCall(StoreAllCall.newInstance(APP, Arrays.asList(APP1, APP2)))
      .addCall(StoreAllCall.newInstance(JOB, Arrays.asList(job1, job2)))
      .addCall(StoreAllCall.newInstance(JOB_CONF, Arrays.asList(jobConf1, jobConf2)))
      .addCall(StoreAllCall.newInstance(TASK, Arrays.asList(TASK11, TASK12, TASK21, TASK22)));
    sourceDb.execute(transaction);


    testSubject = new SimulationManager<>(EMPTY_CONF, predictorMock, SCHEDULER_NAME, SCHEDULER_CLASS, dataStoreMock, SMALL_TOPOLOGY, false);

    SimulationResultPayload ret = testSubject.call();
    System.out.println("-----------------------Before-----------------");
    System.out.println(sourceDb.execute(FindByQueryCall.newInstance(APP, null, ID_FIELD, false)).getEntities());
    System.out.println(sourceDb.execute(FindByQueryCall.newInstance(JOB, null, ID_FIELD, false)).getEntities());
    System.out.println(sourceDb.execute(FindByQueryCall.newInstance(TASK, null, ID_FIELD, false)).getEntities());
    System.out.println(sourceDb.execute(FindByQueryCall.newInstance(JOB_CONF, null, ID_FIELD, false)).getEntities());
    System.out.println(sourceDb.execute(FindByQueryCall.newInstance(COUNTER, null, ID_FIELD, false)).getEntities());

    Database db = Database.from(dataStoreMock, DatabaseReference.get(DatabaseReference.Type.SIMULATION, SCHEDULER_NAME));
    System.out.println("-----------------------After-----------------");
    System.out.println(db.execute(FindByQueryCall.newInstance(APP_HISTORY, null, ID_FIELD, false)).getEntities());
    List<JobProfile> jobs = db.execute(FindByQueryCall.newInstance(JOB_HISTORY, null, ID_FIELD, false)).getEntities();
    System.out.println(jobs);
    List<TaskProfile> tasks = db.execute(FindByQueryCall.newInstance(TASK_HISTORY, null, ID_FIELD, false)).getEntities();
    System.out.println(tasks);
    System.out.println(db.execute(FindByQueryCall.newInstance(JOB_CONF_HISTORY, null, ID_FIELD, false)).getEntities());
    System.out.println(db.execute(FindByQueryCall.newInstance(COUNTER_HISTORY, null, ID_FIELD, false)).getEntities());

    assertThat(jobs.get(0).getStartTime().doubleValue(), closeTo(2000, 1001));
    assertThat(jobs.get(0).getFinishTime().doubleValue(), closeTo(308000, 2001));
    assertThat(jobs.get(1).getStartTime().doubleValue(), closeTo(62000, 1001));
    assertThat(jobs.get(1).getFinishTime().doubleValue(), closeTo(186000, 2001));

    assertThat(tasks.get(0).getStartTime().doubleValue(), closeTo(4000, 2001));
    assertThat(tasks.get(0).getFinishTime().doubleValue(), closeTo(186000, 2001));
    assertThat(tasks.get(1).getStartTime().doubleValue(), closeTo(186000, 2001));
    assertThat(tasks.get(1).getFinishTime().doubleValue(), closeTo(308000, 2001));
    assertThat(tasks.get(2).getStartTime().doubleValue(), closeTo(64000, 2001));
    assertThat(tasks.get(2).getFinishTime().doubleValue(), closeTo(126000, 2001));
    assertThat(tasks.get(3).getStartTime().doubleValue(), closeTo(64000, 2001));
    assertThat(tasks.get(3).getFinishTime().doubleValue(), closeTo(186000, 2001));
  }

  @Test
  public void testMockPrediction() throws Exception {
    dataStoreMock = new MockDataStoreImpl();
    Database sourceDb = Database.from(dataStoreMock, DatabaseReference.getSimulation());

    when(predictorMock.predictTaskBehavior(any(TaskPredictionInput.class)))
      .thenReturn(new TaskPredictionOutput(DURATION_UNIT * 2));

    JobProfile job1 = JOB1.copy();
    job1.setDeadline(0L);
    job1.setFinishTime(null);
    job1.setTotalReduceTasks(2);
    job1.setCompletedMaps(1);

    JobProfile job2 = JOB2.copy();
    job2.setSubmitTime(job1.getStartTime());
    job2.setDeadline(0L);
    job2.setStartTime(null);
    job2.setFinishTime(null);
    job2.setHostName(null);

    JobConfProxy jobConf1 = Records.newRecord(JobConfProxy.class);
    jobConf1.setId(job1.getId());
    Configuration innerConf = new Configuration();
    innerConf.setFloat(MRJobConfig.COMPLETED_MAPS_FOR_REDUCE_SLOWSTART, 0.8f);
    jobConf1.setConf(innerConf);

    JobConfProxy jobConf2 = jobConf1.copy();
    jobConf2.setId(job2.getId());

    TaskProfile task11 = TASK11.copy(), task12 = TASK12.copy(), task13 = TASK12.copy(), task21 = TASK21.copy(), task22 = TASK22.copy();
    task11.setFinishTime(job1.getStartTime() + DURATION_UNIT);
    task12.setStartTime(null);
    task12.setFinishTime(null);
    task13.setStartTime(job1.getStartTime());
    task13.setId(MRBuilderUtils.newTaskId(JOB1_ID, 3, TaskType.REDUCE).toString());
    task13.setHostName(NODE2);
    task13.setFinishTime(null);
    task12.setHostName(null);
    task21.setStartTime(null);
    task21.setFinishTime(null);
    task21.setHostName(null);
    task22.setStartTime(null);
    task22.setFinishTime(null);
    task22.setHostName(null);


    TransactionCall transaction = TransactionCall.newInstance()
      .addCall(StoreAllCall.newInstance(JOB, Arrays.asList(job1, job2)))
      .addCall(StoreAllCall.newInstance(JOB_CONF, Arrays.asList(jobConf1, jobConf2)))
      .addCall(StoreAllCall.newInstance(TASK, Arrays.asList(task11, task12, task13, task21, task22)));
    sourceDb.execute(transaction);

    testSubject = new SimulationManager<>(EMPTY_CONF, predictorMock, SCHEDULER_NAME, SCHEDULER_CLASS, dataStoreMock, SMALL_TOPOLOGY, true);

    SimulationResultPayload ret = testSubject.call();
    System.out.println("-----------------------Before-----------------");
    System.out.println(sourceDb.execute(FindByQueryCall.newInstance(APP, null, ID_FIELD, false)).getEntities());
    System.out.println(sourceDb.execute(FindByQueryCall.newInstance(JOB, null, ID_FIELD, false)).getEntities());
    System.out.println(sourceDb.execute(FindByQueryCall.newInstance(TASK, null, ID_FIELD, false)).getEntities());
    System.out.println(sourceDb.execute(FindByQueryCall.newInstance(JOB_CONF, null, ID_FIELD, false)).getEntities());
    System.out.println(sourceDb.execute(FindByQueryCall.newInstance(COUNTER, null, ID_FIELD, false)).getEntities());

    Database db = Database.from(dataStoreMock, DatabaseReference.get(DatabaseReference.Type.SIMULATION, SCHEDULER_NAME));
    System.out.println("-----------------------After-----------------");
    System.out.println(db.execute(FindByQueryCall.newInstance(APP_HISTORY, null, ID_FIELD, false)).getEntities());
    List<JobProfile> jobs = db.execute(FindByQueryCall.newInstance(JOB_HISTORY, null, ID_FIELD, false)).getEntities();
    System.out.println(jobs);
    List<TaskProfile> tasks = db.execute(FindByQueryCall.newInstance(TASK_HISTORY, null, ID_FIELD, false)).getEntities();
    System.out.println(tasks);
    System.out.println(db.execute(FindByQueryCall.newInstance(JOB_CONF_HISTORY, null, ID_FIELD, false)).getEntities());
    System.out.println(db.execute(FindByQueryCall.newInstance(COUNTER_HISTORY, null, ID_FIELD, false)).getEntities());

    assertThat(jobs.get(0).getStartTime(), is(0L));
    assertThat(jobs.get(0).getFinishTime().doubleValue(), closeTo(182000, 2001));
    assertThat(jobs.get(0).getCompletedMaps(), is(1));
    assertThat(jobs.get(0).getCompletedReduces(), is(2));
    assertThat(jobs.get(1).getStartTime().doubleValue(), closeTo(62000, 1001));
    assertThat(jobs.get(1).getFinishTime().doubleValue(), closeTo(182000, 2001));
    assertThat(jobs.get(1).getCompletedMaps(), is(2));
    assertThat(jobs.get(1).getCompletedReduces(), is(0));

    assertThat(tasks.get(0).getStartTime(), is(0L));
    assertThat(tasks.get(0).getFinishTime(), is(60000L));
    assertThat(tasks.get(1).getStartTime().doubleValue(), closeTo(62000, 2001L));
    assertThat(tasks.get(1).getFinishTime().doubleValue(), closeTo(182000, 2001));
    assertThat(tasks.get(2).getStartTime(), is(0L));
    assertThat(tasks.get(2).getFinishTime().doubleValue(), closeTo(122000, 2001));
    assertThat(tasks.get(3).getStartTime().doubleValue(), closeTo(62000, 2001L));
    assertThat(tasks.get(3).getFinishTime().doubleValue(), closeTo(182000, 2001));
    assertThat(tasks.get(4).getStartTime().doubleValue(), closeTo(62000, 2001));
    assertThat(tasks.get(4).getFinishTime().doubleValue(), closeTo(182000, 2001));
  }

  @Test
  @Ignore
  public void testOnRealDB() throws Exception {
    DataStore dataStore = new DataStoreImpl(REAL_CONF);
    JobBehaviorPredictor predictor = realPredictor(dataStore);

    PolicyPortfolio.StandardPolicy policy = SRTF;
    SimulationManager<? extends PluginPolicy> simulationManager = new SimulationManager<>(REAL_CONF, predictor, policy.name(), policy.getImplClass(), dataStore, REAL_TOPOLOGY, true);
    System.out.println(simulationManager.call());
  }

  private JobBehaviorPredictor realPredictor(DataStore dataStore) {
    JobBehaviorPredictor predictor = new OptimalPredictor(REAL_CONF);
    predictor.train(Database.from(dataStore, DatabaseReference.getMain()));
    predictor.switchDatabase(Database.from(dataStore, DatabaseReference.getSimulation()));
    return predictor;
  }

  @Test
  @Ignore
  public void testMultipleSimulationsOnRealDB() throws InterruptedException {
    DataStore dataStore = new DataStoreImpl(REAL_CONF);
    JobBehaviorPredictor predictor = realPredictor(dataStore);
    PolicyPortfolio portfolio = new PolicyPortfolio(REAL_CONF);

    List<PendingResult> simulations = new ArrayList<>(portfolio.size());
    final ExecutorService executor = Executors.newFixedThreadPool(portfolio.size());

    for (Map.Entry<String, Class<? extends PluginPolicy>> policy : portfolio.entrySet()) {
      System.out.println("Starting simulation for " + policy.getKey());
      Class<? extends PluginPolicy> policyClass = policy.getValue();
      SimulationManager<? extends PluginPolicy> simulation = new SimulationManager<>(REAL_CONF, predictor, policy.getKey(), policyClass, dataStore, REAL_TOPOLOGY, true);
      simulations.add(new PendingResult(simulation, executor.submit(simulation)));
    }

    ResultAggregator resultAggregator = new ResultAggregator(simulations, new SimulationResultHandler() {
      @Override
      public void simulationsDone(List<SimulationResultPayload> finalResults) {
        System.out.println(finalResults);
        executor.shutdown();
      }
    });

    executor.execute(resultAggregator);
    executor.awaitTermination(1, TimeUnit.HOURS);
  }
} 

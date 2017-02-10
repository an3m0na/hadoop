package org.apache.hadoop.tools.posum.data.monitor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.client.data.DataStore;
import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.common.records.call.CallUtils;
import org.apache.hadoop.tools.posum.common.records.call.FindByQueryCall;
import org.apache.hadoop.tools.posum.common.records.call.IdsByQueryCall;
import org.apache.hadoop.tools.posum.common.records.call.StoreLogCall;
import org.apache.hadoop.tools.posum.common.records.call.query.QueryUtils;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.DatabaseReference;
import org.apache.hadoop.tools.posum.common.records.dataentity.LogEntry;
import org.apache.hadoop.tools.posum.common.records.payload.PolicyInfoMapPayload;
import org.apache.hadoop.tools.posum.common.records.payload.PolicyInfoPayload;
import org.apache.hadoop.tools.posum.common.records.payload.SimplePropertyPayload;
import org.apache.hadoop.tools.posum.common.records.payload.TaskPredictionPayload;
import org.apache.hadoop.tools.posum.common.util.PolicyPortfolio;
import org.apache.hadoop.tools.posum.common.util.PosumConfiguration;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.tools.posum.simulation.predictor.BasicPredictor;
import org.apache.hadoop.tools.posum.simulation.predictor.DetailedPredictor;
import org.apache.hadoop.tools.posum.simulation.predictor.JobBehaviorPredictor;
import org.apache.hadoop.tools.posum.simulation.predictor.StandardPredictor;
import org.apache.hadoop.tools.posum.simulation.predictor.TaskPredictionInput;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PosumInfoCollector {

  private static Log logger = LogFactory.getLog(PosumInfoCollector.class);

  private final PosumAPIClient api;
  private final DataStore dataStore;
  private final Configuration conf;
  private final Boolean fineGrained;
  private final Map<String, PolicyInfoPayload> policyMap;
  private Long lastCollectTime = 0L;
  private Long lastPrediction = 0L;
  private Long predictionTimeout = 0L;
  private Long schedulingStart = 0L;
  private String lastUsedPolicy;
  //TODO use only this predictor for regular experiments
  //    private JobBehaviorPredictor predictor;
  private JobBehaviorPredictor basicPredictor;
  private JobBehaviorPredictor standardPredictor;
  private JobBehaviorPredictor detailedPredictor;


  public PosumInfoCollector(Configuration conf, DataStore dataStore) {
    this.dataStore = dataStore;
    this.conf = conf;
    fineGrained = conf.getBoolean(PosumConfiguration.FINE_GRAINED_MONITOR,
      PosumConfiguration.FINE_GRAINED_MONITOR_DEFAULT);
    api = new PosumAPIClient(conf);
    Set<String> policies = new PolicyPortfolio(conf).keySet();
    policyMap = new HashMap<>(policies.size());
    for (String policy : policies) {
      policyMap.put(policy, PolicyInfoPayload.newInstance());
    }
    Database db = Database.from(dataStore, DatabaseReference.getMain());
//        predictor = JobBehaviorPredictor.newInstance(conf);
    basicPredictor = JobBehaviorPredictor.newInstance(conf, BasicPredictor.class);
    basicPredictor.initialize(db);
    standardPredictor = JobBehaviorPredictor.newInstance(conf, StandardPredictor.class);
    standardPredictor.initialize(db);
    detailedPredictor = JobBehaviorPredictor.newInstance(conf, DetailedPredictor.class);
    detailedPredictor.initialize(db);
    predictionTimeout = conf.getLong(PosumConfiguration.PREDICTOR_TIMEOUT,
      PosumConfiguration.PREDICTOR_TIMEOUT_DEFAULT);
  }

  void refresh() {
    long now = System.currentTimeMillis();
    if (fineGrained) {
      //TODO get metrics from all services and persist to database
      if (now - lastPrediction > predictionTimeout) {
        // make new predictions
        IdsByQueryCall getAllTasks = IdsByQueryCall.newInstance(DataEntityCollection.TASK, null);
        List<String> taskIds = dataStore.executeDatabaseCall(getAllTasks, DatabaseReference.getMain()).getEntries();
        for (String taskId : taskIds) {
          // prediction can throw exception if data model changes state during calculation
//                    storePredictionForTask(predictor, taskId);
          storePredictionForTask(basicPredictor, taskId);
          storePredictionForTask(standardPredictor, taskId);
          storePredictionForTask(detailedPredictor, taskId);
        }
        lastPrediction = now;
      }
    }

    // aggregate policy change decisions
    FindByQueryCall findChoices = FindByQueryCall.newInstance(LogEntry.Type.POLICY_CHANGE.getCollection(),
      QueryUtils.and(
        QueryUtils.is("type", LogEntry.Type.POLICY_CHANGE),
        QueryUtils.greaterThan("lastUpdated", lastCollectTime),
        QueryUtils.lessThanOrEqual("lastUpdated", now)
      ));
    List<LogEntry<SimplePropertyPayload>> policyChanges =
      dataStore.executeDatabaseCall(findChoices, DatabaseReference.getLogs()).getEntities();
    if (policyChanges.size() > 0) {
      if (schedulingStart == 0)
        schedulingStart = policyChanges.get(0).getLastUpdated();
      for (LogEntry<SimplePropertyPayload> change : policyChanges) {
        String policy = (String) change.getDetails().getValue();
        PolicyInfoPayload info = policyMap.get(policy);
        if (!policy.equals(lastUsedPolicy)) {
          if (lastUsedPolicy != null) {
            policyMap.get(lastUsedPolicy).stop(change.getLastUpdated());
          }
          lastUsedPolicy = policy;
        }
        info.start(change.getLastUpdated());
      }
      dataStore.executeDatabaseCall(CallUtils.storeStatReportCall(LogEntry.Type.POLICY_MAP,
        PolicyInfoMapPayload.newInstance(policyMap)), null);
    }
    lastCollectTime = now;
  }

  private void storePredictionForTask(JobBehaviorPredictor predictor, String taskId) {
    try {
      TaskPredictionPayload prediction = TaskPredictionPayload.newInstance(
        predictor.getClass().getSimpleName(),
        taskId,
        predictor.predictTaskDuration(new TaskPredictionInput(taskId)).getDuration()
      );
      StoreLogCall storePrediction = CallUtils.storeStatReportCall(LogEntry.Type.TASK_PREDICTION, prediction);
      dataStore.executeDatabaseCall(storePrediction, null);
    } catch (Exception e) {
      if (!(e instanceof PosumException))
        logger.error("Could not predict task duration for " + taskId + " due to: ", e);
      else if (!e.getMessage().startsWith("Task has already finished"))
        logger.debug("Could not predict task duration for " + taskId + " due to: ", e);
    }
  }

}

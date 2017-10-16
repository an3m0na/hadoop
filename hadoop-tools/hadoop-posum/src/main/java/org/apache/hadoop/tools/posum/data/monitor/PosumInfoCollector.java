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
import org.apache.hadoop.tools.posum.common.records.payload.StringListPayload;
import org.apache.hadoop.tools.posum.common.records.payload.StringStringMapPayload;
import org.apache.hadoop.tools.posum.common.records.payload.TaskPredictionPayload;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.tools.posum.common.util.Utils;
import org.apache.hadoop.tools.posum.common.util.conf.PolicyPortfolio;
import org.apache.hadoop.tools.posum.common.util.conf.PosumConfiguration;
import org.apache.hadoop.tools.posum.simulation.predictor.JobBehaviorPredictor;
import org.apache.hadoop.tools.posum.simulation.predictor.TaskPredictionInput;
import org.apache.hadoop.tools.posum.simulation.predictor.basic.BasicPredictor;
import org.apache.hadoop.tools.posum.simulation.predictor.detailed.DetailedPredictor;
import org.apache.hadoop.tools.posum.simulation.predictor.standard.StandardPredictor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.tools.posum.common.records.dataentity.LogEntry.Type.CLUSTER_METRICS;
import static org.apache.hadoop.tools.posum.common.records.dataentity.LogEntry.Type.SYSTEM_METRICS;

public class PosumInfoCollector {

  private static Log logger = LogFactory.getLog(PosumInfoCollector.class);

  private final PosumAPIClient api;
  private final Database logDb;
  private final DataStore dataStore;
  private final Configuration conf;
  private final Map<String, PolicyInfoPayload> policyMap;
  private final Set<String> activeNodes;
  private Long lastCollectTime = 0L;
  private Long lastPrediction = 0L;
  private Long predictionTimeout = 0L;
  private Long schedulingStart = 0L;
  private String lastUsedPolicy;
  private boolean fineGrained;
  private boolean continuousPrediction;
  private JobBehaviorPredictor basicPredictor;
  private JobBehaviorPredictor standardPredictor;
  private JobBehaviorPredictor detailedPredictor;

  public PosumInfoCollector(Configuration conf, DataStore dataStore) {
    this.dataStore = dataStore;
    this.logDb = Database.from(dataStore, DatabaseReference.getLogs());
    this.conf = conf;
    fineGrained = conf.getBoolean(PosumConfiguration.FINE_GRAINED_MONITOR,
      PosumConfiguration.FINE_GRAINED_MONITOR_DEFAULT);
    continuousPrediction = conf.getBoolean(PosumConfiguration.CONTINUOUS_PREDICTION,
      PosumConfiguration.CONTINUOUS_PREDICTION_DEFAULT);
    api = new PosumAPIClient(conf);
    policyMap = new HashMap<>();
    initializePolicyMap();
    activeNodes = new HashSet<>();
    basicPredictor = JobBehaviorPredictor.newInstance(conf, BasicPredictor.class);
    standardPredictor = JobBehaviorPredictor.newInstance(conf, StandardPredictor.class);
    detailedPredictor = JobBehaviorPredictor.newInstance(conf, DetailedPredictor.class);
    predictionTimeout = conf.getLong(PosumConfiguration.PREDICTOR_TIMEOUT,
      PosumConfiguration.PREDICTOR_TIMEOUT_DEFAULT);
  }

  private void initializePolicyMap() {
    Set<String> policies = new PolicyPortfolio(conf).keySet();
    for (String policy : policies) {
      policyMap.put(policy, PolicyInfoPayload.newInstance());
    }
  }

  synchronized void collect() {
    long now = System.currentTimeMillis();

    if(fineGrained) {
      Map<String, String> systemMetrics = new HashMap<>(4);
      for (Utils.PosumProcess posumProcess : Utils.PosumProcess.values()) {
        String response = api.getSystemMetrics(posumProcess);
        if (response != null)
          systemMetrics.put(posumProcess.name(), response);
      }
      if (!systemMetrics.isEmpty()) {
        LogEntry systemMetricsLog = CallUtils.newLogEntry(SYSTEM_METRICS, StringStringMapPayload.newInstance(systemMetrics));
        logDb.execute(StoreLogCall.newInstance(systemMetricsLog));
      }
      String response = api.getClusterMetrics();
      if (response != null) {
        LogEntry clusterMetricsLog = CallUtils.newLogEntry(CLUSTER_METRICS, SimplePropertyPayload.newInstance("", response));
        logDb.execute(StoreLogCall.newInstance(clusterMetricsLog));
      }
    }

    if (continuousPrediction) {
      if (now - lastPrediction > predictionTimeout) {
        // make new predictions
        Database db = Database.from(dataStore, DatabaseReference.getMain());
        basicPredictor.train(db);
        standardPredictor.train(db);
        detailedPredictor.train(db);

        IdsByQueryCall getAllTasks = IdsByQueryCall.newInstance(DataEntityCollection.TASK, null);
        List<String> taskIds = dataStore.execute(getAllTasks, DatabaseReference.getMain()).getEntries();
        for (String taskId : taskIds) {
          // prediction can throw exception if data model changes state during calculation
          storePredictionForTask(basicPredictor, taskId);
          storePredictionForTask(standardPredictor, taskId);
          storePredictionForTask(detailedPredictor, taskId);
        }
        lastPrediction = now;
      }
    }

    // aggregate policy change decisions
    FindByQueryCall findPolicyChanges = FindByQueryCall.newInstance(DataEntityCollection.AUDIT_LOG,
      QueryUtils.and(
        QueryUtils.is("type", LogEntry.Type.POLICY_CHANGE),
        QueryUtils.greaterThan("lastUpdated", lastCollectTime),
        QueryUtils.lessThanOrEqual("lastUpdated", now)
      ));
    List<LogEntry<SimplePropertyPayload>> policyChanges =
      logDb.execute(findPolicyChanges).getEntities();
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
      logDb.execute(CallUtils.storeStatReportCall(LogEntry.Type.POLICY_MAP,
        PolicyInfoMapPayload.newInstance(policyMap)));
    }

    // aggregate node change decisions
    FindByQueryCall findNodeChanges = FindByQueryCall.newInstance(DataEntityCollection.AUDIT_LOG,
      QueryUtils.and(
        QueryUtils.in("type", Arrays.asList(LogEntry.Type.NODE_ADD, LogEntry.Type.NODE_REMOVE)),
        QueryUtils.greaterThan("lastUpdated", lastCollectTime),
        QueryUtils.lessThanOrEqual("lastUpdated", now)
      ));
    List<LogEntry<SimplePropertyPayload>> nodeChanges =
      logDb.execute(findNodeChanges).getEntities();
    if (nodeChanges.size() > 0) {
      for (LogEntry<SimplePropertyPayload> nodeChange : nodeChanges) {
        String hostName = (String) nodeChange.getDetails().getValue();
        if (nodeChange.getType().equals(LogEntry.Type.NODE_ADD))
          activeNodes.add(hostName);
        else
          activeNodes.remove(hostName);
      }
      logDb.execute(CallUtils.storeStatReportCall(LogEntry.Type.ACTIVE_NODES,
        StringListPayload.newInstance(new ArrayList<>(activeNodes))));
    }

    lastCollectTime = now;

    logDb.notifyUpdate();
  }

  private void storePredictionForTask(JobBehaviorPredictor predictor, String taskId) {
    try {
      TaskPredictionPayload prediction = TaskPredictionPayload.newInstance(
        predictor.getClass().getSimpleName(),
        taskId,
        predictor.predictTaskBehavior(new TaskPredictionInput(taskId)).getDuration()
      );
      StoreLogCall storePrediction = CallUtils.storeStatReportCall(LogEntry.Type.TASK_PREDICTION, prediction);
      logDb.execute(storePrediction);
    } catch (Exception e) {
      if (!(e instanceof PosumException))
        logger.error("Could not predict task duration for " + taskId + " due to: ", e);
      else if (!e.getMessage().startsWith("Task has already finished"))
        logger.debug("Could not predict task duration for " + taskId + " due to: ", e);
    }
  }

  public synchronized void reset() {
    // save current active nodes data
    logDb.execute(CallUtils.storeStatReportCall(LogEntry.Type.ACTIVE_NODES,
      StringListPayload.newInstance(new ArrayList<>(activeNodes))));

    // reinitialize predictors
    //        predictor.clearHistory();
    basicPredictor.clearHistory();
    standardPredictor.clearHistory();
    detailedPredictor.clearHistory();

    // reinitialize policy map
    initializePolicyMap();
    long now = System.currentTimeMillis();
    schedulingStart = now;
    PolicyInfoPayload info = policyMap.get(lastUsedPolicy);
    info.start(now);
    logDb.execute(CallUtils.storeStatReportCall(LogEntry.Type.POLICY_MAP,
      PolicyInfoMapPayload.newInstance(policyMap)));
  }
}

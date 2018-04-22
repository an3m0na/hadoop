package org.apache.hadoop.tools.posum.data.monitor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.client.data.DataStore;
import org.apache.hadoop.tools.posum.client.data.Database;
import org.apache.hadoop.tools.posum.client.data.DatabaseUtils;
import org.apache.hadoop.tools.posum.common.records.call.FindByQueryCall;
import org.apache.hadoop.tools.posum.common.records.call.IdsByQueryCall;
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
import org.apache.hadoop.tools.posum.common.util.cluster.PerformanceEvaluator;
import org.apache.hadoop.tools.posum.common.util.communication.CommUtils;
import org.apache.hadoop.tools.posum.common.util.conf.PolicyPortfolio;
import org.apache.hadoop.tools.posum.common.util.conf.PosumConfiguration;
import org.apache.hadoop.tools.posum.simulation.predictor.JobBehaviorPredictor;
import org.apache.hadoop.tools.posum.simulation.predictor.TaskPredictionInput;
import org.apache.hadoop.tools.posum.simulation.predictor.basic.BasicPredictor;
import org.apache.hadoop.tools.posum.simulation.predictor.detailed.DetailedPredictor;
import org.apache.hadoop.tools.posum.simulation.predictor.detailed.DetailedTaskPredictionInput;
import org.apache.hadoop.tools.posum.simulation.predictor.standard.StandardPredictor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.tools.posum.common.records.dataentity.LogEntry.Type.ACTIVE_NODES;
import static org.apache.hadoop.tools.posum.common.records.dataentity.LogEntry.Type.CLUSTER_METRICS;
import static org.apache.hadoop.tools.posum.common.records.dataentity.LogEntry.Type.NODE_ADD;
import static org.apache.hadoop.tools.posum.common.records.dataentity.LogEntry.Type.NODE_REMOVE;
import static org.apache.hadoop.tools.posum.common.records.dataentity.LogEntry.Type.PERFORMANCE;
import static org.apache.hadoop.tools.posum.common.records.dataentity.LogEntry.Type.POLICY_CHANGE;
import static org.apache.hadoop.tools.posum.common.records.dataentity.LogEntry.Type.POLICY_MAP;
import static org.apache.hadoop.tools.posum.common.records.dataentity.LogEntry.Type.SYSTEM_METRICS;
import static org.apache.hadoop.tools.posum.common.records.dataentity.LogEntry.Type.TASK_PREDICTION;

public class PosumInfoCollector {

  private static Log logger = LogFactory.getLog(PosumInfoCollector.class);

  private final PosumAPIClient api;
  private final DataStore dataStore;
  private final Database logDb;
  private final Database mainDb;
  private final Configuration conf;
  private final Map<String, PolicyInfoPayload> policyMap;
  private final Set<String> activeNodes;
  private Long lastCollectTime = 0L;
  private Long lastPrediction = 0L;
  private Long predictionTimeout = 0L;
  private Long schedulingStart = 0L;
  private String lastUsedPolicy;
  private boolean metricsOn;
  private boolean predictionsOn;
  private JobBehaviorPredictor basicPredictor;
  private JobBehaviorPredictor standardPredictor;
  private JobBehaviorPredictor detailedPredictor;
  private PerformanceEvaluator performanceEvaluator;

  public PosumInfoCollector(Configuration conf, DataStore dataStore) {
    this.conf = conf;
    this.dataStore = dataStore;
    logDb = Database.from(dataStore, DatabaseReference.getLogs());
    mainDb = Database.from(dataStore, DatabaseReference.getMain());
    performanceEvaluator = new PerformanceEvaluator(DatabaseUtils.newProvider(mainDb));
    metricsOn = conf.getBoolean(PosumConfiguration.MONITOR_METRICS_ON,
        PosumConfiguration.MONITOR_METRICS_ON_DEFAULT);
    predictionsOn = conf.getBoolean(PosumConfiguration.MONITOR_PREDICTIONS_ON,
        PosumConfiguration.MONITOR_PREDICTIONS_ON_DEFAULT);
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

    if (metricsOn) {
      storeMetricsSnapshots();
    }

    if (predictionsOn) {
      if (now - lastPrediction > predictionTimeout) {
        // make new predictions
        basicPredictor.train(mainDb);
        standardPredictor.train(mainDb);
        detailedPredictor.train(mainDb);
        IdsByQueryCall getAllTasks = IdsByQueryCall.newInstance(DataEntityCollection.TASK, null);
        List<String> taskIds = mainDb.execute(getAllTasks).getEntries();
        for (String taskId : taskIds) {
          // prediction can throw exception if data model changes state during calculation
          storePredictionForTask(basicPredictor, new TaskPredictionInput(taskId));
          storePredictionForTask(standardPredictor, new TaskPredictionInput(taskId));
          storePredictionForTask(detailedPredictor, new DetailedTaskPredictionInput(taskId, true));
          storePredictionForTask(detailedPredictor, new DetailedTaskPredictionInput(taskId, false));
          storePredictionForTask(detailedPredictor, new TaskPredictionInput(taskId));
        }
        lastPrediction = now;
      }
    }

    aggregatePolicyChanges(now);
    aggregateNodeChanges(now);

    // calculate current evaluation score
    DatabaseUtils.storeLogEntry(PERFORMANCE, performanceEvaluator.evaluate(), dataStore);

    lastCollectTime = now;
  }

  private void storeMetricsSnapshots() {
    Map<String, String> systemMetrics = new HashMap<>(4);
    for (CommUtils.PosumProcess posumProcess : CommUtils.PosumProcess.values()) {
      String response = api.getSystemMetrics(posumProcess);
      if (response != null)
        systemMetrics.put(posumProcess.name(), response);
    }
    if (!systemMetrics.isEmpty())
      DatabaseUtils.storeLogEntry(SYSTEM_METRICS, StringStringMapPayload.newInstance(systemMetrics), dataStore);
    String response = api.getClusterMetrics();
    if (response != null)
      DatabaseUtils.storeLogEntry(CLUSTER_METRICS, SimplePropertyPayload.newInstance("", response), dataStore);
  }

  private void aggregatePolicyChanges(long now) {
    FindByQueryCall findPolicyChanges = FindByQueryCall.newInstance(DataEntityCollection.AUDIT_LOG,
        QueryUtils.and(
            QueryUtils.is("type", POLICY_CHANGE),
            QueryUtils.greaterThan("lastUpdated", lastCollectTime),
            QueryUtils.lessThanOrEqual("lastUpdated", now)
        ));
    List<LogEntry<SimplePropertyPayload>> policyChanges = logDb.execute(findPolicyChanges).getEntities();
    if (policyChanges.size() > 0) {
      if (schedulingStart == 0)
        schedulingStart = policyChanges.get(0).getLastUpdated();
      for (LogEntry<SimplePropertyPayload> change : policyChanges) {
        String policy = change.getDetails().getValueAs();
        PolicyInfoPayload info = policyMap.get(policy);
        if (!policy.equals(lastUsedPolicy)) {
          if (lastUsedPolicy != null) {
            policyMap.get(lastUsedPolicy).stop(change.getLastUpdated());
          }
          lastUsedPolicy = policy;
        }
        info.start(change.getLastUpdated());
      }
      DatabaseUtils.storeStatReportCall(POLICY_MAP, PolicyInfoMapPayload.newInstance(policyMap), dataStore);
    }
  }

  private void aggregateNodeChanges(long now) {
    FindByQueryCall findNodeChanges = FindByQueryCall.newInstance(DataEntityCollection.AUDIT_LOG,
        QueryUtils.and(
            QueryUtils.in("type", Arrays.asList(NODE_ADD, NODE_REMOVE)),
            QueryUtils.greaterThan("lastUpdated", lastCollectTime),
            QueryUtils.lessThanOrEqual("lastUpdated", now)
        ));
    List<LogEntry<SimplePropertyPayload>> nodeChanges = logDb.execute(findNodeChanges).getEntities();
    if (nodeChanges.size() > 0) {
      for (LogEntry<SimplePropertyPayload> nodeChange : nodeChanges) {
        String hostName = nodeChange.getDetails().getValueAs();
        if (nodeChange.getType().equals(NODE_ADD))
          activeNodes.add(hostName);
        else
          activeNodes.remove(hostName);
      }
      DatabaseUtils.storeStatReportCall(ACTIVE_NODES, StringListPayload.newInstance(new ArrayList<>(activeNodes)), dataStore);
    }
  }

  private void storePredictionForTask(JobBehaviorPredictor predictor, TaskPredictionInput input) {
    try {
      TaskPredictionPayload prediction = TaskPredictionPayload.newInstance(
          predictor.getClass().getSimpleName(),
          input.getTaskId(),
          predictor.predictTaskBehavior(input).getDuration(),
          input instanceof DetailedTaskPredictionInput ? ((DetailedTaskPredictionInput) input).getLocal() : null
      );
      DatabaseUtils.storeLogEntry(TASK_PREDICTION, prediction, dataStore);
    } catch (Exception e) {
      if (!(e instanceof PosumException))
        logger.error("Could not predict task duration for " + input.getTaskId() + " due to: ", e);
      else if (!e.getMessage().startsWith("Task not found or finished"))
        logger.debug("Could not predict task duration for " + input.getTaskId() + " due to: " + e.getMessage());
    }
  }

  public synchronized void reset() {
    // save current active nodes data
    DatabaseUtils.storeStatReportCall(ACTIVE_NODES, StringListPayload.newInstance(new ArrayList<>(activeNodes)), dataStore);

    // reinitialize predictors
    //        predictor.clearHistory();
    basicPredictor.clearHistory();
    standardPredictor.clearHistory();
    detailedPredictor.clearHistory();

    // reinitialize policy map
    initializePolicyMap();
    long now = System.currentTimeMillis();
    schedulingStart = now;
    if (lastUsedPolicy != null) {
      PolicyInfoPayload info = policyMap.get(lastUsedPolicy);
      info.start(now);
    }
    DatabaseUtils.storeStatReportCall(POLICY_MAP, PolicyInfoMapPayload.newInstance(policyMap), dataStore);
  }
}

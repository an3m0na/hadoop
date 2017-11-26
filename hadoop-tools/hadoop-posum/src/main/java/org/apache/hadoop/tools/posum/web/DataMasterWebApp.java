package org.apache.hadoop.tools.posum.web;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.tools.posum.client.data.DatabaseUtils;
import org.apache.hadoop.tools.posum.common.records.call.FindByQueryCall;
import org.apache.hadoop.tools.posum.common.records.call.RawDocumentsByQueryCall;
import org.apache.hadoop.tools.posum.common.records.call.query.QueryUtils;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.DatabaseReference;
import org.apache.hadoop.tools.posum.common.records.dataentity.LogEntry;
import org.apache.hadoop.tools.posum.common.records.payload.CompoundScorePayload;
import org.apache.hadoop.tools.posum.common.records.payload.PolicyInfoMapPayload;
import org.apache.hadoop.tools.posum.common.records.payload.PolicyInfoPayload;
import org.apache.hadoop.tools.posum.common.records.payload.SimplePropertyPayload;
import org.apache.hadoop.tools.posum.common.records.payload.StringStringMapPayload;
import org.apache.hadoop.tools.posum.common.util.json.JsonArray;
import org.apache.hadoop.tools.posum.common.util.json.JsonElement;
import org.apache.hadoop.tools.posum.common.util.json.JsonObject;
import org.apache.hadoop.tools.posum.data.master.DataMasterContext;

import javax.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Map;

public class DataMasterWebApp extends PosumWebApp {
  private static Log logger = LogFactory.getLog(PosumWebApp.class);
  private final DataMasterContext context;

  public DataMasterWebApp(DataMasterContext context, int metricsAddressPort) {
    super(metricsAddressPort);
    this.context = context;
  }

  @Override
  protected JsonNode handleRoute(String route, HttpServletRequest request) {
    switch (route) {
      case "/policies":
        return getPolicyMetrics(extractSince(request));
      case "/system":
        return getSystemMetrics();
      case "/all-system":
        return getAllSystemMetrics(extractSince(request));
      case "/all-cluster":
        return getAllClusterMetrics(extractSince(request));
      case "/performance":
        return getPerformanceReadings(extractSince(request));
      case "/logs":
        return getLogs(extractSince(request));
      default:
        if (route.startsWith("/data/")) {
          String path = route.substring("/data/".length());
          String db = null;
          String collection = path;
          int index = path.indexOf("/");
          if (index >= 0) {
            db = path.substring(0, index);
            collection = path.substring(index);
          }
          return wrapResult(context.getDataStore().execute(
            RawDocumentsByQueryCall.newInstance(
              DataEntityCollection.fromLabel(collection),
              QueryUtils.withParams(request.getParameterMap())
            ),
            DatabaseReference.fromName(db)));
        }
        return handleUnknownRoute();
    }
  }

  private JsonNode getPerformanceReadings(long since) {
    FindByQueryCall findLogs = FindByQueryCall.newInstance(LogEntry.Type.PERFORMANCE.getCollection(),
      QueryUtils.and(
        QueryUtils.is("type", LogEntry.Type.PERFORMANCE),
        QueryUtils.greaterThan("lastUpdated", since)
      ), "lastUpdated", false);
    List<LogEntry<CompoundScorePayload>> logs = context.getDataStore().execute(findLogs, DatabaseReference.getLogs()).getEntities();
    JsonArray ret = new JsonArray();
    for (LogEntry<CompoundScorePayload> log : logs) {
      ret.add(
        new JsonObject()
          .put("time", log.getLastUpdated())
          .put("score", JsonElement.write(log.getDetails()))
      );
    }
    return wrapResult(new JsonObject()
      .put("time", System.currentTimeMillis())
      .put("entries", ret)
      .getNode());
  }

  private JsonNode getAllSystemMetrics(long since) {
    FindByQueryCall findLogs = FindByQueryCall.newInstance(LogEntry.Type.SYSTEM_METRICS.getCollection(),
      QueryUtils.and(
        QueryUtils.is("type", LogEntry.Type.SYSTEM_METRICS),
        QueryUtils.greaterThan("lastUpdated", since)
      ), "lastUpdated", false);
    List<LogEntry<StringStringMapPayload>> logs = context.getDataStore().execute(findLogs, DatabaseReference.getLogs()).getEntities();
    JsonObject ret = new JsonObject().put("time", System.currentTimeMillis());
    for (LogEntry<StringStringMapPayload> log : logs) {
      for (Map.Entry<String, String> entry : log.getDetails().getEntries().entrySet()) {
        JsonArray list = ret.getAsArray(entry.getKey());
        if (list == null) {
          list = new JsonArray();
          ret.put(entry.getKey(), list);
        }
        try {
          JsonNode result = JsonElement.read(entry.getValue()).getNode();
          if (result != null && result.get("successful").asBoolean())
            list.add(new JsonElement(result.get("result")));
        } catch (Exception e) {
          logger.error("Could not deserialize metrics json " + entry.getValue(), e);
        }
      }
    }
    return wrapResult(ret.getNode());
  }

  private JsonNode getAllClusterMetrics(long since) {
    FindByQueryCall findLogs = FindByQueryCall.newInstance(LogEntry.Type.CLUSTER_METRICS.getCollection(),
      QueryUtils.and(
        QueryUtils.is("type", LogEntry.Type.CLUSTER_METRICS),
        QueryUtils.greaterThan("lastUpdated", since)
      ), "lastUpdated", false);
    List<LogEntry<SimplePropertyPayload>> logs = context.getDataStore().execute(findLogs, DatabaseReference.getLogs()).getEntities();
    JsonArray entries = new JsonArray();
    for (LogEntry<SimplePropertyPayload> log : logs) {
      String stringResult = log.getDetails().getValueAs();
      try {
        JsonNode result = JsonElement.read(stringResult).getNode();
        if (result != null && result.get("successful").asBoolean())
          entries.add(new JsonElement(result.get("result")));
      } catch (Exception e) {
        logger.error("Could not deserialize metrics json " + stringResult, e);
      }
    }
    return wrapResult(new JsonObject()
      .put("time", System.currentTimeMillis())
      .put("entries", entries)
      .getNode());
  }

  private JsonNode getPolicyMetrics(Long since) {
    return wrapResult(new JsonObject()
      .put("time", System.currentTimeMillis())
      .put("distribution", composePolicyMap())
      .put("entries", composeRecentChoices(since))
      .getNode());
  }

  private JsonObject composePolicyMap() {
    JsonObject ret = new JsonObject();
    LogEntry<PolicyInfoMapPayload> policyReport = DatabaseUtils.findStatsLogEntry(LogEntry.Type.POLICY_MAP, context.getDataStore());
    if (policyReport != null) {
      for (Map.Entry<String, PolicyInfoPayload> policyInfo : policyReport.getDetails().getEntries().entrySet()) {
        ret.put(policyInfo.getKey(), new JsonObject()
          .put("time", policyInfo.getValue().getUsageTime())
          .put("number", policyInfo.getValue().getUsageNumber()));
      }
    }
    return ret;
  }

  private JsonArray composeRecentChoices(long since) {
    JsonArray choices = new JsonArray();
    FindByQueryCall findChoices = FindByQueryCall.newInstance(LogEntry.Type.POLICY_CHANGE.getCollection(),
      QueryUtils.and(
        QueryUtils.is("type", LogEntry.Type.POLICY_CHANGE),
        QueryUtils.greaterThan("lastUpdated", since)
      ));
    List<LogEntry<SimplePropertyPayload>> choiceLogs =
      context.getDataStore().execute(findChoices, DatabaseReference.getLogs()).getEntities();
    for (LogEntry<SimplePropertyPayload> choiceEntry : choiceLogs) {
      String policy = choiceEntry.getDetails().getValueAs();
      choices.add(new JsonObject()
        .put("time", choiceEntry.getLastUpdated())
        .put("policy", policy)
      );
    }
    return choices;
  }

  private JsonNode getLogs(Long since) {
    JsonArray list = new JsonArray();
    FindByQueryCall findLogs = FindByQueryCall.newInstance(LogEntry.Type.GENERAL.getCollection(),
      QueryUtils.and(
        QueryUtils.is("type", LogEntry.Type.GENERAL),
        QueryUtils.greaterThan("lastUpdated", since)
      ), "lastUpdated", false);
    List<LogEntry<SimplePropertyPayload>> logs =
      context.getDataStore().execute(findLogs, DatabaseReference.getLogs()).getEntities();
    for (LogEntry<SimplePropertyPayload> logEntry : logs) {
      String message = logEntry.getDetails().getValueAs();
      list.add(new JsonObject()
        .put("timestamp", logEntry.getLastUpdated())
        .put("message", message));
    }
    return wrapResult(list.getNode());
  }


}

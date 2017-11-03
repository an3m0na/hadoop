package org.apache.hadoop.tools.posum.web;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.tools.posum.common.records.call.CallUtils;
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
import org.apache.hadoop.tools.posum.common.util.Utils;
import org.apache.hadoop.tools.posum.common.util.json.JsonArray;
import org.apache.hadoop.tools.posum.common.util.json.JsonElement;
import org.apache.hadoop.tools.posum.common.util.json.JsonObject;
import org.apache.hadoop.tools.posum.data.master.DataMasterContext;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.handler.AbstractHandler;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
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
  protected Handler constructHandler() {
    return new AbstractHandler() {
      @Override
      public void handle(String target, HttpServletRequest request,
                         HttpServletResponse response, int dispatch) {
        try {
          if (target.startsWith("/ajax")) {
            // json request
            String call = target.substring("/ajax".length());
            JsonNode ret;
            String sinceParam;
            Long since = 0L;
            try {
              switch (call) {
                case "/policies":
                  sinceParam = request.getParameter("since");
                  if (sinceParam != null)
                    since = Long.valueOf(sinceParam);
                  ret = getPolicyMetrics(since);
                  break;
                case "/system":
                  ret = getSystemMetrics();
                  break;
                case "/all-system":
                  ret = getAllSystemMetrics();
                  break;
                case "/all-cluster":
                  ret = getAllClusterMetrics();
                  break;
                case "/performance":
                  sinceParam = request.getParameter("since");
                  if (sinceParam != null)
                    since = Long.valueOf(sinceParam);
                  ret = getPerformanceReadings(since);
                  break;
                case "/logs":
                  sinceParam = request.getParameter("since");
                  if (sinceParam != null)
                    since = Long.valueOf(sinceParam);
                  ret = getLogs(since);
                  break;
                default:
                  if (call.startsWith("/data/")) {
                    String path = call.substring("/data/".length());
                    String db = null;
                    String collection = path;
                    int index = path.indexOf("/");
                    if (index >= 0) {
                      db = path.substring(0, index);
                      collection = path.substring(index);
                    }
                    ret = wrapResult(context.getDataStore().execute(
                      RawDocumentsByQueryCall.newInstance(
                        DataEntityCollection.fromLabel(collection),
                        QueryUtils.withParams(request.getParameterMap())
                      ),
                      DatabaseReference.fromName(db)));
                    break;
                  }
                  ret = wrapError("UNKNOWN_ROUTE", "Specified service path does not exist", call);
              }
            } catch (Exception e) {
              ret = wrapError("EXCEPTION_OCCURRED", e.getMessage(), Utils.getErrorTrace(e));
            }
            sendResult(request, response, ret);
          } else {
            // static resource request
            response.setCharacterEncoding("utf-8");
            staticHandler.handle(target, request, response, dispatch);
          }
        } catch (Exception e) {
          logger.error("Error resolving request: ", e);
        }
      }
    };
  }

  private JsonNode getPerformanceReadings(Long since) {
    FindByQueryCall findLogs = FindByQueryCall.newInstance(LogEntry.Type.PERFORMANCE.getCollection(),
      QueryUtils.and(
        QueryUtils.is("type", LogEntry.Type.PERFORMANCE),
        QueryUtils.greaterThan("lastUpdated", since)
      ), "lastUpdated", false);
    List<LogEntry<CompoundScorePayload>> logs = context.getDataStore().execute(findLogs, DatabaseReference.getLogs()).getEntities();
    JsonArray times = new JsonArray();
    JsonArray slowdowns = new JsonArray();
    JsonArray penalties = new JsonArray();
    JsonArray costs = new JsonArray();
    for (LogEntry<CompoundScorePayload> log : logs) {
      times.add(log.getLastUpdated());
      slowdowns.add(log.getDetails().getSlowdown());
      penalties.add(log.getDetails().getPenalty());
      costs.add(log.getDetails().getCost());
    }
    return new JsonObject()
      .put("time", System.currentTimeMillis())
      .put("scores", new JsonObject()
        .put("times", times)
        .put("slowdowns", slowdowns)
        .put("costs", costs)
      )
      .getNode();
  }

  private JsonNode getAllSystemMetrics() {
    FindByQueryCall findLogs = FindByQueryCall.newInstance(LogEntry.Type.SYSTEM_METRICS.getCollection(),
      QueryUtils.is("type", LogEntry.Type.SYSTEM_METRICS), "lastUpdated", false);
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
    return ret.getNode();
  }

  private JsonNode getAllClusterMetrics() {
    FindByQueryCall findLogs = FindByQueryCall.newInstance(LogEntry.Type.CLUSTER_METRICS.getCollection(),
      QueryUtils.is("type", LogEntry.Type.CLUSTER_METRICS), "lastUpdated", false);
    List<LogEntry<SimplePropertyPayload>> logs = context.getDataStore().execute(findLogs, DatabaseReference.getLogs()).getEntities();
    JsonArray entries = new JsonArray();
    for (LogEntry<SimplePropertyPayload> log : logs) {
      String stringResult = (String) log.getDetails().getValue();
      try {
        JsonNode result = JsonElement.read(stringResult).getNode();
        if (result != null && result.get("successful").asBoolean())
          entries.add(new JsonElement(result.get("result")));
      } catch (Exception e) {
        logger.error("Could not deserialize metrics json " + stringResult, e);
      }
    }
    return new JsonObject()
      .put("time", System.currentTimeMillis())
      .put("entries", entries)
      .getNode();
  }

  private JsonNode getPolicyMetrics(Long since) {
    return wrapResult(new JsonObject()
      .put("time", System.currentTimeMillis())
      .put("policies", new JsonObject()
        .put("map", composePolicyMap())
        .put("list", composeRecentChoices(since)))
      .getNode());
  }

  private JsonObject composePolicyMap() {
    JsonObject ret = new JsonObject();
    LogEntry<PolicyInfoMapPayload> policyReport = context.getDataStore().execute(
      CallUtils.findStatReportCall(LogEntry.Type.POLICY_MAP), DatabaseReference.getLogs()).getEntity();
    if (policyReport != null) {
      for (Map.Entry<String, PolicyInfoPayload> policyInfo : policyReport.getDetails().getEntries().entrySet()) {
        ret.put(policyInfo.getKey(), new JsonObject()
          .put("time", policyInfo.getValue().getUsageTime())
          .put("number", policyInfo.getValue().getUsageNumber()));
      }
    }
    return ret;
  }

  private JsonObject composeRecentChoices(Long since) {
    JsonArray times = new JsonArray();
    JsonArray choices = new JsonArray();
    FindByQueryCall findChoices = FindByQueryCall.newInstance(LogEntry.Type.POLICY_CHANGE.getCollection(),
      QueryUtils.and(
        QueryUtils.is("type", LogEntry.Type.POLICY_CHANGE),
        QueryUtils.greaterThan("lastUpdated", since)
      ));
    List<LogEntry<SimplePropertyPayload>> choiceLogs =
      context.getDataStore().execute(findChoices, DatabaseReference.getLogs()).getEntities();
    for (LogEntry<SimplePropertyPayload> choiceEntry : choiceLogs) {
      times.add(choiceEntry.getLastUpdated());
      choices.add((String) choiceEntry.getDetails().getValue());
    }
    return new JsonObject().put("times", times).put("policies", choices);
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
      list.add(new JsonObject()
        .put("timestamp", logEntry.getLastUpdated())
        .put("message", (String) logEntry.getDetails().getValue()));
    }
    return wrapResult(list.getNode());
  }


}

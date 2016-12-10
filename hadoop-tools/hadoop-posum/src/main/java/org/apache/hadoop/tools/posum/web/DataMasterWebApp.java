package org.apache.hadoop.tools.posum.web;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.tools.posum.common.records.call.CallUtils;
import org.apache.hadoop.tools.posum.common.records.call.FindByQueryCall;
import org.apache.hadoop.tools.posum.common.records.call.RawDocumentsByQueryCall;
import org.apache.hadoop.tools.posum.common.records.call.query.QueryUtils;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.dataentity.LogEntry;
import org.apache.hadoop.tools.posum.common.records.payload.PolicyInfoMapPayload;
import org.apache.hadoop.tools.posum.common.records.payload.PolicyInfoPayload;
import org.apache.hadoop.tools.posum.common.records.payload.SimplePropertyPayload;
import org.apache.hadoop.tools.posum.common.util.json.JsonArray;
import org.apache.hadoop.tools.posum.common.util.json.JsonObject;
import org.apache.hadoop.tools.posum.common.util.Utils;
import org.apache.hadoop.tools.posum.data.master.DataMasterContext;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.handler.AbstractHandler;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.List;
import java.util.Map;

public class DataMasterWebApp extends PosumWebApp {
    private static Log logger = LogFactory.getLog(PosumWebApp.class);

    private final transient DataMasterContext context;

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
                        try {
                            switch (call) {
                                case "/policies":
                                    Long since = 0L;
                                    String sinceParam = request.getParameter("since");
                                    if (sinceParam != null) {
                                        try {
                                            since = Long.valueOf(sinceParam);
                                        } catch (Exception e) {
                                            ret = wrapError("INCORRECT_PARAMETERS",
                                                    "Could not parce parameter 'since'", e.getMessage());
                                            break;
                                        }
                                    }
                                    ret = getPolicyMetrics(since);
                                    break;
                                case "/system":
                                    ret = getSystemMetrics();
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
                                        ret = wrapResult(context.getDataStore().executeDatabaseCall(
                                                RawDocumentsByQueryCall.newInstance(
                                                        DataEntityCollection.fromLabel(collection),
                                                        QueryUtils.withParams(request.getParameterMap())
                                                ),
                                                DataEntityDB.fromName(db)));
                                        break;
                                    }
                                    ret = wrapError("UNKNOWN_ROUTE", "Specified service path does not exist", null);
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
        LogEntry<PolicyInfoMapPayload> policyReport = context.getDataStore().executeDatabaseCall(
                CallUtils.findStatReportCall(LogEntry.Type.POLICY_MAP), DataEntityDB.getLogs()).getEntity();
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
                        QueryUtils.greaterThan("timestamp", since)
                ));
        List<LogEntry<SimplePropertyPayload>> choiceLogs =
                context.getDataStore().executeDatabaseCall(findChoices, DataEntityDB.getLogs()).getEntities();
        for (LogEntry<SimplePropertyPayload> choiceEntry : choiceLogs) {
            times.add(choiceEntry.getTimestamp());
            choices.add((String) choiceEntry.getDetails().getValue());
        }
        return new JsonObject().put("times", times).put("policies", choices);
    }
}

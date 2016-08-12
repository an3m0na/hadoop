package org.apache.hadoop.tools.posum.web;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.tools.posum.common.records.dataentity.LogEntry;
import org.apache.hadoop.tools.posum.common.records.payload.SimplePropertyPayload;
import org.apache.hadoop.tools.posum.common.util.json.JsonArray;
import org.apache.hadoop.tools.posum.common.util.json.JsonObject;
import org.apache.hadoop.tools.posum.common.util.PolicyMap;
import org.apache.hadoop.tools.posum.common.util.Utils;
import org.apache.hadoop.tools.posum.database.master.DataMasterContext;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.handler.AbstractHandler;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Map;

/**
 * Created by ane on 4/29/16.
 */
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
                                        ret = wrapResult(context.getDataStore().getRawDocumentList(db,
                                                collection, (Map<String, Object>) request.getParameterMap()));
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
        LogEntry<PolicyMap> policyReport = context.getDataStore().findReport(LogEntry.Type.POLICY_MAP);
        if (policyReport != null) {
            for (Map.Entry<String, PolicyMap.PolicyInfo> policyInfo :
                    policyReport.getDetails().entrySet()) {
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
        for (LogEntry<SimplePropertyPayload> choiceEntry :
                context.getDataStore().<SimplePropertyPayload>findLogs(LogEntry.Type.POLICY_CHANGE, since)) {
            times.add(choiceEntry.getTimestamp());
            choices.add((String) choiceEntry.getDetails().getValue());
        }
        return new JsonObject().put("times", times).put("policies", choices);
    }
}

package org.apache.hadoop.tools.posum.web;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.tools.posum.common.util.JsonArray;
import org.apache.hadoop.tools.posum.common.util.JsonObject;
import org.apache.hadoop.tools.posum.common.util.PolicyMap;
import org.apache.hadoop.tools.posum.common.util.Utils;
import org.apache.hadoop.tools.posum.database.client.DBInterface;
import org.apache.hadoop.tools.posum.database.master.DataMaster;
import org.apache.hadoop.tools.posum.database.monitor.POSUMMonitor;
import org.apache.hadoop.tools.posum.database.store.DataStore;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.handler.AbstractHandler;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Map;

/**
 * Created by ane on 4/29/16.
 */
public class DataMasterWebApp extends POSUMWebApp {
    private static Log logger = LogFactory.getLog(POSUMWebApp.class);

    private final transient DataStore dataStore;
    private final transient POSUMMonitor monitor;

    public DataMasterWebApp(DataStore dataStore, POSUMMonitor monitor, int metricsAddressPort) {
        super(metricsAddressPort);
        this.dataStore = dataStore;
        this.monitor = monitor;
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
                                    ret = getPolicyMetrics();
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
                                        ret = wrapResult(dataStore.getRawDocumentList(db,
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

    private JsonNode getPolicyMetrics() {
        return wrapResult(new JsonObject()
                .put("time", System.currentTimeMillis())
                .put("policies", new JsonObject()
                        .put("map", parsePolicyMap())
                        .put("list", parseRecentChoices()))
                .getNode());
    }

    private JsonObject parsePolicyMap() {
        JsonObject ret = new JsonObject();
        for (Map.Entry<String, PolicyMap.PolicyInfo> policyInfoEntry : monitor.getPolicyMap().entrySet()) {
            ret.put(policyInfoEntry.getKey(), new JsonObject()
                    .put("time", "0")
                    .put("number", "0"));
        }
        return ret;
    }

    private JsonObject parseRecentChoices() {

        JsonArray times = new JsonArray();
        JsonArray choices = new JsonArray();
        for (Map.Entry<Long, String> choiceEntry : monitor.getRecentChoices().entrySet()) {
            times.add(choiceEntry.getKey());
            choices.add(choiceEntry.getValue());
        }
        return new JsonObject().put("times", times).put("policies", choices);
    }
}

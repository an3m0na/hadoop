package org.apache.hadoop.tools.posum.web;

import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.tools.posum.common.util.*;
import org.apache.hadoop.tools.posum.core.scheduler.meta.PortfolioMetaScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.handler.AbstractHandler;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Map;

/**
 * Created by ane on 4/29/16.
 */
public class MetaSchedulerWebApp extends POSUMWebApp {
    private static Log logger = LogFactory.getLog(POSUMWebApp.class);

    private final transient PortfolioMetaScheduler scheduler;

    public MetaSchedulerWebApp(PortfolioMetaScheduler scheduler, int metricsAddressPort) {
        super(metricsAddressPort);
        this.scheduler = scheduler;
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
                                case "/cluster":
                                    ret = getClusterMetrics();
                                    break;
                                case "/scheduler":
                                    ret = getSchedulerMetrics();
                                    break;
                                case "/system":
                                    ret = getSystemMetrics();
                                    break;
                                default:
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

    private JsonNode getSystemMetrics() {
        return wrapResult(new JsonObject()
                .put("time", System.currentTimeMillis())
                .put("jvm", new JsonObject()
                        .put("free", Runtime.getRuntime().freeMemory() / 1024 / 1024 / 1024)
                        .put("max", Runtime.getRuntime().maxMemory() / 1024 / 1024 / 1024)
                        .put("total", Runtime.getRuntime().totalMemory() / 1024 / 1024 / 1024))
                .getNode());
    }

    private JsonNode getSchedulerMetrics() {
        JsonObject timecosts = new JsonObject()
                .put("ALLOCATE", scheduler.getAllocateTimer().getSnapshot().getMean())
                .put("HANDLE", scheduler.getHandleTimer().getSnapshot().getMean());
        //TODO also change event
        for (Map.Entry<SchedulerEventType, Timer> entry : scheduler.getHandleByTypeTimers().entrySet()) {
            timecosts.put("HANDLE_" + entry.getKey().name(), entry.getValue().getSnapshot().getMean());
        }
        return wrapResult(new JsonObject()
                .put("time", System.currentTimeMillis())
                .put("timecost", timecosts)
                //TODO (num applications, allocated GB and vcores) for each queue
                //TODO send this to system monitor DM
                .put("policies", new JsonObject()
                        .put("map", parsePolicyMap())
                        .put("list", parseRecentChoices()))
                .getNode());
    }

    private JsonNode getClusterMetrics() {

        QueueMetrics rootMetrics = scheduler.getRootQueueMetrics();
        if (rootMetrics == null) {
            return wrapError("NULL_METRICS", "Scheduler metrics were null", null);
        }

        return wrapResult(new JsonObject()
                .put("time", System.currentTimeMillis())
                .put("running", new JsonObject()
                        .put("root", new JsonObject()
                                .put("applications", rootMetrics.getAppsRunning())
                                .put("containers", rootMetrics.getAllocatedContainers())))
                .put("memoryGB", new JsonObject()
                        .put("allocated", rootMetrics.getAllocatedMB() / 1024)
                        .put("available", rootMetrics.getAvailableMB() / 1024))
                .put("vcores", new JsonObject()
                        .put("allocated", rootMetrics.getAllocatedVirtualCores())
                        .put("available", rootMetrics.getAvailableVirtualCores()))
                .getNode());
    }

    private JsonObject parsePolicyMap() {
        JsonObject ret = new JsonObject();
        for (Map.Entry<String, PolicyMap.PolicyInfo> policyInfoEntry : scheduler.getPolicyMap().entrySet()) {
            ret.put(policyInfoEntry.getKey(), new JsonObject()
                    .put("time", policyInfoEntry.getValue().getUsageTime())
                    .put("number", policyInfoEntry.getValue().getUsageNumber()));
        }
        return ret;
    }

    private JsonObject parseRecentChoices() {

        JsonArray times = new JsonArray();
        JsonArray choices = new JsonArray();
        for (Map.Entry<Long, String> choiceEntry : scheduler.getRecentChoices().entrySet()) {
            times.add(choiceEntry.getKey());
            choices.add(choiceEntry.getValue());
        }
        return new JsonObject().put("times", times).put("policies", choices);
    }
}

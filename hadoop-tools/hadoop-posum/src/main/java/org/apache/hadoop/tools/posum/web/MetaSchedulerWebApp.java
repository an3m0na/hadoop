package org.apache.hadoop.tools.posum.web;

import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.tools.posum.common.util.*;
import org.apache.hadoop.tools.posum.common.util.json.JsonObject;
import org.apache.hadoop.tools.posum.scheduler.core.PortfolioMetaScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.handler.AbstractHandler;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Map;

public class MetaSchedulerWebApp extends PosumWebApp {
    private static Log logger = LogFactory.getLog(PosumWebApp.class);

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

    private JsonNode getSchedulerMetrics() {

        JsonObject timecosts = new JsonObject();
        if (scheduler.hasMetricsOn()) {
            timecosts.put("ALLOCATE", scheduler.getAllocateTimer().getSnapshot().getMean() / 1000000)
                    .put("HANDLE", scheduler.getHandleTimer().getSnapshot().getMean() / 1000000)
                    .put("CHANGE", scheduler.getChangeTimer().getSnapshot().getMean() / 1000000);
            for (Map.Entry<SchedulerEventType, Timer> entry : scheduler.getHandleByTypeTimers().entrySet()) {
                timecosts.put("HANDLE_" + entry.getKey().name(), entry.getValue().getSnapshot().getMean() / 1000000);
            }
        }
        return wrapResult(new JsonObject()
                .put("time", System.currentTimeMillis())
                .put("timecost", timecosts)
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
}

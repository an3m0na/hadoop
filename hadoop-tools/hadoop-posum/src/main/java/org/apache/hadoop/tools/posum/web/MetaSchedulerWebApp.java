package org.apache.hadoop.tools.posum.web;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hadoop.tools.posum.common.util.PolicyMap;
import org.apache.hadoop.tools.posum.core.scheduler.meta.PortfolioMetaScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.handler.AbstractHandler;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;

/**
 * Created by ane on 4/29/16.
 */
public class MetaSchedulerWebApp extends POSUMWebApp {

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
                                case "/metrics":
                                    ret = generateRealTimeTrackingMetrics();
                                    break;
                                default:
                                    ret = wrapError("UNKNOWN_ROUTE", "Specified service path does not exist", null);
                            }
                        } catch (Exception e) {
                            StringWriter traceWriter = new StringWriter();
                            e.printStackTrace(new PrintWriter(traceWriter));
                            ret = wrapError("EXCEPTION_OCCURRED", e.getMessage(), traceWriter.toString());
                        }
                        sendResult(request, response, ret);
                    } else {
                        // static resource request
                        response.setCharacterEncoding("utf-8");
                        staticHandler.handle(target, request, response, dispatch);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };
    }

    private JsonNode generateRealTimeTrackingMetrics() {

        QueueMetrics rootMetrics = scheduler.getRootQueueMetrics();
        if (rootMetrics == null) {
            return wrapError("NULL_METRICS", "Scheduler metrics were null", null);
        }

        // package results
//        StringBuilder sb = new StringBuilder();
//        sb.append("{");
//                .append(",\"jvm.free.memory\":").append(jvmFreeMemoryGB)
//                .append(",\"jvm.max.memory\":").append(jvmMaxMemoryGB)
//                .append(",\"jvm.total.memory\":").append(jvmTotalMemoryGB)
//                .append(",\"running.applications\":").append(numRunningApps)
//                .append(",\"running.containers\":").append(numRunningContainers)
//                .append(",\"cluster.allocated.memory\":").append(allocatedMemoryGB)
//                .append(",\"cluster.allocated.vcores\":").append(allocatedVCoresGB)
//                .append(",\"cluster.available.memory\":").append(availableMemoryGB)
//                .append(",\"cluster.available.vcores\":").append(availableVCoresGB)
        ;

//        for (String queue : wrapper.getQueueSet()) {
//            sb.append(",\"queue.").append(queue).append(".allocated.memory\":")
//                    .append(queueAllocatedMemoryMap.get(queue));
//            sb.append(",\"queue.").append(queue).append(".allocated.vcores\":")
//                    .append(queueAllocatedVCoresMap.get(queue));
//        }
//        // scheduler allocate & handle
//        sb.append(",\"scheduler.allocate.timecost\":").append(allocateTimecost);
//        sb.append(",\"scheduler.handle.timecost\":").append(handleTimecost);
//        for (SchedulerEventType e : SchedulerEventType.values()) {
//            sb.append(",\"scheduler.handle-").append(e).append(".timecost\":")
//                    .append(handleOperTimecostMap.get(e));
//        }
//        sb.append("}");
//        return sb.toString();

        ObjectNode ret = mapper.createObjectNode();
        ret.put("time", System.currentTimeMillis());


        ret.put("running.applications", rootMetrics.getAppsRunning());
        ret.put("running.containers", rootMetrics.getAllocatedContainers());
        ret.put("policies.map", parsePolicyMap());

        return wrapResult(ret);
    }

    private JsonNode parsePolicyMap() {
        ObjectNode retObject = mapper.createObjectNode();
        for (Map.Entry<String, PolicyMap.PolicyInfo> policyInfoEntry : scheduler.getPolicyMap().entrySet()) {
            ObjectNode policyInfoObject = mapper.createObjectNode();
            policyInfoObject.put("time", policyInfoEntry.getValue().getUsageTime());
            policyInfoObject.put("number", policyInfoEntry.getValue().getUsageNumber());
            retObject.put(policyInfoEntry.getKey(), policyInfoObject);
        }
        return retObject;
    }
}

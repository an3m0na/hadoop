package org.apache.hadoop.tools.posum.web;

import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.tools.posum.common.util.Utils;
import org.apache.hadoop.tools.posum.common.util.json.JsonObject;
import org.apache.hadoop.tools.posum.scheduler.core.PortfolioMetaScheduler;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.apache.hadoop.yarn.util.resource.Resources;
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
      .put("resources", writeResourceReports())
      .getNode());
  }

  private JsonObject writeResourceReports() {
    Resource used = Resource.newInstance(0, 0);
    Resource avail = Resource.newInstance(0, 0);
    int num = 0;
    JsonObject reportsJson = new JsonObject();
    for (Map.Entry<String, SchedulerNodeReport> entry : scheduler.getNodeReports().entrySet()) {
      SchedulerNodeReport report = entry.getValue();
      Resources.addTo(avail, report.getAvailableResource());
      Resources.addTo(used, report.getUsedResource());
      num += report.getNumContainers();
      reportsJson.put(entry.getKey(), writeReport(report));
    }
    reportsJson.put("Total", writeReport(used, avail, num));
    return reportsJson;
  }

  private JsonObject writeReport(Resource used, Resource avail, int num) {
    return new JsonObject()
      .put("used", writeResource(used))
      .put("avail", writeResource(avail))
      .put("num", num);
  }

  private JsonObject writeReport(SchedulerNodeReport report) {
    return writeReport(report.getUsedResource(), report.getAvailableResource(), report.getNumContainers());
  }

  private JsonObject writeResource(Resource resource) {
    return new JsonObject()
      .put("memory", resource.getMemory())
      .put("vcores", resource.getVirtualCores());
  }
}

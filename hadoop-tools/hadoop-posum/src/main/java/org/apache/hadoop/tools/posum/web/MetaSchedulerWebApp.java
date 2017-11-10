package org.apache.hadoop.tools.posum.web;

import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.tools.posum.common.util.json.JsonObject;
import org.apache.hadoop.tools.posum.scheduler.core.PortfolioMetaScheduler;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEventType;
import org.apache.hadoop.yarn.util.resource.Resources;

import javax.servlet.http.HttpServletRequest;
import java.util.Map;

public class MetaSchedulerWebApp extends PosumWebApp {
  private static Log logger = LogFactory.getLog(PosumWebApp.class);

  private final transient PortfolioMetaScheduler scheduler;

  public MetaSchedulerWebApp(PortfolioMetaScheduler scheduler, int metricsAddressPort) {
    super(metricsAddressPort);
    this.scheduler = scheduler;
  }

  @Override
  protected JsonNode handleRoute(String route, HttpServletRequest request) {
    switch (route) {
      case "/cluster":
        return getClusterMetrics();
      case "/scheduler":
        return getSchedulerMetrics();
      case "/system":
        return getSystemMetrics();
      default:
        return handleUnknownRoute();
    }
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
      .put("queues", new JsonObject()
        .put("root", new JsonObject()
          .put("applications", new JsonObject()
            .put("running", rootMetrics.getAppsRunning())
            .put("pending", rootMetrics.getAppsPending()))
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

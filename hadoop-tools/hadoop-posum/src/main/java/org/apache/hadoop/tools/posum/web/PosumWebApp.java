package org.apache.hadoop.tools.posum.web;

import com.fasterxml.jackson.databind.JsonNode;
import com.sun.management.OperatingSystemMXBean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.tools.posum.common.util.communication.CommUtils;
import org.apache.hadoop.tools.posum.common.util.json.JsonElement;
import org.apache.hadoop.tools.posum.common.util.json.JsonObject;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.AbstractHandler;
import org.mortbay.jetty.handler.ResourceHandler;
import org.mortbay.resource.Resource;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;
import java.io.BufferedReader;
import java.io.IOException;
import java.lang.management.ManagementFactory;

public abstract class PosumWebApp extends HttpServlet {
  private static final long serialVersionUID = 1905162041950251407L;
  private static Log logger = LogFactory.getLog(PosumWebApp.class);

  private transient Server server;

  private int port;
  protected final ResourceHandler staticHandler = new ResourceHandler();
  private final Handler handler;

  public PosumWebApp(int metricsAddressPort) {
    port = metricsAddressPort;
    staticHandler.setBaseResource(Resource.newClassPathResource("html"));
    handler = constructHandler();
  }

  protected Handler constructHandler() {
    return new AbstractHandler() {
      @Override
      public void handle(String target, HttpServletRequest request,
                         HttpServletResponse response, int dispatch) {
        try {
          if (target.startsWith("/ajax")) {
            // json request
            String route = target.substring("/ajax".length());
            JsonNode ret;
            try {
              ret = handleRoute(route, request);
            } catch (Exception e) {
              ret = wrapError("EXCEPTION_OCCURRED", e.getMessage(), CommUtils.getErrorTrace(e));
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

  protected JsonNode handleRoute(String route, HttpServletRequest request) {
    switch (route) {
      case "/system":
        return getSystemMetrics();
      default:
        return handleUnknownRoute();
    }
  }

  protected JsonNode handleUnknownRoute() {
    return wrapError("UNKNOWN_ROUTE", "Specified service path does not exist", null);
  }

  public void start() throws Exception {

    server = new Server(port);
    server.setHandler(handler);

    server.start();
  }

  public void stop() throws Exception {
    if (server != null) {
      server.stop();
    }
  }

  protected JsonNode wrapResult(Object result) {
    JsonNode wrapper = new JsonObject()
      .put("successful", true)
      .put("result", JsonElement.write(result))
      .getNode();
    LoggerFactory.getLogger(getClass()).trace("Sending result " + wrapper);
    return wrapper;
  }

  protected JsonNode wrapError(String code, String message, String errorString) {
    LoggerFactory.getLogger(getClass()).error("Sending error: " + message + ".Caused by :" + errorString);
    return new JsonObject()
      .put("successful", false)
      .put("result", new JsonObject().put("code", code).put("message", message))
      .getNode();
  }


  protected void sendResult(HttpServletRequest request,
                            HttpServletResponse response,
                            JsonNode result)
    throws IOException {
    response.setContentType(MediaType.APPLICATION_JSON);
    response.setStatus(HttpServletResponse.SC_OK);
    response.setHeader("Access-Control-Allow-Origin", "*");
    response.setHeader("Access-Control-Allow-Methods", "OPTIONS, GET, POST");

    response.getWriter().println(result.toString());
    ((Request) request).setHandled(true);
  }

  protected JsonNode getSystemMetrics() {
    double max = Runtime.getRuntime().maxMemory();
    double totalMb = Runtime.getRuntime().totalMemory();
    double usedMb = totalMb - Runtime.getRuntime().freeMemory();
    OperatingSystemMXBean osManagement = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
    double processLoad = osManagement.getProcessCpuLoad();
    double totalLoad = osManagement.getSystemCpuLoad();
    int threadCount = ManagementFactory.getThreadMXBean().getThreadCount();
    return wrapResult(new JsonObject()
      .put("time", System.currentTimeMillis())
      .put("jvm", new JsonObject()
        .put("used", String.format("%.3f", usedMb / 1024 / 1024 / 1024))
        .put("max", String.format("%.3f", max / 1024 / 1024 / 1024))
        .put("total", String.format("%.3f", totalMb / 1024 / 1024 / 1024)))
      .put("cpu", new JsonObject()
        .put("total", String.format("%.1f", totalLoad * 100))
        .put("process", String.format("%.1f", processLoad * 100)))
      .put("threadCount", Integer.toString(threadCount))
      .getNode());
  }

  protected long extractSince(HttpServletRequest request) {
    String sinceParam = request.getParameter("since");
    if (sinceParam != null) {
      try {
        return Long.valueOf(sinceParam);
      } catch (Exception e) {
        logger.debug("Could not read since param: " + sinceParam);
      }
    }
    return 0;
  }

  protected JsonObject readPostedObject(HttpServletRequest request) {
    StringBuilder builder = new StringBuilder();
    String line;
    try {
      BufferedReader reader = request.getReader();
      while ((line = reader.readLine()) != null)
        builder.append(line);
      return JsonObject.readObject(builder.toString());
    } catch (Exception e) {
      throw new PosumException("Could not read posted object", e);
    }
  }
}

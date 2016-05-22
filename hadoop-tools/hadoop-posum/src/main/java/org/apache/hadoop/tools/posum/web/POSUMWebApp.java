package org.apache.hadoop.tools.posum.web;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.tools.posum.common.util.JsonElement;
import org.apache.hadoop.tools.posum.common.util.JsonObject;
import org.apache.hadoop.tools.posum.common.util.POSUMException;
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
import java.io.File;
import java.io.IOException;
import java.net.URL;

public class POSUMWebApp extends HttpServlet {
    private static final long serialVersionUID = 1905162041950251407L;
    private static Log logger = LogFactory.getLog(POSUMWebApp.class);

    private transient Server server;

    private int port;
    protected final ResourceHandler staticHandler = new ResourceHandler();
    private final Handler handler;

    public POSUMWebApp(int metricsAddressPort) {
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
                        String call = target.substring("/ajax".length());
                        if ("/system".equals(call))
                            sendResult(request, response, wrapResult(getSystemMetrics()));
                        else
                            sendResult(request, response, wrapResult("Server is online!"));
                    } else {
                        response.setStatus(HttpServletResponse.SC_OK);
                        response.setCharacterEncoding("utf-8");
                        response.setContentType("text");

                        response.getWriter().println("Server is online!");
                        ((Request) request).setHandled(true);
                    }
                } catch (Exception e) {
                    logger.error("Error resolving request: ", e);
                }
            }
        };
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
                .put("result", JsonElement.wrapObject(result))
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
                              HttpServletResponse response, JsonNode result)
            throws IOException {
        response.setContentType("text/json");
        response.setStatus(HttpServletResponse.SC_OK);
        response.setHeader("Access-Control-Allow-Origin", "*");
        response.setHeader("Access-Control-Allow-Methods", "OPTIONS, GET, POST");

        response.getWriter().println(result.toString());
        ((Request) request).setHandled(true);
    }

    protected JsonNode getSystemMetrics() {
        double free = Runtime.getRuntime().freeMemory();
        double max = Runtime.getRuntime().maxMemory();
        double total = Runtime.getRuntime().totalMemory();
        return wrapResult(new JsonObject()
                .put("time", System.currentTimeMillis())
                .put("jvm", new JsonObject()
                        .put("free", String.format("%.3f", free / 1024 / 1024 / 1024))
                        .put("max", String.format("%.3f", max / 1024 / 1024 / 1024))
                        .put("total", String.format("%.3f", total / 1024 / 1024 / 1024)))
                .getNode());
    }
}

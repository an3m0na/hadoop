package org.apache.hadoop.tools.posum.web;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
    protected ObjectMapper mapper = new ObjectMapper();
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
                        sendResult(request, response, wrapResult("Server is online!"));
                    } else {
                        response.setStatus(HttpServletResponse.SC_OK);
                        response.setCharacterEncoding("utf-8");
                        response.setContentType("text");

                        response.getWriter().println("Server is online!");
                        ((Request) request).setHandled(true);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
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
        ObjectNode wrapper = mapper.createObjectNode();
        JsonNode out = result instanceof JsonNode ? (JsonNode) result : mapper.valueToTree(result);
        wrapper.put("result", out);
        wrapper.put("successful", true);
        LoggerFactory.getLogger(getClass()).debug("Sending result " + wrapper);
        return wrapper;
    }

    protected JsonNode wrapError(String code, String message, String errorString) {
        ObjectNode wrapper = mapper.createObjectNode();
        ObjectNode error = mapper.createObjectNode();
        error.put("code", code);
        error.put("message", message);
        wrapper.put("result", error);
        wrapper.put("successful", false);
        LoggerFactory.getLogger(getClass()).error("Sending error " + errorString);
        return wrapper;
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
}

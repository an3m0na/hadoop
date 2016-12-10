package org.apache.hadoop.tools.posum.web;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.tools.posum.common.util.Utils;
import org.apache.hadoop.tools.posum.simulation.master.SimulationMasterContext;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.handler.AbstractHandler;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class SimulatorWebApp extends PosumWebApp {
    private static final long serialVersionUID = 1905162041950251407L;
    private static Log logger = LogFactory.getLog(SimulatorWebApp.class);

    private SimulationMasterContext context;

    public SimulatorWebApp(SimulationMasterContext context, int metricsAddressPort) {
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

}

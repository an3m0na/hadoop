package org.apache.hadoop.tools.posum.web;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.hadoop.tools.posum.common.util.JsonObject;
import org.apache.hadoop.tools.posum.common.util.Utils;
import org.apache.hadoop.tools.posum.core.master.POSUMMasterContext;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.handler.AbstractHandler;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Created by ane on 4/29/16.
 */
public class MasterWebApp extends POSUMWebApp {
    private POSUMMasterContext context;

    public MasterWebApp(POSUMMasterContext context, int metricsAddressPort) {
        super(metricsAddressPort);
        this.context = context;
        staticHandler.setWelcomeFiles(new String[]{"posumstats.html"});
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
                                case "/conf":
                                    ret = getConfiguration();
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
                    e.printStackTrace();
                }
            }
        };
    }

    private JsonNode getConfiguration() {
        return wrapResult(new JsonObject()
                .put("dmAddress", context.getCommService().getDMAddress())
                .put("psAddress", context.getCommService().getPSAddress())
                .put("smAddress", context.getCommService().getSMAddress())
                .getNode());
    }
}

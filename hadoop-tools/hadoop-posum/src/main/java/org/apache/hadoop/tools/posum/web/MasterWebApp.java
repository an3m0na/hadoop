package org.apache.hadoop.tools.posum.web;

import org.mortbay.jetty.Handler;
import org.mortbay.jetty.handler.AbstractHandler;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Created by ane on 4/29/16.
 */
public class MasterWebApp extends POSUMWebApp {

    public MasterWebApp(int metricsAddressPort) {
        super(metricsAddressPort);

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
                        //TODO isolate call name and apply handler
                        sendResult(request, response, wrapResult("Server is online!"));
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
}

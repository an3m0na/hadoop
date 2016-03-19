package org.apache.hadoop.tools.posum.common;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.json.JSONConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.tools.posum.common.records.AppProfile;
import org.apache.hadoop.tools.posum.common.records.JobProfile;
import org.apache.hadoop.tools.posum.master.scheduler.data.DataOrientedScheduler;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import java.util.*;

/**
 * Created by ane on 2/24/16.
 */
public class RestClient {

    private static Log logger = LogFactory.getLog(RestClient.class);

    private Client client;

    public RestClient() {
        ClientConfig clientConfig = new DefaultClientConfig();
        clientConfig.getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);
        client = Client.create(clientConfig);
    }

    public enum TrackingUI {
        RM("ResourceManager", "http://localhost:8088", "ws/v1/"),
        HISTORY("History", "http://localhost:8088", "ws/v1/history/mapreduce/"),
        AM("ApplicationMaster", "http://localhost:8088", "proxy/%s/ws/v1/mapreduce/");

        private static final Map<String, TrackingUI> labelMap = new HashMap<>();

        public String label;
        public String root;
        public String host;

        static {
            for (TrackingUI field : TrackingUI.values()) {
                labelMap.put(field.label, field);
            }
        }

        TrackingUI(String label, String host, String root) {
            this.label = label;
            this.host = host;
            this.root = root;
        }

        public static TrackingUI fromLabel(String label) {
            return labelMap.get(label);
        }

    }

    public JSONObject getInfo(TrackingUI trackingUI, String path, String[] args) {
        WebResource resource = client.resource(trackingUI.host).path(String.format(trackingUI.root + path, args));
        ClientResponse response = resource.accept(MediaType.APPLICATION_JSON_TYPE).get(ClientResponse.class);

        if (response.getStatus() != 200 || !response.getType().equals(MediaType.APPLICATION_JSON_TYPE)){
            throw new WebApplicationException();
        }

        try {
            JSONObject object = response.getEntity(JSONObject.class);
            logger.debug("[RestClient] Raw response:" + object);
            return object;
        } catch (Exception e) {
            logger.error("Could not parse response as JSON ", e);
        }
        return null;
    }
}

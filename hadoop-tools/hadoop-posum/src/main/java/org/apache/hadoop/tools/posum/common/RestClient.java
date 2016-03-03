package org.apache.hadoop.tools.posum.common;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.json.JSONConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
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

    public enum TrackingUI {
        RM("ResourceManager", "http://localhost:8088", "ws/v1/"),
        HISTORY("History", "http://localhost:8088", "ws/v1/"),
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

    private static Log logger = LogFactory.getLog(DataOrientedScheduler.class);

    private Client client;

    public RestClient() {
        ClientConfig clientConfig = new DefaultClientConfig();
        clientConfig.getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);
        client = Client.create(clientConfig);
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

    public List<AppProfile> getAppsInfo() {
        List<AppProfile> apps = null;
        try {
            JSONObject wrapper = getInfo(TrackingUI.RM, "cluster/apps", new String[]{});
            if (wrapper.isNull("apps"))
                return Collections.emptyList();
            JSONArray rawApps = wrapper.getJSONObject("apps").getJSONArray("app");
            apps = new ArrayList<>(rawApps.length());
            for (int i = 0; i < rawApps.length(); i++) {
                JSONObject rawApp = rawApps.getJSONObject(i);
                AppProfile app = new AppProfile();
                app.setAppId(rawApp.getString("id"));
                app.setStartTime(rawApp.getLong("startedTime"));
                app.setFinishTime(rawApp.getLong("finishedTime"));
                app.setName(rawApp.getString("name"));
                app.setUser(rawApp.getString("user"));
                app.setState(rawApp.getString("state"));
                app.setStatus(rawApp.getString("finalStatus"));
                app.setTrackingUI(rawApp.getString("trackingUI"));
                //TODO maybe queue
                apps.add(app);
            }
        } catch (JSONException | YarnException e) {
            logger.debug("[RestClient] Exception parsing apps", e);
        }
        return apps;
    }

    public List<JobProfile> getJobsInfo(ApplicationId appId) {
        List<JobProfile> jobs = null;
        try {
            JSONObject wrapper = getInfo(TrackingUI.AM, "jobs", new String[]{appId.toString()});
            if (wrapper.isNull("jobs"))
                return Collections.emptyList();
            JSONArray rawJobs = wrapper.getJSONObject("jobs").getJSONArray("job");
            jobs = new ArrayList<>(rawJobs.length());
            if (jobs.size() != 1)
                throw new YarnException("Application contains unexpected number of jobs: " + jobs.size());
            for (int i = 0; i < rawJobs.length(); i++) {
                JSONObject rawJob = rawJobs.getJSONObject(i);
//                JobProfile job = new JobProfile();
//                AppProfile app = new AppProfile();
//                app.setAppId(rawApp.getString("appId"));
//                app.setStartTime(rawApp.getLong("startedTime"));
//                app.setFinishTime(rawApp.getLong("finishedTime"));
//                app.setName(rawApp.getString("name"));
//                app.setUser(rawApp.getString("user"));
//                app.setState(rawApp.getString("state"));
//                app.setStatus(rawApp.getString("status"));
                //TODO maybe queue
//                jobs.add(job);
            }
        } catch (JSONException | YarnException e) {
            logger.debug("[RestClient] Exception parsing apps", e);
        }
        return jobs;
    }

    public Map<String, String> getJobConfProperties(ApplicationId appId, JobId jobId, Map<String, String> requested) {
        Map<String, String> ret = new HashMap<>(requested.size());
        try {
            JSONObject wrapper = getInfo(TrackingUI.AM, "jobs/%s/conf", new String[]{
                    appId.toString(),
                    jobId.toString()
            });
            try {
                JSONArray properties = wrapper.getJSONObject("conf").getJSONArray("property");
                for (int i = 0; i < properties.length(); i++) {
                    JSONObject property = properties.getJSONObject(i);
                    String requestedLabel = requested.get(property.getString("name"));
                    if (requestedLabel != null)
                        ret.put(requestedLabel, property.getString("value"));
                }
            } catch (JSONException e) {
                logger.debug("[RestClient] Exception parsing job conf", e);
            }
        } catch (WebApplicationException e) {
            logger.error("[RestClient] Could not get job conf for " + jobId, e);
        }
        return ret;

    }
}

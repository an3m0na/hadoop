package org.apache.hadoop.tools.posum.predictor;

import org.apache.hadoop.tools.posum.common.RestClient;
import org.apache.hadoop.tools.posum.common.records.AppProfile;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by ane on 2/24/16.
 */
public class TestRestClient {

    RestClient client = new RestClient();

    @Test
    public void testJobInfo() {
        List<AppProfile> apps = client.getAppsInfo();
        System.out.println(apps);
        for (AppProfile app : apps) {
            if (RestClient.TrackingUI.AM.equals(app.getTrackingUI())) {
                System.out.println("For job: " + app.getId());
                Map<String, String> requested = new HashMap<>(2);
                requested.put("mapreduce.input.fileinputformat.inputdir", "inputdir");
                requested.put("mapreduce.input.fileinputformat.numinputfiles", "numinputfiles");
                Map<String, String> properties = client.getJobConfProperties(app.getId(), app.getId(), requested);
                System.out.println("Input dir is: " + properties.get("inputdir"));
                System.out.println("Input files are: " + properties.get("numinputfiles"));
            }
        }
    }
}

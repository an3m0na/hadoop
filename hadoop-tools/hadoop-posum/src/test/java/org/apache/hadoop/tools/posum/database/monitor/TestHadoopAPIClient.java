package org.apache.hadoop.tools.posum.database.monitor;

import org.apache.hadoop.tools.posum.common.util.POSUMConfiguration;
import org.apache.hadoop.tools.posum.common.util.RestClient;
import org.apache.hadoop.tools.posum.common.records.dataentity.AppProfile;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by ane on 2/24/16.
 */
public class TestHadoopAPIClient {

    private HadoopAPIClient collector = new HadoopAPIClient(POSUMConfiguration.newInstance());

    @Test
    public void testJobInfo() {
        List<AppProfile> apps = collector.getAppsInfo();
        System.out.println(apps);
        for (AppProfile app : apps) {
            if (RestClient.TrackingUI.AM.equals(app.getTrackingUI())) {
                System.out.println("For job: " + app.getId());
                Map<String, String> requested = new HashMap<>(2);
                requested.put("mapreduce.input.fileinputformat.inputdir", "inputdir");
                requested.put("mapreduce.input.fileinputformat.numinputfiles", "numinputfiles");
                Map<String, String> properties = collector.getJobConfProperties(app.getId(), app.getId(), requested);
                System.out.println("Input dir is: " + properties.get("inputdir"));
                System.out.println("Input files are: " + properties.get("numinputfiles"));
            }
        }
    }
}
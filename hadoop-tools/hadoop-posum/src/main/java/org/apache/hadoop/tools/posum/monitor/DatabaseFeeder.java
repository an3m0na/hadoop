package org.apache.hadoop.tools.posum.monitor;

import org.apache.hadoop.tools.posum.common.RestClient;
import org.apache.hadoop.tools.posum.common.records.AppProfile;
import org.apache.hadoop.tools.posum.database.DataCollection;
import org.apache.hadoop.tools.posum.database.DataStore;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by ane on 2/4/16.
 */
public class DatabaseFeeder {
    private RestClient restClient = new RestClient();
    private Set<String> running = new HashSet<>();
    private Set<String> finished = new HashSet<>();
    private DataStore dataStore;

    public DatabaseFeeder(DataStore dataStore) {
        this.dataStore = dataStore;
    }

    public void feedDatabase() {
        List<AppProfile> apps = restClient.getAppsInfo();
        for (AppProfile app : apps) {
            if (!finished.contains(app.getAppId())) {
                if (RestClient.TrackingUI.HISTORY.equals(app.getTrackingUI())) {
                    //TODO move it, its jobs and its tasks from the current collections to the history collections
                } else {
                    if (!running.contains(app.getAppId())) {
                        running.add(app.getAppId());
                        storeAppInfo(app);
                        //TODO store its info, jobs and tasks in current collections
                    } else {
                        //TODO update its info, jobs and tasks in current collections
                    }
                }
            }
        }
    }

    public void storeAppInfo(AppProfile app) {
        if (RestClient.TrackingUI.AM.equals(app.getTrackingUI())) {
            //TODO fetch jobs
            //TODO for each job, fetch tasks and tasks attempts
            //TODO store task info
            //TODO store job info
        }
        dataStore.store(DataCollection.APPS, app);
    }
}

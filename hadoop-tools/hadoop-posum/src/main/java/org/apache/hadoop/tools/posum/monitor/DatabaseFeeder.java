package org.apache.hadoop.tools.posum.monitor;

import org.apache.hadoop.tools.posum.common.RestClient;
import org.apache.hadoop.tools.posum.common.records.AppProfile;
import org.apache.hadoop.tools.posum.database.DataCollection;
import org.apache.hadoop.tools.posum.database.DataStore;

import java.util.*;

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
            if (!finished.contains(app.getId())) {
                if (RestClient.TrackingUI.HISTORY.equals(app.getTrackingUI())) {
                    moveAppToHistory(app);
                } else {
                    if (!running.contains(app.getId())) {
                        registerNewApp(app);
                    } else {
                        updateAppInfo(app);
                    }
                }
            }
        }
    }

    private void moveAppToHistory(AppProfile app) {
        running.remove(app.getId());
        dataStore.delete(DataCollection.APPS, app.getId());
        dataStore.delete(DataCollection.JOBS, app.getId());
        dataStore.delete(DataCollection.TASKS, "jobId", app.getId());
        //TODO fetch job info from history server
        //TODO fetch task info from history server
        //TODO save task info to history
        //TODO save job info to history
        dataStore.store(DataCollection.APPS_HISTORY, app);
    }

    public void registerNewApp(AppProfile app) {
        running.add(app.getId());
        if (RestClient.TrackingUI.AM.equals(app.getTrackingUI())) {
            //TODO fetch jobs
            //TODO for each job, fetch tasks and tasks attempts
            //TODO store task info
            //TODO store job info
        }
//        dataStore.store(DataCollection.APPS, app);
        dataStore.updateOrStore(DataCollection.APPS, app);
    }


    public void updateAppInfo(AppProfile app) {
        if (RestClient.TrackingUI.AM.equals(app.getTrackingUI())) {
            //TODO fetch jobs
            //TODO for each job, fetch tasks and tasks attempts
            //TODO store task info
            //TODO store job info
        }
        dataStore.updateOrStore(DataCollection.APPS, app);
    }
}

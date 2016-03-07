package org.apache.hadoop.tools.posum.monitor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.tools.posum.common.RestClient;
import org.apache.hadoop.tools.posum.common.records.AppProfile;
import org.apache.hadoop.tools.posum.common.records.JobProfile;
import org.apache.hadoop.tools.posum.database.DataCollection;
import org.apache.hadoop.tools.posum.database.DataStore;

import java.util.*;

/**
 * Created by ane on 2/4/16.
 */
public class DatabaseFeeder implements Configurable {

    private static Log logger = LogFactory.getLog(DatabaseFeeder.class);

    private Set<String> running = new HashSet<>();
    private Set<String> finished = new HashSet<>();
    private DataStore dataStore;
    private SystemInfoCollector collector;
    private Configuration conf;

    public DatabaseFeeder(Configuration conf, DataStore dataStore) {
        this.dataStore = dataStore;
        this.conf = conf;
        this.collector = new SystemInfoCollector(conf);
    }

    @Override
    public void setConf(Configuration conf) {

    }

    @Override
    public Configuration getConf() {
        return null;
    }

    public void feedDatabase() {
        List<AppProfile> apps = collector.getAppsInfo();
        logger.debug("[" + getClass().getSimpleName() + "] Found " + apps.size() + " apps");
        for (AppProfile app : apps) {
            if (!finished.contains(app.getId())) {
                logger.debug("[" + getClass().getSimpleName() + "] App " + app.getId() + " not finished");
                if (RestClient.TrackingUI.HISTORY.equals(app.getTrackingUI())) {
                    logger.debug("[" + getClass().getSimpleName() + "] App " + app.getId() + " finished just now");
                    moveAppToHistory(app);
                } else {
                    logger.debug("[" + getClass().getSimpleName() + "] App " + app.getId() + " is running");
                    running.add(app.getId());
                    updateAppInfo(app);
                }
            }
        }
    }

    private void moveAppToHistory(AppProfile app) {
        logger.debug("[" + getClass().getSimpleName() + "] Moving " + app.getId() + " to history");
        running.remove(app.getId());
        finished.add(app.getId());
        dataStore.delete(DataCollection.APPS, app.getId());
        dataStore.delete(DataCollection.JOBS, "appId", app.getId());
        dataStore.delete(DataCollection.TASKS, "jobId", app.getId());
        //TODO fetch job info from history server
        //TODO fetch task info from history server
        //TODO save task info to history
        //TODO save job info to history
        dataStore.updateOrStore(DataCollection.APPS_HISTORY, app);
    }

    public void updateAppInfo(AppProfile app) {
        logger.debug("[" + getClass().getSimpleName() + "] Updating " + app.getId() + " info");
        if (RestClient.TrackingUI.AM.equals(app.getTrackingUI())) {
            List<JobProfile> jobs = collector.getRunningJobsInfo(app.getId());
            logger.debug("[" + getClass().getSimpleName() + "] Found " + jobs.size() + " jobs for " + app.getId());
            for (JobProfile job : jobs) {
                //TODO for each job, fetch tasks and tasks attempts
                //TODO store task info
                dataStore.updateOrStore(DataCollection.JOBS, job);
            }
        } else {
            //app is not yet tracked
            logger.debug("[" + getClass().getSimpleName() + "] App " + app.getId() + " is not tracked");
            List<JobProfile> jobs = collector.getSubmittedJobsInfo(app.getId());
            for (JobProfile job : jobs) {
                dataStore.updateOrStore(DataCollection.JOBS, job);
            }
        }
        dataStore.updateOrStore(DataCollection.APPS, app);
    }
}

package org.apache.hadoop.tools.posum.database.monitor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.tools.posum.common.RestClient;
import org.apache.hadoop.tools.posum.common.records.profile.AppProfile;
import org.apache.hadoop.tools.posum.common.records.profile.HistoryProfile;
import org.apache.hadoop.tools.posum.common.records.profile.JobProfile;
import org.apache.hadoop.tools.posum.common.records.profile.TaskProfile;
import org.apache.hadoop.tools.posum.database.store.DataCollection;
import org.apache.hadoop.tools.posum.database.store.DataStore;

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
        dataStore.delete(DataCollection.TASKS, "appId", app.getId());
        //TODO fetch job info from history server
        //TODO fetch task info from history server
        //TODO save task info to history
        //TODO save job info to history
        dataStore.updateOrStore(DataCollection.APPS_HISTORY, app);
        dataStore.store(DataCollection.HISTORY, new HistoryProfile<>(app));
    }

    public void updateAppInfo(AppProfile app) {
        logger.debug("[" + getClass().getSimpleName() + "] Updating " + app.getId() + " info");
        if (RestClient.TrackingUI.AM.equals(app.getTrackingUI())) {
            JobProfile lastJobInfo = dataStore.getJobProfileForApp(app.getId());
            JobProfile job = collector.getRunningJobInfo(app.getId(), lastJobInfo);
            if (job == null)
                logger.debug("[" + getClass().getSimpleName() + "] Could not find job for " + app.getId());
            else {
                List<TaskProfile> tasks = collector.getRunningTasksInfo(job);
                Integer mapDuration = 0, reduceDuration = 0, avgDuration = 0, mapNo = 0, reduceNo = 0, avgNo = 0;
                for (TaskProfile task : tasks) {
                    Integer duration = task.getDuration();
                    if (duration > 0) {
                        if (TaskType.MAP.equals(task.getType())) {
                            mapDuration += task.getDuration();
                            mapNo++;
                        }
                        if (TaskType.REDUCE.equals(task.getType())) {
                            reduceDuration += task.getDuration();
                            reduceNo++;
                        }
                        avgDuration += duration;
                        avgNo++;
                    }
                    if (avgNo > 0) {
                        job.setAvgTaskDuration(avgDuration / avgNo);
                        if (mapNo > 0)
                            job.setAvgMapDuration(mapDuration / mapNo);
                        if (reduceNo > 0)
                            job.setAvgReduceDuration(reduceDuration / reduceNo);
                    }
                    dataStore.updateOrStore(DataCollection.TASKS, task);
                    dataStore.store(DataCollection.HISTORY, new HistoryProfile<>(task));
                }
                dataStore.updateOrStore(DataCollection.JOBS, job);
                dataStore.store(DataCollection.HISTORY, new HistoryProfile<>(job));
            }
        } else {
            //app is not yet tracked
            logger.debug("[" + getClass().getSimpleName() + "] App " + app.getId() + " is not tracked");
            try {
                JobProfile job = collector.getSubmittedJobInfo(app.getId());
                dataStore.updateOrStore(DataCollection.JOBS, job);
                dataStore.store(DataCollection.HISTORY, job);
            } catch (Exception e) {
                logger.error("Could not get job info from staging dir!", e);
            }
        }
        dataStore.updateOrStore(DataCollection.APPS, app);
        dataStore.store(DataCollection.HISTORY, new HistoryProfile<>(app));
    }
}

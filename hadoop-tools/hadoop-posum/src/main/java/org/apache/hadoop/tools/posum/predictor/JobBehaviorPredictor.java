package org.apache.hadoop.tools.posum.predictor;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.tools.posum.database.DataStore;

/**
 * Created by ane on 2/9/16.
 */
public abstract class JobBehaviorPredictor implements Configurable {

    Configuration conf;
    DataStore dataStore;

    public JobBehaviorPredictor(DataStore dataStore) {
        this.conf = new Configuration(false);
        this.dataStore = dataStore;
    }

    public abstract Integer predictJobDuration(String jobId);

    public abstract Integer predictTaskDuration(String jobId, TaskType type);

    public abstract Integer predictTaskDuration(String jobId, String taskId);

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }
}

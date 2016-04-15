package org.apache.hadoop.tools.posum.simulator.predictor;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.tools.posum.database.store.DataStoreInterface;

/**
 * Created by ane on 2/9/16.
 */
public abstract class JobBehaviorPredictor implements Configurable {

    Configuration conf;
    DataStoreInterface dataStoreInterface;

    public JobBehaviorPredictor(DataStoreInterface dataStoreInterface) {
        this.conf = new Configuration(false);
        this.dataStoreInterface = dataStoreInterface;
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
